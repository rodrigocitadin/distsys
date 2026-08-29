package main

import (
	"encoding/json"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BroadcastMessage struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

type GossipMessage struct {
	Type     string `json:"type"`
	Messages []int  `json:"messages"`
}

type TopologyMessage struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
}

type PeerMessage struct {
	Peer    string
	Message int
}

type state struct {
	seen    map[int]struct{}
	pending map[PeerMessage]struct{}
	peers   []string
}

type command func(*state)

func main() {
	n := maelstrom.NewNode()
	commands := make(chan command)

	go func() {
		s := &state{
			seen:    make(map[int]struct{}),
			pending: make(map[PeerMessage]struct{}),
		}
		for cmd := range commands {
			cmd(s)
		}
	}()

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for range ticker.C {
			peerMessagesCh := make(chan map[string][]int, 1)
			commands <- func(s *state) {
				out := make(map[string][]int)
				for peerMessage, _ := range s.pending {
					out[peerMessage.Peer] = append(out[peerMessage.Peer], peerMessage.Message)
				}

				peerMessagesCh <- out
			}

			peerMessages := <-peerMessagesCh
			for peer, messages := range peerMessages {
				if len(messages) == 0 {
					continue
				}

				body := GossipMessage{
					Type:     "gossip",
					Messages: messages,
				}
				n.RPC(peer, body, func(msg maelstrom.Message) error {
					var body map[string]any
					if err := json.Unmarshal(msg.Body, &body); err != nil {
						return err
					}

					commands <- func(s *state) {
						if body["type"] == "gossip_ok" {
							for _, message := range messages {
								delete(s.pending, PeerMessage{peer, message})
							}
						}
					}

					return nil
				})
			}
		}
	}()

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body TopologyMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		commands <- func(s *state) {
			s.peers = body.Topology[n.ID()]
		}

		return n.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		result := make(chan []int, 1)
		commands <- func(s *state) {
			out := make([]int, 0, len(s.seen))
			for message := range s.seen {
				out = append(out, message)
			}
			result <- out
		}

		return n.Reply(msg, map[string]any{"type": "read_ok", "messages": <-result})
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body BroadcastMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		syncState(commands, []int{body.Message}, msg.Src)
		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	n.Handle("gossip", func(msg maelstrom.Message) error {
		var body GossipMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		syncState(commands, body.Messages, msg.Src)
		return n.Reply(msg, map[string]any{"type": "gossip_ok"})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func syncState(commands chan<- command, messages []int, sender string) {
	commands <- func(s *state) {
		for _, message := range messages {
			syncPending(s, message, sender)
			s.seen[message] = struct{}{}
		}
	}
}

func syncPending(s *state, message int, sender string) {
	if _, dup := s.seen[message]; !dup {
		for _, peer := range s.peers {
			if sender != peer {
				peerMessage := PeerMessage{peer, message}
				s.pending[peerMessage] = struct{}{}
			}
		}
	}
}
