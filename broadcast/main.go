package main

import (
	"encoding/json"
	"log"
	"slices"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type GossipMessage struct {
	Type         string   `json:"type"`
	NodesWhoKnow []string `json:"nodes_who_know"`
	Message      int      `json:"message"`
}

type state struct {
	seen  map[int]struct{}
	peers []string
}

type command func(*state)

func main() {
	n := maelstrom.NewNode()
	commands := make(chan command)

	go func() {
		s := &state{seen: make(map[int]struct{})}
		for cmd := range commands {
			cmd(s)
		}
	}()

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body struct {
			Topology map[string][]string `json:"topology"`
		}
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
		var body struct {
			Message int `json:"message"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		gossip(n, commands, body.Message, []string{n.ID()})

		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	n.Handle("gossip", func(msg maelstrom.Message) error {
		var body GossipMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		gossip(n, commands, body.Message, append(body.NodesWhoKnow, n.ID()))

		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func gossip(n *maelstrom.Node, commands chan<- command, message int, whoKnows []string) {
	targets := make(chan []string, 1)
	commands <- func(s *state) {
		if _, dup := s.seen[message]; dup {
			targets <- nil
			return
		}
		s.seen[message] = struct{}{}

		var out []string
		for _, peer := range s.peers {
			if !slices.Contains(whoKnows, peer) {
				out = append(out, peer)
			}
		}
		targets <- out
	}

	body := GossipMessage{Type: "gossip", NodesWhoKnow: whoKnows, Message: message}
	for _, peer := range <-targets {
		n.Send(peer, body)
	}
}
