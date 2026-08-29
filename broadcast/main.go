package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	messagesCh := make(chan int, 10)
	var messages []int

	go func() {
		for message := range messagesCh {
			messages = append(messages, message)
		}
	}()

	n := maelstrom.NewNode()

	n.Handle("topology", func(msg maelstrom.Message) error {
		out := map[string]any{"type": "topology_ok"}
		return n.Reply(msg, out)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		out := map[string]any{"type": "read_ok", "messages": messages}
		return n.Reply(msg, out)
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		messagesCh <- int(body["message"].(float64))

		out := map[string]any{"type": "broadcast_ok"}
		return n.Reply(msg, out)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
