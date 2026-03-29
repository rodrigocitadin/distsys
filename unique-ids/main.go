package main

import (
	"encoding/json"
	"log"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	epoch        = int64(1700000000000000)
	nodeBits     = 10
	sequenceBits = 12

	nodeShift = sequenceBits
	timeShift = sequenceBits + nodeBits

	sequenceMask = (1 << sequenceBits) - 1
)

type Smolflake struct {
	state       int64
	shiftedNode int64
}

func NewSmolflake(nodeIDStr string) *Smolflake {
	var nodeID int64
	if len(nodeIDStr) > 1 {
		parsed, _ := strconv.ParseInt(nodeIDStr[1:], 10, 64)
		nodeID = parsed % (1 << nodeBits)
	}

	return &Smolflake{
		state:       0,
		shiftedNode: nodeID << nodeShift,
	}
}

func (s *Smolflake) NextID() int64 {
	for {
		now := time.Now().UnixMicro() - epoch
		oldState := atomic.LoadInt64(&s.state)

		lastTs := oldState >> sequenceBits
		seq := oldState & sequenceMask

		var nextTs int64
		var nextSeq int64

		if now <= lastTs {
			nextTs = lastTs
			nextSeq = (seq + 1) & sequenceMask

			if nextSeq == 0 {
				nextTs = lastTs + 1
			}
		} else {
			nextTs = now
			nextSeq = 0
		}

		newState := (nextTs << sequenceBits) | nextSeq

		if atomic.CompareAndSwapInt64(&s.state, oldState, newState) {
			return (nextTs << timeShift) | s.shiftedNode | nextSeq
		}
	}
}

func main() {
	n := maelstrom.NewNode()

	var smolflake atomic.Pointer[Smolflake]

	n.Handle("init", func(msg maelstrom.Message) error {
		smolflake.Store(NewSmolflake(n.ID()))

		return n.Reply(msg, map[string]any{
			"type": "init_ok",
		})
	})

	n.Handle("generate", func(msg maelstrom.Message) error {
		sf := smolflake.Load()
		for sf == nil {
			runtime.Gosched()
			sf = smolflake.Load()
		}

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		out := map[string]any{
			"type": "generate_ok",
			"id":   sf.NextID(),
		}

		return n.Reply(msg, out)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
