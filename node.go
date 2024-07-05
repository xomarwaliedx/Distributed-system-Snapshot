package asg3

import (
	"log"
)

// The main participant of the distributed snapshot protocol.
// nodes exchange token messages and marker messages among each other.
// Token messages represent the transfer of tokens from one node to another.
// Marker messages represent the progress of the snapshot process. The bulk of
// the distributed protocol is implemented in `HandlePacket` and `StartSnapshot`.

type Node struct {
	sim             *ChandyLamportSim
	id              string
	tokens          int
	outboundLinks   map[string]*Link         // key = link.dest
	inboundLinks    map[string]*Link         // key = link.src
	startedSnapshot map[int]int              // key: snap id, value: inbounds
	snapTokenCount  map[int]int              // key: snap id, value: number of tokens
	incomingMarkers map[int]map[string][]int // key: snap id, key: src  value: incoming tokens
	srcDone         map[int]map[string]bool  // key: snap id, key: src  value: done
	// TODO: add more fields here (what does each node need to keep track of?)

}

// A unidirectional communication channel between two nodes
// Each link contains an event queue (as opposed to a packet queue)
type Link struct {
	src      string
	dest     string
	msgQueue *Queue
}

func CreateNode(id string, tokens int, sim *ChandyLamportSim) *Node {
	return &Node{
		sim:             sim,
		id:              id,
		tokens:          tokens,
		outboundLinks:   make(map[string]*Link),
		inboundLinks:    make(map[string]*Link),
		startedSnapshot: make(map[int]int),
		snapTokenCount:  make(map[int]int, 0),
		incomingMarkers: make(map[int]map[string][]int),
		srcDone:         make(map[int]map[string]bool),
		// TODO: You may need to modify this if you make modifications above

	}
}

// Add a unidirectional link to the destination node
func (node *Node) AddOutboundLink(dest *Node) {
	if node == dest {
		return
	}
	l := Link{node.id, dest.id, NewQueue()}
	node.outboundLinks[dest.id] = &l
	dest.inboundLinks[node.id] = &l
}

// Send a message on all of the node's outbound links
func (node *Node) SendToNeighbors(message Message) {
	for _, nodeId := range getSortedKeys(node.outboundLinks) {
		link := node.outboundLinks[nodeId]
		node.sim.logger.RecordEvent(
			node,
			SentMsgRecord{node.id, link.dest, message})
		link.msgQueue.Push(SendMsgEvent{
			node.id,
			link.dest,
			message,
			node.sim.GetReceiveTime()})
	}
}

// Send a number of tokens to a neighbor attached to this node
func (node *Node) SendTokens(numTokens int, dest string) {
	if node.tokens < numTokens {
		log.Fatalf("node %v attempted to send %v tokens when it only has %v\n",
			node.id, numTokens, node.tokens)
	}
	message := Message{isMarker: false, data: numTokens}
	node.sim.logger.RecordEvent(node, SentMsgRecord{node.id, dest, message})
	// Update local state before sending the tokens
	node.tokens -= numTokens
	link, ok := node.outboundLinks[dest]
	if !ok {
		log.Fatalf("Unknown dest ID %v from node %v\n", dest, node.id)
	}

	link.msgQueue.Push(SendMsgEvent{
		node.id,
		dest,
		message,
		node.sim.GetReceiveTime()})
}

func (node *Node) HandlePacket(src string, message Message) {
	if message.isMarker {
		if _, ok := node.startedSnapshot[message.data]; !ok {
			node.StartSnapshot(message.data)
			if node.srcDone[message.data] == nil {
				node.srcDone[message.data] = make(map[string]bool)
			}
			node.srcDone[message.data][src] = true
		} else {
			node.startedSnapshot[message.data] -= 1
			if node.srcDone[message.data] == nil {
				node.srcDone[message.data] = make(map[string]bool)
			}
			node.srcDone[message.data][src] = true
			if node.startedSnapshot[message.data] == 0 {
				node.sim.NotifyCompletedSnapshot(node.id, message.data)
			}
		}
	} else {
		for key := range node.startedSnapshot {

			if node.startedSnapshot[key] > 0 && !node.srcDone[key][src] {
				node.incomingMarkers[key][src] = append(node.incomingMarkers[key][src], message.data)
			}
		}
		node.tokens += message.data
	}

	// TODO: Write this method
}

func (node *Node) StartSnapshot(snapshotId int) {
	if snapshotId < 0 {
		snapshotId = snapshotId*-1 - 1
		node.startedSnapshot[snapshotId] = len(node.inboundLinks)
	} else {
		node.startedSnapshot[snapshotId] = len(node.inboundLinks) - 1
	}
	node.SendToNeighbors(Message{isMarker: true, data: snapshotId})
	node.snapTokenCount[snapshotId] = node.tokens
	node.incomingMarkers[snapshotId] = make(map[string][]int)
	if node.startedSnapshot[snapshotId] == 0 {
		node.sim.NotifyCompletedSnapshot(node.id, snapshotId)
	}
	// ToDo: Write this method
}
