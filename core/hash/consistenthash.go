package hash

import (
	"fmt"
	"sort"
	"strconv"
	"sync"

	"github.com/tal-tech/go-zero/core/lang"
	"github.com/tal-tech/go-zero/core/mapping"
)

const (
	TopWeight = 100

	minReplicas = 100
	prime       = 16777619
)

type (
	HashFunc func(data []byte) uint64

	ConsistentHash struct {
		hashFunc HashFunc
		replicas int
		keys     []uint64
		ring     map[uint64][]interface{}
		nodes    map[string]lang.PlaceholderType
		lock     sync.RWMutex
	}
)

func NewConsistentHash() *ConsistentHash {
	return NewCustomConsistentHash(minReplicas, Hash)
}

func NewCustomConsistentHash(replicas int, fn HashFunc) *ConsistentHash {
	if replicas < minReplicas {
		replicas = minReplicas
	}

	if fn == nil {
		fn = Hash
	}

	return &ConsistentHash{
		hashFunc: fn,
		replicas: replicas,
		ring:     make(map[uint64][]interface{}),
		nodes:    make(map[string]lang.PlaceholderType),
	}
}

// Add adds the node with the number of h.replicas,
// the later call will overwrite the replicas of the former calls.
func (h *ConsistentHash) Add(node interface{}) {
	h.AddWithReplicas(node, h.replicas)
}

// AddWithReplicas adds the node with the number of replicas,
// replicas will be truncated to h.replicas if it's larger than h.replicas,
// the later call will overwrite the replicas of the former calls.
func (h *ConsistentHash) AddWithReplicas(node interface{}, replicas int) {
	// 如果要插入的node节点存在，则先删除
	h.Remove(node)

	// 如果传入的副本数大于当前的副本数，则以当前的副本数为准
	if replicas > h.replicas {
		replicas = h.replicas
	}

	// 获取节点的字符串
	nodeRepr := repr(node)
	h.lock.Lock()
	defer h.lock.Unlock()

	// 在nodes中插入当前节点
	h.addNode(nodeRepr)

	// 遍历副本数，循环将当前节点及其hash值存入keys及ring中
	for i := 0; i < replicas; i++ {
		hash := h.hashFunc([]byte(nodeRepr + strconv.Itoa(i)))
		h.keys = append(h.keys, hash)
		h.ring[hash] = append(h.ring[hash], node)
	}

	// 将keys按照重小到大排序，以便获取节点时搜索
	sort.Slice(h.keys, func(i int, j int) bool {
		return h.keys[i] < h.keys[j]
	})
}

// 按权重增加节点
// AddWithWeight adds the node with weight, the weight can be 1 to 100, indicates the percent,
// the later call will overwrite the replicas of the former calls.
func (h *ConsistentHash) AddWithWeight(node interface{}, weight int) {
	// don't need to make sure weight not larger than TopWeight,
	// because AddWithReplicas makes sure replicas cannot be larger than h.replicas
	replicas := h.replicas * weight / TopWeight
	h.AddWithReplicas(node, replicas)
}

func (h *ConsistentHash) Get(v interface{}) (interface{}, bool) {
	h.lock.RLock()
	defer h.lock.RUnlock()

	if len(h.ring) == 0 {
		return nil, false
	}

	hash := h.hashFunc([]byte(repr(v)))
	index := sort.Search(len(h.keys), func(i int) bool {
		return h.keys[i] >= hash
	}) % len(h.keys)

	nodes := h.ring[h.keys[index]]
	switch len(nodes) {
	case 0:
		return nil, false
	case 1:
		return nodes[0], true
	default:
		// 根据固定值与传入的KEY组成的字符串的HASH值与找到的节点数进行取模，以确定最终选择哪个节点
		innerIndex := h.hashFunc([]byte(innerRepr(v)))
		pos := int(innerIndex % uint64(len(nodes)))
		return nodes[pos], true
	}
}

func (h *ConsistentHash) Remove(node interface{}) {
	// 获取节点字符串
	nodeRepr := repr(node)

	h.lock.Lock()
	defer h.lock.Unlock()

	// 判断是否存在该节点
	if !h.containsNode(nodeRepr) {
		return
	}

	// 遍历keys及ring，将对应数据全部删除掉
	for i := 0; i < h.replicas; i++ {
		hash := h.hashFunc([]byte(nodeRepr + strconv.Itoa(i)))
		index := sort.Search(len(h.keys), func(i int) bool {
			return h.keys[i] >= hash
		})
		if index < len(h.keys) {
			h.keys = append(h.keys[:index], h.keys[index+1:]...)
		}
		h.removeRingNode(hash, nodeRepr)
	}

	// 在nodes上删除对应节点
	h.removeNode(nodeRepr)
}

func (h *ConsistentHash) removeRingNode(hash uint64, nodeRepr string) {
	// 根据hash找到对应的节点切片
	if nodes, ok := h.ring[hash]; ok {
		// 基于当前切片创建新的切片（共用一个底层数组，节省内存）
		newNodes := nodes[:0]
		// 遍历切片，将要删除的节点排除在新切片之外
		for _, x := range nodes {
			if repr(x) != nodeRepr {
				newNodes = append(newNodes, x)
			}
		}
		// 如果新切片存在元素，则将新切片赋予ring[hash]，否则删除ring中的hash节点
		if len(newNodes) > 0 {
			h.ring[hash] = newNodes
		} else {
			delete(h.ring, hash)
		}
	}
}

func (h *ConsistentHash) addNode(nodeRepr string) {
	h.nodes[nodeRepr] = lang.Placeholder
}

func (h *ConsistentHash) containsNode(nodeRepr string) bool {
	_, ok := h.nodes[nodeRepr]
	return ok
}

func (h *ConsistentHash) removeNode(nodeRepr string) {
	delete(h.nodes, nodeRepr)
}

func innerRepr(node interface{}) string {
	return fmt.Sprintf("%d:%v", prime, node)
}

// 将传入对象转为字符串类型
func repr(node interface{}) string {
	return mapping.Repr(node)
}
