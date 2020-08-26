package mathx

import (
	"math/rand"
	"sync"
	"time"
)

type Proba struct {
	// rand.New(...) returns a non thread safe object
	r    *rand.Rand
	lock sync.Mutex
}

func NewProba() *Proba {
	return &Proba{
		r: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// proba 取值范围在 [0.0, 1.0)之间， 表示proba的概率通过
func (p *Proba) TrueOnProba(proba float64) (truth bool) {
	p.lock.Lock()
	truth = p.r.Float64() < proba
	p.lock.Unlock()
	return
}
