package collection

import (
	"sync"
	"time"

	"github.com/tal-tech/go-zero/core/timex"
)

type (
	RollingWindowOption func(rollingWindow *RollingWindow)

	// 移动窗口，用于统计数据
	RollingWindow struct {
		lock          sync.RWMutex
		size          int
		win           *window
		interval      time.Duration
		offset        int
		ignoreCurrent bool
		lastTime      time.Duration
	}
)

func NewRollingWindow(size int, interval time.Duration, opts ...RollingWindowOption) *RollingWindow {
	if size < 1 {
		panic("size must be greater than 0")
	}

	w := &RollingWindow{
		size:     size,
		win:      newWindow(size),
		interval: interval,
		lastTime: timex.Now(),
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

func (rw *RollingWindow) Add(v float64) {
	rw.lock.Lock()
	defer rw.lock.Unlock()
	// 根据时间重置桶及偏离量
	rw.updateOffset()
	// 根据重置后的偏移量添加当前值
	rw.win.add(rw.offset, v)
}

func (rw *RollingWindow) Reduce(fn func(b *Bucket)) {
	rw.lock.RLock()
	defer rw.lock.RUnlock()

	var diff int
	// 获取
	span := rw.span()
	// ignore current bucket, because of partial data
	if span == 0 && rw.ignoreCurrent {
		diff = rw.size - 1
	} else {
		diff = rw.size - span
	}
	if diff > 0 {
		// rw.offset（之前的偏移量） + span（新增的偏移量） + 1，即为要开始处理的位置
		offset := (rw.offset + span + 1) % rw.size
		rw.win.reduce(offset, diff, fn)
	}
}

// 根据当前时间与最后更新时间计算偏移的增量
func (rw *RollingWindow) span() int {
	// 根据lastTime与当前时间的差值除于interval间隔，得到当前偏移量
	offset := int(timex.Since(rw.lastTime) / rw.interval)
	// 如果偏移量在[0, size)之间，则直接返回该值，否则返回size
	if 0 <= offset && offset < rw.size {
		return offset
	} else {
		return rw.size
	}
}

// 基于时间更新偏移量
func (rw *RollingWindow) updateOffset() {
	// 获取当前偏移的增量
	span := rw.span()
	if span <= 0 {
		return
	}

	offset := rw.offset
	// reset expired buckets
	// 重置过期的桶
	// 计算起始的位置
	start := offset + 1
	// 起始位置加上偏移增量，即为要重置的终点位置
	steps := start + span
	// 桶数量初始化时就设定好，当前偏移量，如果超出最大值，则从最开始处进行覆盖重置
	var remainder int
	if steps > rw.size {
		remainder = steps - rw.size
		steps = rw.size
	}

	// reset expired buckets
	for i := start; i < steps; i++ {
		rw.win.resetBucket(i)
	}
	for i := 0; i < remainder; i++ {
		rw.win.resetBucket(i)
	}
	// 重新设置当前偏移量及最后更新时间
	rw.offset = (offset + span) % rw.size
	rw.lastTime = timex.Now()
}

type Bucket struct {
	Sum   float64
	Count int64
}

// sum累加值;count计数
func (b *Bucket) add(v float64) {
	b.Sum += v
	b.Count++
}

// 重置sum和count
func (b *Bucket) reset() {
	b.Sum = 0
	b.Count = 0
}

type window struct {
	buckets []*Bucket
	size    int
}

func newWindow(size int) *window {
	buckets := make([]*Bucket, size)
	for i := 0; i < size; i++ {
		buckets[i] = new(Bucket)
	}
	return &window{
		buckets: buckets,
		size:    size,
	}
}

// 根据offset确定桶的位置，并累加v
func (w *window) add(offset int, v float64) {
	w.buckets[offset%w.size].add(v)
}

// 对[start, start+count)区间的桶进行fn操作
func (w *window) reduce(start, count int, fn func(b *Bucket)) {
	for i := 0; i < count; i++ {
		fn(w.buckets[(start+i)%w.size])
	}
}

// 根据offset重置桶
func (w *window) resetBucket(offset int) {
	w.buckets[offset%w.size].reset()
}

func IgnoreCurrentBucket() RollingWindowOption {
	return func(w *RollingWindow) {
		w.ignoreCurrent = true
	}
}
