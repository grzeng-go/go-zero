package breaker

import (
	"math"
	"time"

	"github.com/tal-tech/go-zero/core/collection"
	"github.com/tal-tech/go-zero/core/mathx"
)

const (
	// 250ms for bucket duration
	// 移动窗口中每个桶的时间间隔为 10s/40 = 250ms
	window     = time.Second * 10
	buckets    = 40
	k          = 1.5
	protection = 5
)

// googleBreaker is a netflixBreaker pattern from google.
// see Client-Side Throttling section in https://landing.google.com/sre/sre-book/chapters/handling-overload/
type googleBreaker struct {
	// 阈值，用于计算max(0, (request - k * accept)/request + 1)，K越大表示能容忍的异常请求越大
	k float64
	// 熔断器状态  0为关闭；1为开启 当熔断器开启时，则表示当前服务异常
	//state int32
	// rollingWindow用来保存，过去10s时间内发生的所有请求及成功的请求
	stat *collection.RollingWindow
	// 概率通过，当熔断时，通过该工具类实现，放一小部分流量进行访问后端，其他一概拒绝请求
	proba *mathx.Proba
}

func newGoogleBreaker() *googleBreaker {
	bucketDuration := time.Duration(int64(window) / int64(buckets))
	st := collection.NewRollingWindow(buckets, bucketDuration)
	return &googleBreaker{
		stat:  st,
		k:     k,
		//state: StateClosed,
		proba: mathx.NewProba(),
	}
}

// 判断是否接受请求
func (b *googleBreaker) accept() error {
	// 获取成功请求数、总请求数
	accepts, total := b.history()
	// 计算权重数值（k*accepts）
	weightedAccepts := b.k * float64(accepts)
	// https://landing.google.com/sre/sre-book/chapters/handling-overload/#eq2101
	// 根据公式计算请求拒绝概率
	dropRatio := math.Max(0, (float64(total-protection)-weightedAccepts)/float64(total+1))
	// 当拒绝概率小于等于0时，如果熔断器为开启状态，则将其关闭，然后返回
	if dropRatio <= 0 {
		/*if atomic.LoadInt32(&b.state) == StateOpen {
			atomic.CompareAndSwapInt32(&b.state, StateOpen, StateClosed)
		}*/
		return nil
	}

	/*if atomic.LoadInt32(&b.state) == StateClosed {
		atomic.CompareAndSwapInt32(&b.state, StateClosed, StateOpen)
	}*/
	if b.proba.TrueOnProba(dropRatio) {
		return ErrServiceUnavailable
	}

	return nil
}

func (b *googleBreaker) allow() (internalPromise, error) {
	// 判断请求是否放行
	if err := b.accept(); err != nil {
		return nil, err
	}

	// 如果请求被放行的话，则返回一个Promise
	return googlePromise{
		b: b,
	}, nil
}

func (b *googleBreaker) doReq(req func() error, fallback func(err error) error, acceptable Acceptable) error {
	// 判断请求是否放行，如果返回异常，则判断fallback是否有传，有传的话则调用fallback处理异常，没有的话，直接返回异常
	if err := b.accept(); err != nil {
		if fallback != nil {
			return fallback(err)
		}

		return err
	}

	defer func() {
		// 如果出现panic异常，则覆盖panic，标记失败，然后重新panic异常
		if e := recover(); e != nil {
			b.markFailure()
			panic(e)
		}
	}()

	// 执行请求
	err := req()
	// 通过acceptable判断请求返回的异常，返回true的话，则标记成功，反之标记失败
	if acceptable(err) {
		b.markSuccess()
	} else {
		b.markFailure()
	}

	return err
}

// 标记请求成功（即往移动窗口增加1）
func (b *googleBreaker) markSuccess() {
	b.stat.Add(1)
}

// 标记请求失败（即往移动窗口增加0）
func (b *googleBreaker) markFailure() {
	b.stat.Add(0)
}

// 统计10s内，所有成功的请求以及总的请求数
func (b *googleBreaker) history() (accepts int64, total int64) {
	b.stat.Reduce(func(b *collection.Bucket) {
		accepts += int64(b.Sum)
		total += b.Count
	})

	return
}

type googlePromise struct {
	b *googleBreaker
}

func (p googlePromise) Accept() {
	p.b.markSuccess()
}

func (p googlePromise) Reject() {
	p.b.markFailure()
}
