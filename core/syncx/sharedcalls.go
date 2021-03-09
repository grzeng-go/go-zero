package syncx

import "sync"

type (
	// SharedCalls lets the concurrent calls with the same key to share the call result.
	// For example, A called F, before it's done, B called F. Then B would not execute F,
	// and shared the result returned by F which called by A.
	// The calls with the same key are dependent, concurrent calls share the returned values.
	// A ------->calls F with key<------------------->returns val
	// B --------------------->calls F with key------>returns val
	// 调用某个方法，如果发现当前已经有人在调用的话，则不会调用该方法，而是与当前调用的共享结果
	SharedCalls interface {
		Do(key string, fn func() (interface{}, error)) (interface{}, error)
		DoEx(key string, fn func() (interface{}, error)) (interface{}, bool, error)
	}

	call struct {
		wg  sync.WaitGroup
		val interface{}
		err error
	}

	sharedGroup struct {
		calls map[string]*call
		lock  sync.Mutex
	}
)

// NewSharedCalls returns a SharedCalls.
func NewSharedCalls() SharedCalls {
	return &sharedGroup{
		calls: make(map[string]*call),
	}
}

func (g *sharedGroup) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	c, done := g.createCall(key)
	if done {
		return c.val, c.err
	}

	// 如果当前没有其他人在执行的话，则创建个key-call，让后续的请求不要直接执行，然后执行调用，并返回结果
	g.makeCall(c, key, fn)
	return c.val, c.err
}

// 与Do具备相同功能，只是增加了fresh返回值，用来判断是直接执行了方法，还是返回别人执行的结果
func (g *sharedGroup) DoEx(key string, fn func() (interface{}, error)) (val interface{}, fresh bool, err error) {
	c, done := g.createCall(key)
	if done {
		return c.val, false, c.err
	}

	g.makeCall(c, key, fn)
	return c.val, true, c.err
}

func (g *sharedGroup) createCall(key string) (c *call, done bool) {
	g.lock.Lock()
	// 判断当前是否已经有人在执行该调用， 如果有的话，阻塞直到对方执行完后，直接获取它执行的结果
	if c, ok := g.calls[key]; ok {
		g.lock.Unlock()
		c.wg.Wait()
		return c, true
	}

	// 创建一个call，用来保存执行结果或异常
	c = new(call)
	// 给call中的WaitGroup +1，后面调用的以此来进行阻塞
	c.wg.Add(1)
	// 将key-call保存起来，后来的调用，根据key获取call
	g.calls[key] = c
	g.lock.Unlock()

	return c, false
}

func (g *sharedGroup) makeCall(c *call, key string, fn func() (interface{}, error)) {
	defer func() {
		// delete key first, done later. can't reverse the order, because if reverse,
		// another Do call might wg.Wait() without get notified with wg.Done()
		// 执行结束后，要先删除key-call，确保后续进来的请求不会在被阻塞，然后在释放WaitGroup
		g.lock.Lock()
		delete(g.calls, key)
		g.lock.Unlock()
		c.wg.Done()
	}()
	// 执行方法， 将结果及异常保存到call中
	c.val, c.err = fn()
}
