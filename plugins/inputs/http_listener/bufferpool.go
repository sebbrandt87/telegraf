package http_listener

type pool struct {
	buffers chan []byte
}

func NewPool(n int) *pool {
	p := &pool{
		buffers: make(chan []byte, n),
	}
	for i := 0; i < n; i++ {
		p.buffers <- make([]byte, MAX_LINE_SIZE)
	}
	return p
}

func (p *pool) get() []byte {
	select {
	case b := <-p.buffers:
		return b
	default:
		// pool is empty, so make a new buffer
		return make([]byte, MAX_LINE_SIZE)
	}
}

func (p *pool) put(b []byte) {
	select {
	case p.buffers <- b:
	default:
		// the pool is full, so drop this buffer
		b = nil
	}
}
