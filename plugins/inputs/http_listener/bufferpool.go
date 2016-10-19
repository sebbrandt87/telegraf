package http_listener

type pool struct {
	bigBuffers   chan []byte
	smallBuffers chan []byte
}

func NewPool(n int) *pool {
	p := &pool{
		bigBuffers:   make(chan []byte, 30),
		smallBuffers: make(chan []byte, 500),
	}
	for i := 0; i < 30; i++ {
		p.bigBuffers <- make([]byte, MAX_LINE_SIZE)
	}
	for i := 0; i < 500; i++ {
		p.smallBuffers <- make([]byte, 50*1000)
	}
	return p
}

func (p *pool) get(maxSize int64) []byte {
	switch {
	case maxSize <= 50*1000:
		select {
		case b := <-p.smallBuffers:
			return b
		default:
			// pool is empty, so make a new buffer
			println("make new SMALL")
			return make([]byte, 50*1000)
		}
	default:
		select {
		case b := <-p.bigBuffers:
			return b
		default:
			// pool is empty, so make a new buffer
			println("make new BIG")
			return make([]byte, MAX_LINE_SIZE)
		}
	}
}

func (p *pool) put(b []byte) {
	switch {
	case len(b) <= 50*1000:
		select {
		case p.smallBuffers <- b:
		default:
			// the pool is full, so drop this buffer
		}
	default:
		select {
		case p.bigBuffers <- b:
		default:
			// the pool is full, so drop this buffer
		}
	}
}
