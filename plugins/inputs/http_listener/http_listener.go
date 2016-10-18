package http_listener

import (
	"bytes"
	"compress/gzip"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/inputs/http_listener/stoppableListener"
	"github.com/influxdata/telegraf/plugins/parsers"
)

const (
	// DEFAULT_REQUEST_BODY_MAX is the default maximum request body size, in bytes.
	// if the request body is over this size, we will return an HTTP 413 error.
	// 512 MB
	DEFAULT_REQUEST_BODY_MAX = 512 * 1000 * 1000

	// MAX_LINE_SIZE is the maximum size, in bytes, that can be allocated for
	// a single InfluxDB point.
	// 1 MB
	MAX_LINE_SIZE = 1 * 1000 * 1000
)

type HttpListener struct {
	ServiceAddress string
	ReadTimeout    internal.Duration
	WriteTimeout   internal.Duration
	MaxBodySize    int64

	sync.Mutex

	listener *stoppableListener.StoppableListener

	parser parsers.Parser
	acc    telegraf.Accumulator
	pool   *pool
}

const sampleConfig = `
  ## Address and port to host HTTP listener on
  service_address = ":8186"

  ## maximum duration before timing out read of the request
  read_timeout = "10s"
  ## maximum duration before timing out write of the response
  write_timeout = "10s"

  ## Maximum allowed http request body size in bytes.
  ## 0 means to use the default of 1,000,000,000 bytes (1 gigabyte)
  max_body_size = 0
`

func (t *HttpListener) SampleConfig() string {
	return sampleConfig
}

func (t *HttpListener) Description() string {
	return "Influx HTTP write listener"
}

func (t *HttpListener) Gather(_ telegraf.Accumulator) error {
	return nil
}

func (t *HttpListener) SetParser(parser parsers.Parser) {
	t.parser = parser
}

// Start starts the http listener service.
func (t *HttpListener) Start(acc telegraf.Accumulator) error {
	t.Lock()
	defer t.Unlock()

	if t.MaxBodySize == 0 {
		t.MaxBodySize = DEFAULT_REQUEST_BODY_MAX
	}

	t.acc = acc
	t.pool = NewPool(100)

	var rawListener, err = net.Listen("tcp", t.ServiceAddress)
	if err != nil {
		return err
	}
	t.listener, err = stoppableListener.New(rawListener)
	if err != nil {
		return err
	}

	go t.httpListen()

	log.Printf("I! Started HTTP listener service on %s\n", t.ServiceAddress)

	return nil
}

// Stop cleans up all resources
func (t *HttpListener) Stop() {
	t.Lock()
	defer t.Unlock()

	t.listener.Stop()
	t.listener.Close()

	log.Println("I! Stopped HTTP listener service on ", t.ServiceAddress)
}

// httpListen listens for HTTP requests.
func (t *HttpListener) httpListen() error {
	if t.ReadTimeout.Duration < time.Second {
		t.ReadTimeout.Duration = time.Second * 10
	}
	if t.WriteTimeout.Duration < time.Second {
		t.WriteTimeout.Duration = time.Second * 10
	}

	var server = http.Server{
		Handler:      t,
		ReadTimeout:  t.ReadTimeout.Duration,
		WriteTimeout: t.WriteTimeout.Duration,
	}

	return server.Serve(t.listener)
}

func (t *HttpListener) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	switch req.URL.Path {
	case "/write":
		t.serveWrite(res, req)
	case "/query":
		// Deliver a dummy response to the query endpoint, as some InfluxDB
		// clients test endpoint availability with a query
		res.Header().Set("Content-Type", "application/json")
		res.Header().Set("X-Influxdb-Version", "1.0")
		res.WriteHeader(http.StatusOK)
		res.Write([]byte("{\"results\":[]}"))
	case "/ping":
		// respond to ping requests
		res.WriteHeader(http.StatusNoContent)
	default:
		// Don't know how to respond to calls to other endpoints
		http.NotFound(res, req)
	}
}

func (t *HttpListener) serveWrite(res http.ResponseWriter, req *http.Request) {
	// Check that the content length is not too large for us to handle.
	if req.ContentLength > t.MaxBodySize {
		toolarge(res)
		return
	}

	// Handle gzip request bodies
	var body io.ReadCloser
	if req.Header.Get("Content-Encoding") == "gzip" {
		r, err := gzip.NewReader(req.Body)
		defer r.Close()
		if err != nil {
			log.Println("E! " + err.Error())
			badrequest(res)
			return
		}
		body = http.MaxBytesReader(res, r, t.MaxBodySize)
	} else {
		body = http.MaxBytesReader(res, req.Body, t.MaxBodySize)
	}

	var return400 bool
	var buf []byte
	bufstart := 0
	for {
		if bufstart == 0 {
			buf = t.pool.get()
		}
		n, err := io.ReadFull(body, buf[bufstart:])

		if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
			log.Println("E! " + err.Error())
			// problem reading the request body
			badrequest(res)
			t.pool.put(buf)
			return
		}

		if err == io.ErrUnexpectedEOF || err == io.EOF || n == 0 {
			// finished reading the request body
			if err := t.parse(buf[:n+bufstart]); err != nil {
				log.Println("E! " + err.Error())
				return400 = true
			}
			t.pool.put(buf)
			if return400 {
				badrequest(res)
			} else {
				res.WriteHeader(http.StatusNoContent)
			}
			return
		}

		// if we got down here it means that we filled our buffer, and there
		// are still bytes remaining to be read. So we will parse up until the
		// final newline, then push the rest of the bytes into the next buffer.
		i := bytes.LastIndexByte(buf, '\n')
		if i == -1 {
			// drop any line longer than the max buffer size
			t.pool.put(buf)
			newlinei := findnewline(body)
			log.Printf("E! http_listener received a single line of %d bytes, maximum is %d bytes",
				MAX_LINE_SIZE+newlinei, MAX_LINE_SIZE)
			return400 = true
			continue
		}
		if err := t.parse(buf[:i]); err != nil {
			log.Println("E! " + err.Error())
			return400 = true
		}
		// rotate the bit remaining after the last newline to the front of the buffer
		bufstart = len(buf) - i
		copy(buf[:bufstart], buf[i:])
	}
}

func (t *HttpListener) parse(b []byte) error {
	metrics, err := t.parser.Parse(b)

	for _, m := range metrics {
		t.acc.AddFields(m.Name(), m.Fields(), m.Tags(), m.Time())
	}

	return err
}

func toolarge(res http.ResponseWriter) {
	res.Header().Set("Content-Type", "application/json")
	res.Header().Set("X-Influxdb-Version", "1.0")
	res.WriteHeader(http.StatusRequestEntityTooLarge)
	res.Write([]byte(`{"error":"http: request body too large"}`))
}

func badrequest(res http.ResponseWriter) {
	res.Header().Set("Content-Type", "application/json")
	res.Header().Set("X-Influxdb-Version", "1.0")
	res.WriteHeader(http.StatusBadRequest)
	res.Write([]byte(`{"error":"http: bad request"}`))
}

// findnewline finds the next newline in the given reader. It returns the number
// of bytes it had to read to get there.
func findnewline(r io.Reader) int {
	counter := 0
	// read until the next newline:
	var tmp [1]byte
	for {
		_, err := r.Read(tmp[:])
		if err != nil {
			break
		}
		counter++
		if tmp[0] == '\n' {
			break
		}
	}
	return counter
}

func init() {
	inputs.Add("http_listener", func() telegraf.Input {
		return &HttpListener{}
	})
}
