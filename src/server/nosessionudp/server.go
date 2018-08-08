package nosessionudp

import (
	"bytes"
	"net"
	"sync"
	"time"

	"../../balance"
	"../../config"
	"../../core"
	"../../discovery"
	"../../healthcheck"
	"../../logging"
	"../../stats"
	"../modules/access"
	"../scheduler"
)

const UDP_PACKET_SIZE = 65507

/**
 * UDP server implementation
 */
type Server struct {

	/* Server name */
	name string

	/* Server configuration */
	cfg config.Server

	/* Scheduler */
	scheduler *scheduler.Scheduler

	/* Stats handler */
	statsHandler *stats.Handler

	/* Server connection */
	serverConn *net.UDPConn

	connections map[core.Target]connection
	bufPool     sync.Pool

	/* Access module checks if client is allowed to connect */
	access *access.Access
}

type connection struct {
	conn    *net.UDPConn
	backend *core.Backend
}

/**
 * Creates new NoSessionUDP server
 */
func New(name string, cfg config.Server) (*Server, error) {

	log := logging.For("nosessionudp/server")

	statsHandler := stats.NewHandler(name)
	scheduler := &scheduler.Scheduler{
		Balancer:     balance.New(nil, cfg.Balance),
		Discovery:    discovery.New(cfg.Discovery.Kind, *cfg.Discovery),
		Healthcheck:  healthcheck.New(cfg.Healthcheck.Kind, *cfg.Healthcheck),
		StatsHandler: statsHandler,
	}

	server := &Server{
		name:         name,
		cfg:          cfg,
		scheduler:    scheduler,
		statsHandler: statsHandler,
		connections:  make(map[core.Target]connection),
		bufPool:      sync.Pool{New: func() interface{} { return new(bytes.Buffer) }},
	}

	/* Add access if needed */
	if cfg.Access != nil {
		access, err := access.NewAccess(cfg.Access)
		if err != nil {
			return nil, err
		}
		server.access = access
	}

	log.Info("Creating NoSessionUDP server '", name, "': ", cfg.Bind, " ", cfg.Balance, " ", cfg.Discovery.Kind, " ", cfg.Healthcheck.Kind)
	return server, nil
}

/**
 * Returns current server configuration
 */
func (this *Server) Cfg() config.Server {
	return this.cfg
}

/**
 * Starts server
 */
func (this *Server) Start() error {

	log := logging.For("nosessionudp/server")

	this.statsHandler.Start()
	this.scheduler.Start()

	// Start listening
	if err := this.listen(); err != nil {
		this.Stop()
		log.Error("Error starting UDP Listen ", err)
		return err
	}

	interval, err := time.ParseDuration(this.cfg.Discovery.Interval)
	if err != nil {
		log.Fatal(err)
	}

	/* deal with backends updates */
	go func() {
		for {
			bem := this.scheduler.BackendsMap()

			/* process NEW backends */
			for _, be := range bem {
				if _, ok := this.connections[be.Target]; !ok {
					backendAddr, err := net.ResolveUDPAddr("udp", be.Target.String())
					if err != nil {
						log.Error("Error ResolveUDPAddr: ", err)
						continue
					}
					backendConn, err := net.DialUDP("udp", nil, backendAddr)
					if err != nil {
						log.Debug("Error connecting to backend: ", err)
						continue
					}
					this.connections[be.Target] = connection{conn: backendConn, backend: be}
					log.Info("Created new UDP connection to ", be.Target.String())
				}
			}

			/* process REMOVED backends */
			for target, conn := range this.connections {
				if _, ok := bem[target]; !ok {
					conn.conn.Close()
					delete(this.connections, target)
					log.Info("Closed UDP connection to ", target.String())
				}
			}

			time.Sleep(interval)
		}
	}()

	return nil
}

/**
 * Start accepting connections
 */
func (this *Server) listen() error {

	log := logging.For("nosessionudp/server")

	listenAddr, err := net.ResolveUDPAddr("udp", this.cfg.Bind)
	if err != nil {
		log.Error("Error resolving server bind addr ", err)
		return err
	}

	this.serverConn, err = net.ListenUDP("udp", listenAddr)

	if err != nil {
		log.Error("Error starting UDP server: ", err)
		return err
	}

	// Main proxy loop goroutine
	go func() {
		index := uint64(0)
		buf := make([]byte, UDP_PACKET_SIZE)
		for {
			n, _, err := this.serverConn.ReadFromUDP(buf)

			if err != nil {
				log.Error("Error ReadFromUDP: ", err)
				continue
			}

			b := this.bufPool.Get().(*bytes.Buffer)
			b.Reset()
			b.Write(buf[:n])

			log.Debug("Got UPD packet")
			go func(buf *bytes.Buffer, idx uint64) {
				defer this.bufPool.Put(buf)
				conn := this.selectConnection(idx)
				if conn == nil {
					log.Error("No available backend connections")
					return
				}
				_, err = conn.conn.Write(buf.Bytes())
				if err != nil {
					log.Error("Error sending data to backend: ", err)
					return
				}

				log.Debug("Sent UDP packet to ", conn.backend.Target.String())
				this.scheduler.IncrementTx(*conn.backend, 1)

			}(b, index)

			index += 1
		}
	}()

	return nil
}

/**
 * Stop, dropping all connections
 */
func (this *Server) Stop() {
	log := logging.For("nosessionudp/server")
	log.Info("Stopping ", this.name)

	this.serverConn.Close()

	this.scheduler.Stop()
	this.statsHandler.Stop()

	for _, conn := range this.connections {
		conn.conn.Close()
	}
}

func (this *Server) selectConnection(idx uint64) *connection {
	maxconn := uint64(len(this.connections))
	if maxconn == 0 {
		return nil
	}
	selector := idx % maxconn
	var conn connection
	for _, conn = range this.connections {
		if selector == 0 {
			break
		}
		selector--
	}
	return &conn
}
