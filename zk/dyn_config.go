package zk

import (
	"bytes"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

const DynConfPath = "/zookeeper/config"

type DynConf struct {
	Version int64
	Servers []ZkServer
}

func (cfg *DynConf) ClientAddrs() []string {
	addrs := make([]string, 0, len(cfg.Servers))
	for _, sc := range cfg.Servers {
		if sc.Type == TypeParticipant {
			addrs = append(addrs, fmt.Sprintf("%s:%d", sc.ClientHost, sc.ClientPort))
		}
	}
	return addrs
}

type ZkServerType string

const (
	TypeParticipant = ZkServerType("participant")
	TypeObserver    = ZkServerType("observer")
)

type ZkServer struct {
	ServerConfigServer // Love this name.
	ClientHost         string
	ClientPort         int
	Type               ZkServerType
}

// Parse the dynamic config node for Zookeeper.
func ParseDynConfig(data []byte) (*DynConf, error) {
	cfg := &DynConf{Servers: make([]ZkServer, 0, 16)}
	for i, ln := range bytes.Split(data, []byte("\n")) {
		lineno := i + 1
		line := string(ln)
		var err error
		if strings.HasPrefix(line, "version=") {
			cfg.Version, err = strconv.ParseInt(line[len("version="):], 16, 64)
		} else if strings.HasPrefix(line, "server.") {
			sc := ZkServer{}
			scanLine := strings.Map(func(r rune) rune {
				switch r {
				case '=', ':', ';':
					return ' '
				}
				return r
			}, line[len("server."):])
			n, scanErr := fmt.Sscanf(scanLine, "%d %s %d %d %s %s %d",
				&sc.ID, &sc.Host, &sc.PeerPort, &sc.LeaderElectionPort, &sc.Type, &sc.ClientHost, &sc.ClientPort)
			if scanErr != nil {
				err = scanErr
			} else if n != 7 {
				err = fmt.Errorf("found %v of 7 fields", n)
			}
			cfg.Servers = append(cfg.Servers, sc)
		}
		if err != nil {
			return nil, fmt.Errorf("unable to parse zk dynamic config line %d: %v\n\t%q", lineno, err, line)
		}
	}
	return cfg, nil
}

// TODO(msolo) Exposing failure stats in a structured way rathern than
// simple log output would make it easier to monitor issues at scale.
func (c *Conn) handleReconfig() {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	for {
		// Don't stampede, reconfig is an optimization, not for correctness.
		time.Sleep(time.Duration(rnd.Int63n(int64(c.sessionTimeoutMs) * 1e6)))
		data, _, evtCh, err := c.GetW(DynConfPath)
		if err == ErrClosing || err == ErrSessionExpired {
			return
		}
		if err == nil {
			cfg, cfgErr := ParseDynConfig(data)
			if cfgErr != nil {
				err = cfgErr
			} else {
				// If this re-init fails, the existing host list is unharmed.
				if err = c.hostProvider.Init(cfg.ClientAddrs()); err == nil {
					c.logger.Printf("dynamic config updated: %s", strings.Join(cfg.ClientAddrs(), ","))
				}
			}
		}
		if err != nil {
			c.logger.Printf("dynamic config failed: %s", err)
			continue
		}
		evt := <-evtCh
		if evt.Type == EventNotWatching && c.closeOnSessionExpiration {
			// If the client has been closed or expired, just exit.
			return
		}
		// After any other event, sleep a bit and re-read.
	}
}

// When connected to a Zookeeper 3.5.x quorum supporting dynamic
// reconfiguration, this option keeps the host provider up-to-date by
// watching the config node found at DynConfPath.
func WithDynamicReconfig() connOption {
	return func(c *Conn) {
		go c.handleReconfig()
	}
}
