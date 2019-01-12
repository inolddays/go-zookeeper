package zk

import (
	"math/rand"
	"net"
	"sync"
	"time"
)

// SimpleDNSHostProvider implements a shuffled list of DNS entries
// that are resolved on demand. This allows easy swapping of server
// hosts without restarting programs using this host provider.
type SimpleDNSHostProvider struct {
	mu         sync.Mutex // Protects everything, so we can add asynchronous updates later.
	servers    []string
	curr       int
	attempt    int
	lookupHost func(string) ([]string, error) // Override of net.LookupHost, for testing.
}

// Init is called first, with the servers specified in the connection
// string. It uses DNS to look up addresses for each server on demand.
// It assumes that each DNS entry resolves to exactly one address.
func (hp *SimpleDNSHostProvider) Init(servers []string) error {
	hp.mu.Lock()
	defer hp.mu.Unlock()
	hp.servers = append([]string{}, servers...)
	// Randomize the order of the servers to avoid creating hotspots
	hp.curr = rand.New(rand.NewSource(time.Now().UnixNano())).Intn(len(hp.servers))
	return nil
}

func (hp *SimpleDNSHostProvider) resolveAddr(addr string) (string, error) {
	lookupHost := hp.lookupHost
	if lookupHost == nil {
		lookupHost = net.LookupHost
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}
	addrs, err := lookupHost(host)
	if err != nil {
		return "", err
	}
	// We intentionally return only the first addr resolved for an A/AAAA record.
	return net.JoinHostPort(addrs[0], port), nil
}

// Next returns the next server to connect to. retryStart will be true
// if we've looped through all known servers without Connected() being
// called.
func (hp *SimpleDNSHostProvider) Next() (server string, retryStart bool) {
	hp.mu.Lock()
	defer hp.mu.Unlock()
	hp.curr = (hp.curr + 1) % len(hp.servers)
	hp.attempt++
	if hp.attempt > len(hp.servers) {
		retryStart = true
		hp.attempt = 1
	}
	addr, err := hp.resolveAddr(hp.servers[hp.curr])
	if err != nil {
		// There isn't a better way to handle an error in the current API,
		// so return an empty address to fast fail in connect.
		return "", retryStart
	}
	return addr, retryStart
}

// Connected notifies the HostProvider of a successful connection.
func (hp *SimpleDNSHostProvider) Connected() {
	hp.mu.Lock()
	defer hp.mu.Unlock()
	hp.attempt = 1
}

// Len returns the number of servers available
func (hp *SimpleDNSHostProvider) Len() int {
	hp.mu.Lock()
	defer hp.mu.Unlock()
	return len(hp.servers)
}
