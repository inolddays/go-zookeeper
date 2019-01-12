package zk

import (
	"testing"
	"time"
)

// TestSimpleDNSHostProviderRetryStart tests the `retryStart` functionality
// matches that of DNSHostProvider.
//
// SimpleDNSHostProvider preserves the existing behavior for now, but
// it's not entirely clear this is the correct call. The major side
// effect of the `retryStart` functionality is that outstanding
// operations can fast-fail when none of the zk servers can be reached
// - even if this is intermittent.  This behavior can be problematic
// for programs using this API since it requires retry loops in user code
// to make an operation succeed.
func TestSimpleDNSHostProviderRetryStart(t *testing.T) {
	t.Parallel()

	hp := &SimpleDNSHostProvider{lookupHost: func(host string) ([]string, error) {
		return []string{"192.0.2.1"}, nil
	}}

	if err := hp.Init([]string{"foo.example.com:12345", "bar.example.com:12345", "baz.example.com:12345"}); err != nil {
		t.Fatal(err)
	}

	testDNSHostProviderRetryStart(t, hp)
}

// TestDNSHostProviderReconnect tests that the zk.Conn correctly
// reconnects when the Zookeeper instance it's connected to
// restarts. It wraps the DNSHostProvider in a lightweight facade that
// remaps addresses to localhost:$PORT combinations corresponding to
// the test ZooKeeper instances.
func TestSimpleDNSHostProviderReconnect(t *testing.T) {
	ts, err := StartTestCluster(3, nil, logWriter{t: t, p: "[ZKERR] "})
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Stop()

	innerHp := &SimpleDNSHostProvider{lookupHost: func(host string) ([]string, error) {
		return []string{host}, nil
	}}
	ports := make([]int, 0, len(ts.Servers))
	for _, server := range ts.Servers {
		ports = append(ports, server.Port)
	}
	hp := newLocalHostPortsFacade(innerHp, ports)
	addrs := []string{"foo.example.com:12345", "bar.example.com:12345", "baz.example.com:12345"}
	zk, _, err := Connect(addrs, time.Second, WithHostProvider(hp))
	if err != nil {
		t.Fatalf("Connect returned error: %+v", err)
	}
	defer zk.Close()

	testDNSHostProviderReconnect(t, ts, zk)
}
