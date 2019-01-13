package zk

import (
	"reflect"
	"testing"
)

var sampleData = `server.1=127.0.0.1:3381:3481:participant;127.0.0.1:3181
server.2=127.0.0.1:3382:3482:participant;127.0.0.1:3182
server.3=127.0.0.1:3383:3483:participant;127.0.0.1:3183
version=100000000
`

func TestDynConf(t *testing.T) {
	dynConf, err := ParseDynConfig([]byte(sampleData))
	if err != nil {
		t.Errorf("ParseDynConfig failed: %s", err)
	}
	wantClientAddrs := []string{"127.0.0.1:3181", "127.0.0.1:3182", "127.0.0.1:3183"}
	if !reflect.DeepEqual(dynConf.ClientAddrs(), wantClientAddrs) {
		t.Errorf("ParseDynConfig ClientAddrs failed: %q != %q", dynConf.ClientAddrs(), wantClientAddrs)
	}
	t.Logf("dynConf: #%v", dynConf)
}
