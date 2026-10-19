package yellowdog

import (
	"context"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"
)

// The pattern the platform matches to identify a client; a header that does not match it leaves
// the client unidentified.
var platformUserAgent = regexp.MustCompile(`yd/(\d+)\.(\d+)\.(\d+) ([^ /]+)/(\d+)\.(\d+)\.(\d+) ([^ /]+)/(.+)`)

func TestDefaultUserAgentMatchesThePlatformFormat(t *testing.T) {
	match := platformUserAgent.FindStringSubmatch(DefaultUserAgent())
	if match == nil {
		t.Fatalf("DefaultUserAgent() = %q, which the platform cannot parse", DefaultUserAgent())
	}
	if match[4] != "yellowdog-sdk-go" || match[8] != "go" {
		t.Errorf("DefaultUserAgent() = %q, want application yellowdog-sdk-go and language go", DefaultUserAgent())
	}
}

func TestRequestsSendTheUserAgent(t *testing.T) {
	cases := []struct {
		name      string
		userAgent string
		want      string
	}{
		{"default when unset", "", DefaultUserAgent()},
		{"configured value", "my-tool/2.0.0", "my-tool/2.0.0"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var got string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got = r.Header.Get("User-Agent")
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"id":"ns-1","namespace":"example"}`))
			}))
			defer server.Close()

			client, err := NewClient(&Config{BaseURL: server.URL, APIKey: "key", APISecret: "secret", UserAgent: c.userAgent})
			if err != nil {
				t.Fatalf("NewClient: %v", err)
			}
			defer func() { _ = client.Close() }()

			if _, err := client.Namespaces.GetNamespace(context.Background(), "ns-1"); err != nil {
				t.Fatalf("GetNamespace: %v", err)
			}
			if got != c.want {
				t.Errorf("User-Agent = %q, want %q", got, c.want)
			}
		})
	}
}
