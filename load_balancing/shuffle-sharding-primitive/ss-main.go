package main

import (
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

////////////////// SINGLE BACKEND SERVER //////////////

// Backend represents state of 1 HTTP server and has methods to manipulate its state. For eg, to mark whether that server is alive or not.

type Backend struct {
	URL          *url.URL
	Alive        bool
	mux          sync.RWMutex
	ReverseProxy *httputil.ReverseProxy
}

// SetAlive for this backend
func (b *Backend) SetAlive(alive bool) {
	b.mux.Lock()
	b.Alive = alive
	b.mux.Unlock()
}

// IsAlive returns true when backend is alive
func (b *Backend) IsAlive() (alive bool) {
	b.mux.RLock()
	alive = b.Alive
	b.mux.RUnlock()
	return alive
}

// checks whether a backend is Alive by establishing a TCP connection
func (b *Backend) isBackendAlive() bool {
	timeout := 2 * time.Second
	// d := net.Dialer{
	// 	LocalAddr: b.URL.Host,
	// 	Timeout:   timeout,
	// }
	conn, err := net.DialTimeout("tcp", b.URL.Host, timeout)
	// conn, err := d.Dial("tcp", b.URL.Host)
	if err != nil {
		log.Printf("\nSite %s unreachable, error: %s ", b.URL.Host, err.Error())
		return false
	}
	defer conn.Close()
	return true
}

///////////////////  SHARDS //////////////

// Shard represents a collection of HTTP servers. Think of it like drawing a hand of 4 cards from a deck of 52 cards. So, in our case each "hand" of servers is assigned to one client. If a client misbehaves due to which an entire shard goes down, even then others clients wont be affected since no 2 shards have more than 1 overlapping server.

type Shard struct {
	backends      []*Backend
	current       uint64 // Indicates current server (maintains state for round robin)
	InUseByTenant bool   // whether the Shard is assigned to a tenant
	TenantID      string // Tenant to whom this shard is assigned
}

func (s Shard) MarkBackendStatus(status bool) {
	// todo: just a placeholder - revisit & figure out where to put this
	for _, server := range s.backends {
		server.SetAlive(status)
	}
}

func (s Shard) IsShardAlive() bool {
	isAlive := true
	for _, server := range s.backends {
		isAlive = isAlive && server.isBackendAlive()
	}
	return isAlive
}

// HealthCheck pings the Shard's backends and updates the status in each server
func (s *Shard) HealthCheck() {
	isAlive := false
	for _, b := range s.backends {

		isServerAlive := b.isBackendAlive()
		b.SetAlive(isServerAlive)
		isAlive = isAlive || isServerAlive

		// Print this only for debugging
		// if !alive {
		// 	status = "down"
		// }
		// status := "up"
		// log.Printf("Server %s [%s]\n", b.URL, status)
	}
	log.Printf("Shard status: %t\n", isAlive)
}

func (s *Shard) initialiseShard(servers []string) {

	fmt.Printf("s.initialiseShard(): %v", servers)
	for _, server := range servers {
		serverUrl, err := url.Parse(server)
		if err != nil {
			log.Fatal("Unable to parse server URL while initialising shards: ", err)
		}

		proxy := httputil.NewSingleHostReverseProxy(serverUrl)
		proxy.ErrorHandler = func(writer http.ResponseWriter, request *http.Request, e error) {
			log.Printf("[%s] %s\n", serverUrl.Host, e.Error())
			retries := GetRetryFromContext(request)
			if retries < 3 {
				select {
				case <-time.After(10 * time.Millisecond):
					ctx := context.WithValue(request.Context(), Retry, retries+1)
					proxy.ServeHTTP(writer, request.WithContext(ctx))
				}
				return
			}

			// after 3 retries, mark this backend server as down
			s.MarkBackendStatus(false)

			// if the same request routing for few attempts with different backends, increase the count
			attempts := GetAttemptsFromContext(request)
			log.Printf("%s(%s) Attempting retry %d\n", request.RemoteAddr, request.URL.Path, attempts)
			ctx := context.WithValue(request.Context(), Attempts, attempts+1)
			lb(writer, request.WithContext(ctx))
		}

		b := &Backend{
			URL:          serverUrl,
			Alive:        true,
			ReverseProxy: proxy,
		}

		s.backends = append(s.backends, b)
		s.InUseByTenant = false
	}

}

/////  Implementing Roud Robin within a Shard

// This function simply adds 1 to `s.current` inorder to send request in round robin fashion to next server in the shard. This number will be modulo'd in another function to get the next server in the Shard.
// Since a lot of requests can reach a shard simultanesouly, we are using atomic (RWMutex) to safely change this state.
func (s *Shard) NextIndex() int {
	return int(atomic.AddUint64(&s.current, uint64(1)) % uint64(len(s.backends)))
}

// GetNextPeer returns next active peer to take a connection
func (s *Shard) GetNextPeer() *Backend {
	// loop entire backends to find out an Alive backend
	next := s.NextIndex()
	l := len(s.backends) + next // start from next and move a full cycle
	for i := next; i < l; i++ {
		idx := i % len(s.backends) // take an index by modding with length
		// if we have an alive backend, use it and store if its not the original one
		if s.backends[idx].IsAlive() {
			if i != next {
				atomic.StoreUint64(&s.current, uint64(idx)) // change the value to indicate the current healthy server in the shard.
			} 
			return s.backends[idx]
		}
	}
	return nil
}

/////////////////// SHARD POOL  /////////

type ShardPool []Shard

var serverShardPool ShardPool // easiest way to initialise & track state. But not best way. Fix later
var serverList []string
var tenantShardMapping map[string]int // Hashtable | key: tenant-id, value: index of shard on the `ShardList`

// Compute all nCr combinations for 'n' servers & 'r' shardsize
// Inspired by https://leetcode.com/problems/combinations/solutions/794032/python-js-go-by-dfs-backtracking-w-hint/
func generateServerCombos(serverList []string, shardSize int) [][]string {
	result := make([][]string, 0)

	var util_nCr func(start int, curServerCombo []string)
	util_nCr = func(start int, curServerCombo []string) {
		// Basecase: When shard is full
		if len(curServerCombo) == shardSize {
			// make a copy of current combination
			dst := make([]string, shardSize)

			// we do an explicit copy here so that dst & curServerCombo have differnet underlying arrays for their slices (so that any future change to curServerCombo wont affect to dst)
			copy(dst, curServerCombo)

			// fmt.Printf("\nAnother shard found: %v\n", dst)
			result = append(result, dst)
			return
		}

		// Traverse along array & pick servers to construct each combination in nCr
		for i := start; i < len(serverList); i++ {
			curServerCombo = append(curServerCombo, serverList[i])
			util_nCr(i+1, curServerCombo)
			curServerCombo = curServerCombo[:len(curServerCombo)-1] // pop last element
		}
	}

	util_nCr(0, make([]string, 0))
	fmt.Println("Total number of shards: ", len(result))
	// fmt.Printf("\nFinal ShardList: %v", result)

	return result
}

// Initialise a shard with given combination of servers
func (sp *ShardPool) initialiseShardPool(serverList []string, shardSize int) {
	result := generateServerCombos(serverList, shardSize)

	// fmt.Println("\nshard combos: ", result)
	for _, shard := range result {
		fmt.Println(shard)
		var s Shard
		(&s).initialiseShard(shard)
		// fmt.Println("\nShard s after intialisation: ", s)
		*sp = append(*sp, s)
	}

}

func generateHash(tenantID string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(tenantID))
	return h.Sum32()
}

// Function picks a randomly available shard & assigns it to client. (Need to explore if there is a better way to do this)
// NOTE: Alternate implmentation would be to assign an ID to each shard (ID could be based on hash of underlying server addresses/names) and  maintaining the shards in a HashTable (key: shardID, value: `Shard`). But the challenge would arise in assigning a shard to a given client id (right now, we assign based shard to a tenant on slice's index).
func fetchTenantShard(tenantID string, sp ShardPool) int {
	suffix := ""

	// If shard already exists
	if idx, ok := tenantShardMapping[tenantID]; ok {
		fmt.Printf("Tenant already present: %d", idx)
		return tenantShardMapping[tenantID]
	}
	fmt.Printf("\nTenant not found (%d). Creating one", tenantShardMapping[tenantID])
	// If tenant is new & hence new shard needs to be assigned
	for i := 0; i < 10; i++ {

		shard_idx := int(generateHash(tenantID+suffix) % uint32(len(sp)))
		if !sp[shard_idx].InUseByTenant {
			sp[shard_idx].InUseByTenant = true
			sp[shard_idx].TenantID = tenantID
			tenantShardMapping[tenantID] = shard_idx
			return shard_idx
		} else {
			suffix = fmt.Sprint(i)
			fmt.Printf("\nShard ID collision for tenant %s. So, concatenate %s suffix to generate tenant ID again", tenantID, suffix)
		}
	}
	return -1 // too many collisions; consider different way of mapping
}

func (sp ShardPool) HealthCheck() {
	for _, shard := range sp {
		shard.HealthCheck()
	}
}

// https://boltandnuts.wordpress.com/2017/11/20/go-slice-vs-maps/

////////////// LOAD BALANCER  ////////////////

// We use `context` pkg to track request specific data such as Attempt count and Retry count.
// First, we need to specify keys for the context. It is recommended to use non-colliding integer keys rather than strings. Go provides iota keyword to implement constants incrementally, each containing a unique value. That is a perfect solution defining integer keys.
const (
	Attempts int = iota
	Retry
)

// GetAttemptsFromContext returns the attempts for request
func GetAttemptsFromContext(r *http.Request) int {
	if attempts, ok := r.Context().Value(Attempts).(int); ok {
		return attempts
	}
	return 1
}

// Then we can retrieve the value as usually we do with a HashMap like follows. The default return value may depend on the use case.
func GetRetryFromContext(r *http.Request) int {
	if retry, ok := r.Context().Value(Retry).(int); ok {
		return retry
	}
	return 0
}

// healthCheck runs a routine for check status of the shards of a shardpool every 2 mins
func healthCheckCron(sp ShardPool) {
	t := time.NewTicker(time.Second * 30)
	for {
		select {
		case <-t.C:
			log.Println("Starting Shardpool Healthcheck...")
			sp.HealthCheck()
			log.Println("Completed Shardpool Healthcheck")
		}
	}
}

// just a placeholder
func lb(w http.ResponseWriter, r *http.Request) {
	attempts := GetAttemptsFromContext(r)
	if attempts > 3 {
		log.Printf("%s(%s) Max attempts reached, terminating\n", r.RemoteAddr, r.URL.Path)
		http.Error(w, "Service not available", http.StatusServiceUnavailable)
		return
	}

	tenant := r.Header.Get("tenant")
	fmt.Printf("\nRequest made by tenant: %s\n ", tenant)
	shardIdx := fetchTenantShard(tenant, serverShardPool) //todo: Referencing global variable not a good idea, fix it later
	shard := &serverShardPool[shardIdx]

	server := shard.GetNextPeer()
	if server != nil {
		server.ReverseProxy.ServeHTTP(w, r)
		return
	}
	http.Error(w, "Service not available (all backends down)", http.StatusServiceUnavailable)
}

//////////////////////////////

func init() {
	tenantShardMapping = make(map[string]int)
}

func main() {
	var backends string
	var port, shardSize int

	flag.StringVar(&backends, "backends", "", "Load balanced backends, use commas to separate")
	flag.IntVar(&port, "port", 3030, "Port to serve")
	flag.IntVar(&shardSize, "shardsize", -1, "Number of servers per shard")
	flag.Parse()

	if len(backends) == 0 {
		log.Fatal("Please provide one or more backends to load balance")
	}
	if shardSize <= 1 {
		log.Fatal("Pls provide a shardSize > 1")
	}
	serverList = strings.Split(backends, ",")
	// fmt.Printf("\nLength: %d, Server list: %v\n", len(serverList), serverList)

	(&serverShardPool).initialiseShardPool(serverList, shardSize)

	fmt.Printf("\nserverShardPool: len=%d cap=%d", len(serverShardPool), cap(serverShardPool))
	lbServer := http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: http.HandlerFunc(lb),
	}

	go healthCheckCron(serverShardPool)

	log.Printf("Load Balancer started at :%d\n", port)
	if err := lbServer.ListenAndServe(); err != nil {
		log.Fatal(err)
	}

}




