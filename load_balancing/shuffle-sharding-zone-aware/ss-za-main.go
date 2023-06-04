package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"math"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
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
	AZ           string // Availability Zone for the server
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

func (s *Shard) initialiseShard(servers []map[string]string) {

	// fmt.Printf("s.initialiseShard(): %v", servers)
	for _, serverObj := range servers {
		server := serverObj["server"]
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
			AZ:           serverObj["az"],
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

// Uses K8s definition of `maxSkewness`
// NOTE: This function used to check skewness only. But in future, it can be modified to check for other constraints as well to decide if a given combination is a suitable shard.
func maxSkewSatisfied(combo []map[string]string, azCount map[string]int, maxSkew int) bool {
	minCount := math.MaxInt16

	// azCount is a just a map of various AZ & count of servers in those AZs. We & pass this explicitly since we want ALL  zones to be counted during calculation for skew. 

	// calculate server count of each region within the combo
	for _, server := range combo {
		if az, ok := server["az"]; ok {
			azCount[az] += 1
		} else {
			fmt.Printf("\nUnable to fetch region of server %s; so rejecting the combo\n", server["server"])
			return false
		}
	}

	// calculate global minimum
	for _, v := range azCount {
		if v < minCount {
			minCount = v
		}
	}

	// Since golang doesnt have inbuilt function to calculate abs(int)
	absInt := func(x int) int {
		if x < 0 {
			return x * -1
		} else {
			return x
		}
	}

	// If diff b/w serverCount & global minimum less than maxSkew for all region, then satisifedi. Else return false
	for _, v := range azCount {
		skew := absInt(v - minCount)
		if skew > maxSkew {
			return false
		}
	}

	return true
}

func mapClone(src map[string]int) map[string]int {
	dst := make(map[string]int, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// Compute all nCr combinations for 'n' servers & 'r' shardsize with Zone Awareness
// Inspired by https://leetcode.com/problems/combinations/solutions/794032/python-js-go-by-dfs-backtracking-w-hint/
// TODO: add constraint in main() that maxSkew should be >= 1 and also maxSkew
func generateZAServerCombos(totalServerList []map[string][]string, shardSize int, maxSkew int) [][]map[string]string {

	var potpourri []map[string]string        // Looks like: [ {server:"localhos:8080", az: "east1"}, {server: "localhost:8081", az: "east-2"}]
	result := make([][]map[string]string, 0) // Slice with each element of type `[]map[string]string`
	azCount := make(map[string]int)

	for _, azServerList := range totalServerList {

		// create potpourri of all servers
		for az, serversInAZ := range azServerList {
			azCount[az] = 0
			for _, server := range serversInAZ {				
				server := map[string]string{"server": server, "az": az}
				potpourri = append(potpourri, server)
			}
		}
	}

	var util_nCr func(start int, curServerCombo []map[string]string)
	util_nCr = func(start int, curServerCombo []map[string]string) {
		// Basecase: When shard is full
		if len(curServerCombo) == shardSize {
			// make a copy of current combination
			dst := make([]map[string]string, shardSize)
			copy(dst, curServerCombo)
			// fmt.Printf("Another shard found: %v\n\n/////////////////////////////////\n", dst)
			result = append(result, dst)
			return
		}

		// Traverse along array containing all servers & pick servers to construct each combination in nCr
		for i := start; i < len(potpourri); i++ {

			// if skewness constraint not satisfied then reject that combination
			// we are passing a copy since map is a pointer to hmap structure & this acts likea pass-by-reference
			if maxSkewSatisfied(append(curServerCombo, potpourri[i]), mapClone(azCount), maxSkew) {
				curServerCombo = append(curServerCombo, potpourri[i])
				// fmt.Printf("\ncurServerCombo: %v\n##########\n", curServerCombo)
				util_nCr(i+1, curServerCombo)
				curServerCombo = curServerCombo[:len(curServerCombo)-1] // pop last element
			}

		}

	}

	util_nCr(0, make([]map[string]string, 0))
	fmt.Println("Total number of shards: ", len(result))
	return result
}

// Initialise a shard with given combination of servers
func (sp *ShardPool) initialiseShardPool(serverList []map[string][]string, shardSize int, maxSkew int) {
	result := generateZAServerCombos(serverList, shardSize, maxSkew)

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

type ServerConf struct {
	Zones []map[string][]string
}

func ParseServerConf(filePath string) []map[string][]string {
	info, err := os.Stat(filePath)
	if os.IsNotExist(err) && !info.IsDir() {
		log.Fatalf("\nFile %s doesnt exist (or it's a directory)", filePath)
	}

	var serverConf ServerConf // map[string]interface{}

	jsonContents, err := os.ReadFile(filePath)
	if err != nil {
		fmt.Println("Unable to parse json file: ", err)
	}
	if err := json.Unmarshal(jsonContents, &serverConf); err != nil {
		fmt.Println(err)
	}
	// fmt.Println(serverConf)

	return serverConf.Zones

}

func main() {
	var backendConf string
	var port, shardSize, maxSkew int

	flag.StringVar(&backendConf, "backendConfFile", "server-conf.json", "Path to the JSON file in which Server backends are to be provided. Sample file included in repo")
	flag.IntVar(&port, "port", 3030, "Port to serve")
	flag.IntVar(&shardSize, "shardSize", -1, "Number of servers per shard")
	flag.IntVar(&maxSkew, "maxskew", 1, "AZ Skewness while distributing servers in  a shard. Refer k8s defintion of skew in PodTopologySpreadConstraints")
	flag.Parse()

	if shardSize <= 1 {
		log.Fatal("Pls provide a shardSize > 1")
	}

	serverList := ParseServerConf(backendConf)

	(&serverShardPool).initialiseShardPool(serverList, shardSize, maxSkew)

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

