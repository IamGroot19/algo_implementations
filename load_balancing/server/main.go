package main

import (
	"flag"
	"fmt"
	"net/http"

	prometheus "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var pingCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "ping_request_count",
		Help: "No of request handled by Ping handler",
	},
	[]string{"tenant"},
)

func ping(w http.ResponseWriter, req *http.Request) {
	tenant := req.Header.Get("tenant")
	pingCounter.With(prometheus.Labels{"tenant": tenant}).Inc()
	resp := fmt.Sprintf("pong (from %d)",port)
	fmt.Fprintf(w, resp)
}

func healthcheck(w http.ResponseWriter, req *http.Request) {
	w.WriteHeader(http.StatusOK)
	return
}

var port int // shouldnt be declared as global ideally; hackyFix for printing which server responded to the HTTP request

func main() {
	flag.IntVar(&port, "port", 0, "Port to serve")
	flag.Parse()

	if port == 0 {
		panic("please provide an explicit port value")
	}
	prometheus.MustRegister(pingCounter)
	http.HandleFunc("/ping", ping)
	http.HandleFunc("/health", healthcheck)
	http.Handle("/metrics", promhttp.Handler())

	server := http.Server{
		Addr: fmt.Sprintf(":%d", port),
	}
	fmt.Printf("Starting server")
	server.ListenAndServe()
}
