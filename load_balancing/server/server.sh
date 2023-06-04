#!/bin/bash

## TODO: Refactor this script into into docker compose later

go build -o server

ports=(8090 8091 8092 8093 8094 8095)

function start() {
    echo "Inside start()"
    for serverPort in "${ports[@]}"; do
        cmd="cd /server && chmod +x server && sleep 5 && ./server -port $serverPort"
        echo "Cmd to run inside container: $cmd"
        docker run --restart always -d -v "$(pwd):/server" --net host --name "server-$serverPort" --memory=150m --cpus=0.2  golang:1.19.9-buster bash -c "$cmd"

        server="\"localhost:$serverPort\""
        combined="${combined}${combined:+,}$server"
        
    done

    # cat "$combined" ## Only for debugging

cat  <<EOF > prometheus.yml
global:
  scrape_interval: 15s
scrape_configs:
  - job_name: prometheus
    static_configs:
      - targets: ["localhost:9090"]
  - job_name: simple_server
    static_configs:
      - targets: [ $combined ]
EOF

    docker run --rm -ti  -v "$(pwd)/prometheus.yml:/etc/prometheus/prometheus.yml" --net host --name prometheus prom/prometheus:v2.40.7 
}

function stop() {
    for serverPort in "${ports[@]}"; do
        docker rm -f server-"$serverPort"        
    done
    docker rm -f prometheus
}

if [[ "$1" == "start" ]]; then
    start
elif [[ "$1" == "stop" ]]; then
    stop
else
    echo "Provide either of flg args: 'start', 'stop'"
fi
