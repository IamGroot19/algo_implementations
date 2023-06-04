Various algos that i found interesting & wanted to learn by implementing them myself.

NOTE: The code might not be production ready or might not cover all edge cases. So you use them at your own risk. 

What different Packages do:
- `server/` : Has go codes for a simple http server and a shell script to spawn docker cntainer for http servers & prometheus.
- `round-robin`: Round-robin LB algo implementation 
- `shuffle-sharding-primitive`: Primitive shuffle sharding implementation 
- `shuffle-sharding-zone-aware`: Shuffle Sharding mplementation with Zone-awareness & skewness constraint.

Instructions:
- To spawn servers + prometheus, run  ../server/server.sh (to control number of server, just add/remove ports from `ports` inside shell script)

- To spawn this load balancer server, do a ` go run main.go -backends "http://localhost:8090,http://localhost:8091,http://localhost:8092,http://localhost:8093,http://localhost:8094,http://localhost:8095" - shardsize 2`

