Various algos that i found interesting & wanted to learn by implementing them myself.

NOTE: The code might not be production ready or might not cover all edge cases. So you use them at your own risk. 


Instructions:
- To spawn servers + prometheus, run  ../server/server.sh (to control number of server, just add/remove ports from `ports` inside shell script)
- To spawn this load balancer server, do a ` go run ss-main.go -backends "http://localhost:8090,http://localhost:8091,http://localhost:8092,http://localhost:8093,http://localhost:8094,http://localhost:8095" - shardsize 2`

