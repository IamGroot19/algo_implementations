To avoid reinventing the wheel, I picked this excellent tutorial on Round Robin based LB: https://kasvith.me/posts/lets-create-a-simple-lb-go/ (fullcode:) https://github.com/kasvith/simplelb)


Sample execution
```
$ go run main.go -backends "http://localhost:8090,http://localhost:8091,http://localhost:8092,http://localhost:8093,http://localhost:8094" -port 8080

2023/05/30 20:01:35 Health check completed
2023/05/30 20:02:05 Starting health check...
2023/05/30 20:02:05 http://localhost:8090 [up]
2023/05/30 20:02:05 http://localhost:8091 [up]
2023/05/30 20:02:05 http://localhost:8092 [up]
2023/05/30 20:02:05 http://localhost:8093 [up]
2023/05/30 20:02:05 
Site localhost:8094 unreachable, error: dial tcp 127.0.0.1:8094: connect: connection refused 
2023/05/30 20:02:05 http://localhost:8094 [down]
2023/05/30 20:02:05 Health check completed

```