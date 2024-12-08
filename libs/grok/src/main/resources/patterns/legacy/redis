REDISTIMESTAMP %{MONTHDAY} %{MONTH} %{TIME}
REDISLOG \[%{POSINT:pid}\] %{REDISTIMESTAMP:timestamp} \* 
REDISMONLOG %{NUMBER:timestamp} \[%{INT:database} %{IP:client}:%{NUMBER:port}\] "%{WORD:command}"\s?%{GREEDYDATA:params}
