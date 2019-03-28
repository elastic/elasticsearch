FROM java:8-jre

RUN apt-get update && apt-get install net-tools

EXPOSE 9998
EXPOSE 9999

CMD java -cp "/fixture:/fixture/*" hdfs.MiniHDFS /data