FROM java:8-jre

ADD ./build/fixture /fixture

EXPOSE 9998
EXPOSE 9999

CMD java -cp `cat /fixture/.classpath` hdfs.MiniHDFS /fixture/shared