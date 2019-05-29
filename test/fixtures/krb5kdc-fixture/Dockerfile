FROM ubuntu:14.04
ADD . /fixture
RUN echo kerberos.build.elastic.co > /etc/hostname && echo "127.0.0.1 kerberos.build.elastic.co" >> /etc/hosts
RUN bash /fixture/src/main/resources/provision/installkdc.sh

EXPOSE 88
EXPOSE 88/udp

CMD sleep infinity