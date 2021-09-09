FROM ubuntu:14.04
ADD . /fixture
RUN echo kerberos.build.elastic.co > /etc/hostname
RUN bash /fixture/src/main/resources/provision/installkdc.sh

EXPOSE 88
EXPOSE 88/udp

CMD sleep infinity
