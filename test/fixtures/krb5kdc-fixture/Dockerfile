FROM ubuntu:24.04

ADD . /fixture
# Update the package listing
RUN apt-get update

# Install sudo
RUN apt-get install -y sudo python3
RUN echo kerberos.build.elastic.co > /etc/hostname
RUN bash /fixture/src/main/resources/provision/installkdc.sh

EXPOSE 88
EXPOSE 88/udp

CMD ["sleep", "infinity"]
