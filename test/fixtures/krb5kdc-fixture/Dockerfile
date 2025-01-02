FROM ubuntu:24.04

ADD . /fixture
RUN apt-get update && apt-get install -y --no-install-recommends python3 krb5-kdc krb5-admin-server && apt-get clean &&  rm -rf /var/lib/apt/lists/*

RUN echo kerberos.build.elastic.co > /etc/hostname
RUN bash /fixture/src/main/resources/provision/installkdc.sh

EXPOSE 88
EXPOSE 88/udp

CMD ["sleep", "infinity"]
