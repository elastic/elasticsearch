FROM alpine:3.21.0

ADD src/main/resources /fixture
RUN apk update && apk add -y --no-cache python3 krb5 krb5-server

RUN echo kerberos.build.elastic.co > /etc/hostname
RUN sh /fixture/provision/installkdc.sh

EXPOSE 88
EXPOSE 88/udp

CMD ["sleep", "infinity"]
