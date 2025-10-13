FROM ubuntu:24.04

ENV TZ="Etc/UTC"

RUN DEBIAN_FRONTEND=noninteractive apt-get update -qqy && \
    apt-get install -qqy tzdata winbind samba ldap-utils

COPY smb/provision/installsmb.sh /fixture/provision/installsmb.sh
COPY smb/certs/ca.key /fixture/certs/ca.key
COPY smb/certs/ca.pem /fixture/certs/ca.pem
COPY smb/certs/cert.pem /fixture/certs/cert.pem
COPY smb/certs/key.pem /fixture/certs/key.pem

RUN chmod +x /fixture/provision/installsmb.sh

CMD ["/bin/sh", "-c", "/fixture/provision/installsmb.sh && service samba-ad-dc restart && echo Samba started && sleep infinity"]
