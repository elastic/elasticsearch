FROM ubuntu:16.04
RUN apt-get update -qqy && apt-get install -qqy samba ldap-utils
ADD . /fixture
RUN chmod +x /fixture/src/main/resources/provision/installsmb.sh
RUN /fixture/src/main/resources/provision/installsmb.sh

EXPOSE 389
EXPOSE 636
EXPOSE 3268
EXPOSE 3269

CMD service samba-ad-dc restart && sleep infinity
