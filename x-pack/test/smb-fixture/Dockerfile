FROM ubuntu:16.04
ADD . /fixture
RUN chmod +x /fixture/src/main/resources/provision/installsmb.sh

EXPOSE 389
EXPOSE 636
EXPOSE 3268
EXPOSE 3269

CMD /fixture/src/main/resources/provision/installsmb.sh

