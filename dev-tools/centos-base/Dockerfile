FROM centos:7

# Install docker
RUN yum install -y yum-utils device-mapper-persistent-data lvm2
RUN yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
RUN yum install -y docker-ce docker-ce-cli containerd.io

# Required for java-latest-openjdk-headless
RUN yum install -y epel-release

RUN mkdir /build

ADD mkimage-elasticsearch.sh /build

# Update the tag if we move off centos:7
CMD ["/build/mkimage-elasticsearch.sh", "-t", "7", "elasticsearch/centos-base"]
