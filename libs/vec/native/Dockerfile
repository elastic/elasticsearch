FROM centos:7

RUN yum install -y git gcc gcc-c++ make java-11-openjdk
COPY . /workspace
WORKDIR /workspace
RUN ./gradlew --quiet --console=plain clean vecSharedLibrary

CMD cat build/libs/vec/shared/libvec.so
