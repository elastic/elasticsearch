FROM debian:latest

RUN apt update
RUN apt install -y gcc g++ openjdk-17-jdk
COPY . /workspace
WORKDIR /workspace
RUN ./gradlew --quiet --console=plain clean buildSharedLibrary
RUN strip --strip-unneeded build/output/libvec.so

CMD cat build/output/libvec.so
