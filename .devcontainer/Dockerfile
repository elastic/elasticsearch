FROM fedora:34
ARG USERNAME=es-dev
ARG USER_UID=1000
ARG USER_GID=${USER_UID}

RUN dnf --refresh -y update \
    && dnf install -y git ripgrep neovim emacs java-1.8.0-openjdk-devel java-11-openjdk-devel java-latest-openjdk-devel \
    && dnf clean all \
    && echo 'export JAVA8_HOME=/usr/lib/jvm/jre-1.8.0-openjdk' >> /etc/profile \
    && echo 'export JAVA11_HOME=/usr/lib/jvm/jre-11-openjdk' >> /etc/profile \
    && echo 'export JAVA16_HOME=/usr/lib/jvm/jre-16-openjdk' >> /etc/profile \
    && echo 'export JAVA_HOME=$JAVA16_HOME' >> /etc/profile \
    && echo 'export RUNTIME_JAVA_HOME=$JAVA_HOME' >> /etc/profile \
    && source /etc/profile \
    && alternatives --set java_sdk_openjdk $(readlink -f /usr/lib/jvm/jre-16-openjdk) \
    && alternatives --set java $(readlink -f /usr/lib/jvm/jre-16-openjdk/bin/java) \
    && alternatives --set javac $(readlink -f /usr/lib/jvm/jre-16-openjdk/bin/javac)

RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME \
    && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
    && chmod 0440 /etc/sudoers.d/$USERNAME

USER $USERNAME