FROM ${base_image} AS builder

USER root

# Add plugins infrastructure
RUN mkdir -p /opt/plugins/archive
RUN chmod -R 0555 /opt/plugins

COPY filebeat-${version}.tar.gz metricbeat-${version}.tar.gz /tmp/
RUN set -eux ; \\
    for beat in filebeat metricbeat ; do \\
      if [ ! -s /tmp/\$beat-${version}.tar.gz ]; then \\
        echo "/tmp/\$beat-${version}.tar.gz is empty - cannot uncompress" 2>&1 ; \\
        exit 1 ; \\
      fi ; \\
      if ! tar tf /tmp/\$beat-${version}.tar.gz >/dev/null; then \\
        echo "/tmp/\$beat-${version}.tar.gz is corrupt - cannot uncompress" 2>&1 ; \\
        exit 1 ; \\
      fi ; \\
      mkdir -p /opt/\$beat ; \\
      tar xf /tmp/\$beat-${version}.tar.gz -C /opt/\$beat --strip-components=1 ; \\
    done

COPY plugins/*.zip /opt/plugins/archive/

RUN chown 1000:1000 /opt/plugins/archive/*
RUN chmod 0444 /opt/plugins/archive/*

FROM ${base_image}
USER root

RUN <%= retry.loop("apk", "export DEBIAN_FRONTEND=noninteractive && apk update && apk update && apk add --no-cache wget") %>

# tweak entry point for ESS specific wolfi image
ENTRYPOINT ["/sbin/tini", "--"]
CMD ["/app/elasticsearch.sh"]
# Generate a stub command that will be overwritten at runtime
RUN mkdir /app && \\
    echo -e '#!/bin/bash\\nexec /usr/local/bin/docker-entrypoint.sh eswrapper' > /app/elasticsearch.sh && \\
    chmod 0555 /app/elasticsearch.sh

COPY --from=builder --chown=0:0 /opt /opt
USER 1000:0
ENV ES_PLUGIN_ARCHIVE_DIR /opt/plugins/archive
