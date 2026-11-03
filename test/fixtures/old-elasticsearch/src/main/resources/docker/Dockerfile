FROM eclipse-temurin:8-jdk

ARG ES_VERSION
ARG TARGETARCH

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl gosu unzip \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /tmp
# Resolves the download URL for ES_VERSION and unpacks it to /usr/share/elasticsearch.
# - 0.90.13 and 1.7.6 live on download.elastic.co under version-specific paths.
# - 2.4.5 lives on download.elastic.co under a Maven-style release path.
# - >=5.0, <7.9 are architecture-independent zips on artifacts.elastic.co.
# - >=7.9 ship per-architecture archives that bundle their own JDK.
# The curl download is retried because fetching the ES distribution from
# artifacts.elastic.co/download.elastic.co occasionally hits transient network errors
# (e.g. HTTP/2 stream errors, curl exit 92) that otherwise fail the whole image build and,
# in turn, the dependent test suites (see #156120). This keeps the local fallback build
# (used when the prebaked fixture image cannot be pulled) resilient.
RUN set -e; \
 case "$ES_VERSION" in \
   0.90.13|1.7.6) \
     url="https://download.elastic.co/elasticsearch/elasticsearch/elasticsearch-${ES_VERSION}.zip"; ext=zip ;; \
   2.4.5) \
     url="https://download.elastic.co/elasticsearch/release/org/elasticsearch/distribution/zip/elasticsearch/${ES_VERSION}/elasticsearch-${ES_VERSION}.zip"; ext=zip ;; \
   7.9.3|7.10.0) \
     case "$TARGETARCH" in \
       amd64) arch=x86_64 ;; \
       arm64) arch=aarch64 ;; \
       *) echo "Unsupported TARGETARCH [$TARGETARCH] for ES_VERSION [$ES_VERSION]" >&2; exit 1 ;; \
     esac; \
     url="https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-${ES_VERSION}-linux-${arch}.tar.gz"; ext=tar.gz ;; \
   *) \
     url="https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-${ES_VERSION}.zip"; ext=zip ;; \
 esac; \
 curl -fsSL --retry 5 --retry-delay 3 --retry-connrefused --retry-all-errors -o "elasticsearch.${ext}" "$url"; \
 if [ "$ext" = "zip" ]; then unzip -q "elasticsearch.${ext}"; else tar -xzf "elasticsearch.${ext}"; fi; \
 rm -f "elasticsearch.${ext}"; \
 mv "elasticsearch-${ES_VERSION}" /usr/share/elasticsearch

RUN chmod -R +x /usr/share/elasticsearch/bin \
 && echo 'http.host: 0.0.0.0' >> /usr/share/elasticsearch/config/elasticsearch.yml \
 && echo 'transport.host: 127.0.0.1' >> /usr/share/elasticsearch/config/elasticsearch.yml \
 && addgroup --system elasticsearch \
 && adduser --system --ingroup elasticsearch --home /usr/share/elasticsearch --no-create-home elasticsearch \
 && chown -R elasticsearch:elasticsearch /usr/share/elasticsearch

# Persisted to the runtime environment so entrypoint.sh can branch on it (e.g. the
# foreground flag 0.90.13 requires) without re-deriving it from the archive layout.
ENV ES_VERSION=${ES_VERSION}

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

WORKDIR /usr/share/elasticsearch
EXPOSE 9200 9300
ENTRYPOINT ["/entrypoint.sh"]
