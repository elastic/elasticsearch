#!/bin/bash

# Debug Elasticsearch with ContextualAI Service
# This script starts Elasticsearch in debug mode on port 5005

set -e

echo "Starting Elasticsearch in debug mode..."

# Parse arguments: --f or -f triggers a full rebuild (clean + assemble)
FULL_REBUILD=false
for arg in "$@"; do
    case "$arg" in
        -f|--f|--full)
            FULL_REBUILD=true
            ;;
    esac
done

if [ "$FULL_REBUILD" = true ]; then
    echo "Performing full rebuild: clean + assemble"
    #./gradlew :distribution:archives:darwin-tar:assemble --parallel :x-pack:plugin:inference:clean :x-pack:plugin:inference:processResources :x-pack:plugin:inference:compileJava
    ./gradlew :x-pack:plugin:inference:clean :x-pack:plugin:inference:processResources :x-pack:plugin:inference:compileJava :distribution:archives:darwin-tar:assemble --no-parallel
else
    echo "Building Elasticsearch distribution for debugging (assemble only)..."
    ./gradlew :distribution:archives:darwin-tar:assemble --parallel
fi

# Extract the distribution into the local debug folder (overwrite existing)
mkdir -p build/distribution/local
cd build/distribution/local
rm -rf elasticsearch-9.2.0-SNAPSHOT
tar -xzf ../../../distribution/archives/darwin-tar/build/distributions/elasticsearch-9.2.0-SNAPSHOT-darwin-x86_64.tar.gz
cd ../../..

# Kill any existing processes
echo "Killing any existing Elasticsearch processes..."
pkill -f elasticsearch || true
sleep 2

# Create debug config - disable security for easier debugging
cd build/distribution/local/elasticsearch-9.2.0-SNAPSHOT

cat > config/elasticsearch.yml << EOF
# Debug configuration - NO SECURITY for easier debugging
xpack.security.enabled: false
xpack.security.http.ssl.enabled: false
xpack.security.transport.ssl.enabled: false
network.host: localhost
http.port: 9200
cluster.name: elasticsearch-debug
node.name: debug-node
discovery.type: single-node
xpack.ml.enabled: false
EOF

echo "Starting Elasticsearch with remote debugging enabled (NO SECURITY)..."
echo "Debug port: 5005"
echo "Connect your debugger to localhost:5005"

# Set JVM debug options via ES_JAVA_OPTS
export ES_JAVA_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=*:5005"

# Start with configuration options - NO SECURITY for debugging
echo "Starting Elasticsearch in foreground with debug logging..."
echo "Press Ctrl+C to stop"
echo ""
./bin/elasticsearch \
    -E cluster.name=elasticsearch-debug \
    -E node.name=debug-node \
    -E discovery.type=single-node \
    -E xpack.security.enabled=false \
    -E network.host=localhost \
    -E http.port=9200 \
    -E xpack.ml.enabled=false \
    -E logger.org.elasticsearch.xpack.inference=DEBUG \
    -E logger.org.elasticsearch.xpack.inference.services.contextualai=TRACE \
    -E logger.org.elasticsearch.xpack.inference.external=DEBUG \
    -E logger.org.elasticsearch.xpack.inference.services=DEBUG \
    -E logger.org.elasticsearch.action.admin.cluster.settings=DEBUG \
    -E logger.org.elasticsearch.xpack.inference.services.validation=DEBUG