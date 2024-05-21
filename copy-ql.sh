#!/bin/bash
set -euo pipefail
IFS=$'\n\t'

echo Adapt java src directories and rename the plugin
cd x-pack/plugin/esql-core/src/main/java/org/elasticsearch/xpack
mkdir esql && mv ql esql/core
cd esql/core/plugin
mv QlPlugin.java EsqlCorePlugin.java
find . -type f -name 'EsqlCorePlugin.java' -exec sed -i s/'class QlPlugin'/'class EsqlCorePlugin'/g {} +
cd ../../../../../../../../..
# we are now at /elasticsearch/x-pack/plugin/esql-core
cd src/test/java/org/elasticsearch/xpack
mkdir esql && mv ql esql/core
cd ../../../../../..
# we are now at /elasticsearch/x-pack/plugin/esql-core
cd test-fixtures/src/main/java/org/elasticsearch/xpack
mkdir esql && mv ql esql/core
cd ../../../../../../..
# we are now at /elasticsearch/x-pack/plugin/esql-core

echo adapt java packages in esql-core module
find . -type f -name '*.java' -exec sed -i s/xpack\\.ql/xpack.esql.core/g {} +

echo rename the plugin in the gradle files
find . -type f -name '*.gradle' -exec sed -i s/\'x-pack-ql\'/\'x-pack-esql-core\'/g {} +
find . -type f -name '*.gradle' -exec sed -i s/\'ql\'/\'esql-core\'/g {} +
find . -type f -name '*.gradle' -exec sed -i s/ql\.plugin\.QlPlugin/esql.core.plugin.EsqlCorePlugin/g {} +
find . -type f -name '*.gradle' -exec sed -i s/ql\:test\-fixtures/esql-core\:test-fixtures/g {} +

echo modify ESQL to use the new plugin
cd ../esql

echo point to the new plugin from Gradle
find . -type f -name '*.gradle' -exec sed -i s/\'x-pack-ql\'/\'x-pack-esql-core\'/g {} +
find . -type f -name '*.gradle' -exec sed -i s/\'ql\'/\'esql-core\'/g {} +
find . -type f -name '*.gradle' -exec sed -i s/ql\:test\-fixtures/esql-core\:test-fixtures/g {} +

echo update packages
find . -type f -name '*.java' -exec sed -i s/xpack\\.ql/xpack.esql.core/g {} +

echo update csv tests
find . -type f -name '*.csv-spec' -exec sed -i s/xpack\\.ql/xpack.esql.core/g {} +

echo fix spotless
cd ../../..
./gradlew :x-pack:plugin:esql-core:spotlessApply
./gradlew :x-pack:plugin:esql:spotlessApply

echo done!
