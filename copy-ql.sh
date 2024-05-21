#!/bin/bash
set -euo pipefail
IFS=$'\n\t'

cd x-pack/plugin/

# you never know...
rm -rf qlcore

echo create a new qlcore module copying ql
cp -R ql qlcore

echo Adapt java src directories and rename the plugin
cd qlcore/src/main/java/org/elasticsearch/xpack
mv ql qlcore
cd qlcore/plugin
mv QlPlugin.java QlCorePlugin.java
find . -type f -name 'QlCorePlugin.java' -exec sed -i s/'class QlPlugin'/'class QlCorePlugin'/g {} +
cd ../../../../../../../..
# we are now at /elasticsearch/x-pack/plugin/qlcore
cd src/test/java/org/elasticsearch/xpack
mv ql qlcore
cd ../../../../../..
# we are now at /elasticsearch/x-pack/plugin/qlcore
cd test-fixtures/src/main/java/org/elasticsearch/xpack
mv ql qlcore
cd ../../../../../../..
# we are now at /elasticsearch/x-pack/plugin/qlcore

echo adapt java packages in qlcore module
find . -type f -name '*.java' -exec sed -i s/xpack\\.ql/xpack.qlcore/g {} +

echo rename the plugin in the gradle files
find . -type f -name '*.gradle' -exec sed -i s/\'x-pack-ql\'/\'x-pack-qlcore\'/g {} +
find . -type f -name '*.gradle' -exec sed -i s/\'ql\'/\'qlcore\'/g {} +
find . -type f -name '*.gradle' -exec sed -i s/ql\.plugin\.QlPlugin/qlcore.plugin.QlCorePlugin/g {} +
find . -type f -name '*.gradle' -exec sed -i s/ql\:test\-fixtures/qlcore\:test-fixtures/g {} +



echo modify ESQL to use the new plugin
cd ../esql

echo point to the new plugin from Gradle
find . -type f -name '*.gradle' -exec sed -i s/\'x-pack-ql\'/\'x-pack-qlcore\'/g {} +
find . -type f -name '*.gradle' -exec sed -i s/\'ql\'/\'qlcore\'/g {} +
find . -type f -name '*.gradle' -exec sed -i s/ql\:test\-fixtures/qlcore\:test-fixtures/g {} +

echo update packages
find . -type f -name '*.java' -exec sed -i s/xpack\\.ql/xpack.qlcore/g {} +

echo fix bwc tests
find . -type f -name '*.csv-spec' -exec sed -i s/'skip\:\-8\.11\.99\, reason\:ql exceptions were updated in 8\.12'/'skip:-8.13.99, reason:warning messages changed in 8.14'/g {} +
find . -type f -name '*.csv-spec' -exec sed -i s/'evalDateDiffErrorOutOfIntegerRange\#\[skip\:\-8\.12\.99\, reason\:date_diff added in 8\.13'/'evalDateDiffErrorOutOfIntegerRange\#\[skip:-8.13.99, reason:warning messages changed in 8.14'/g {} +
find . -type f -name '*.csv-spec' -exec sed -i s/'convertDoubleToUL\#\[skip\:\-8\.11\.99, reason\:ql exceptions updated in 8\.12'/'convertDoubleToUL\#\[skip:-8.13.99, reason:warning messages changed in 8.14'/g {} +
find . -type f -name '*.csv-spec' -exec sed -i s/xpack\\.ql/xpack.qlcore/g {} +

echo fix spotless
cd ../../..
./gradlew :x-pack:plugin:qlcore:spotlessApply
./gradlew :x-pack:plugin:esql:spotlessApply

echo done!
