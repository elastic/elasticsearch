#!/bin/sh
mvn install -DskipTests
perl dev-tools/src/main/resources/license-check/check_license_and_sha.pl \
     --update distribution/licenses/ distribution/zip/target/releases/elasticsearch-3.0.0-SNAPSHOT.zip elasticsearch-3.0.0-SNAPSHOT
perl dev-tools/src/main/resources/license-check/check_license_and_sha.pl \
     --update plugins/analysis-icu/licenses/ plugins/analysis-icu/target/releases/analysis-icu-3.0.0-SNAPSHOT.zip analysis-icu-3.0.0-SNAPSHOT
perl dev-tools/src/main/resources/license-check/check_license_and_sha.pl \
     --update plugins/analysis-kuromoji/licenses/ plugins/analysis-kuromoji/target/releases/analysis-kuromoji-3.0.0-SNAPSHOT.zip analysis-kuromoji-3.0.0-SNAPSHOT
perl dev-tools/src/main/resources/license-check/check_license_and_sha.pl \
     --update plugins/analysis-phonetic/licenses/ plugins/analysis-phonetic/target/releases/analysis-phonetic-3.0.0-SNAPSHOT.zip analysis-phonetic-3.0.0-SNAPSHOT
perl dev-tools/src/main/resources/license-check/check_license_and_sha.pl \
     --update plugins/analysis-smartcn/licenses/ plugins/analysis-smartcn/target/releases/analysis-smartcn-3.0.0-SNAPSHOT.zip analysis-smartcn-3.0.0-SNAPSHOT
perl dev-tools/src/main/resources/license-check/check_license_and_sha.pl \
     --update plugins/analysis-stempel/licenses/ plugins/analysis-stempel/target/releases/analysis-stempel-3.0.0-SNAPSHOT.zip analysis-stempel-3.0.0-SNAPSHOT
