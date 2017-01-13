#!/bin/sh
mvn install -DskipTests
perl dev-tools/src/main/resources/license-check/check_license_and_sha.pl \
     --update distribution/licenses/ distribution/zip/target/releases/elasticsearch-2.4.1-SNAPSHOT.zip elasticsearch-2.4.1-SNAPSHOT
perl dev-tools/src/main/resources/license-check/check_license_and_sha.pl \
     --update plugins/analysis-icu/licenses/ plugins/analysis-icu/target/releases/analysis-icu-2.4.1-SNAPSHOT.zip analysis-icu-2.4.1-SNAPSHOT
perl dev-tools/src/main/resources/license-check/check_license_and_sha.pl \
     --update plugins/analysis-kuromoji/licenses/ plugins/analysis-kuromoji/target/releases/analysis-kuromoji-2.4.1-SNAPSHOT.zip analysis-kuromoji-2.4.1-SNAPSHOT
perl dev-tools/src/main/resources/license-check/check_license_and_sha.pl \
     --update plugins/analysis-phonetic/licenses/ plugins/analysis-phonetic/target/releases/analysis-phonetic-2.4.1-SNAPSHOT.zip analysis-phonetic-2.4.1-SNAPSHOT
perl dev-tools/src/main/resources/license-check/check_license_and_sha.pl \
     --update plugins/analysis-smartcn/licenses/ plugins/analysis-smartcn/target/releases/analysis-smartcn-2.4.1-SNAPSHOT.zip analysis-smartcn-2.4.1-SNAPSHOT
perl dev-tools/src/main/resources/license-check/check_license_and_sha.pl \
     --update plugins/analysis-stempel/licenses/ plugins/analysis-stempel/target/releases/analysis-stempel-2.4.1-SNAPSHOT.zip analysis-stempel-2.4.1-SNAPSHOT
perl dev-tools/src/main/resources/license-check/check_license_and_sha.pl \
     --update modules/lang-expression/licenses/ modules/lang-expression/target/releases/lang-expression-2.4.1-SNAPSHOT.zip lang-expression-2.4.1-SNAPSHOT
