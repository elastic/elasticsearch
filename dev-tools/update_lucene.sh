#!/bin/sh
gradle assemble
perl dev-tools/src/main/resources/license-check/check_license_and_sha.pl \
     --update distribution/licenses/ distribution/zip/build/distributions/elasticsearch-3.0.0-SNAPSHOT.zip elasticsearch-3.0.0-SNAPSHOT
perl dev-tools/src/main/resources/license-check/check_license_and_sha.pl \
     --update plugins/analysis-icu/licenses/ plugins/analysis-icu/build/distributions/analysis-icu-3.0.0-SNAPSHOT.zip analysis-icu-3.0.0-SNAPSHOT
perl dev-tools/src/main/resources/license-check/check_license_and_sha.pl \
     --update plugins/analysis-kuromoji/licenses/ plugins/analysis-kuromoji/build/distributions/analysis-kuromoji-3.0.0-SNAPSHOT.zip analysis-kuromoji-3.0.0-SNAPSHOT
perl dev-tools/src/main/resources/license-check/check_license_and_sha.pl \
     --update plugins/analysis-phonetic/licenses/ plugins/analysis-phonetic/build/distributions/analysis-phonetic-3.0.0-SNAPSHOT.zip analysis-phonetic-3.0.0-SNAPSHOT
perl dev-tools/src/main/resources/license-check/check_license_and_sha.pl \
     --update plugins/analysis-smartcn/licenses/ plugins/analysis-smartcn/build/distributions/analysis-smartcn-3.0.0-SNAPSHOT.zip analysis-smartcn-3.0.0-SNAPSHOT
perl dev-tools/src/main/resources/license-check/check_license_and_sha.pl \
     --update plugins/analysis-stempel/licenses/ plugins/analysis-stempel/build/distributions/analysis-stempel-3.0.0-SNAPSHOT.zip analysis-stempel-3.0.0-SNAPSHOT
perl dev-tools/src/main/resources/license-check/check_license_and_sha.pl \
     --update plugins/lang-expression/licenses/ plugins/lang-expression/build/distributions/lang-expression-3.0.0-SNAPSHOT.zip lang-expression-3.0.0-SNAPSHOT
