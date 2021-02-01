/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle.internal

import org.elasticsearch.gradle.VersionProperties
import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome
import spock.lang.Unroll

class InternalDistributionArchiveCheckPluginFuncTest extends AbstractGradleFuncTest {

    def setup() {
        ["darwin-zip", 'darwin-tar'].each { projName ->
            settingsFile << """
            include ':${projName}'
            """

            file("${projName}/build.gradle") << """
                plugins {
                  id 'elasticsearch.internal-distribution-archive-check'
                }"""
        }
        file("SomeFile.txt") << """
            some dummy txt file 
        """

        buildFile << """
            allprojects {
                apply plugin:'base'
                ext.elasticLicenseUrl = "http://foo.bar"
            }
            tasks.register("buildDarwinTar", Tar) {
                compression = Compression.GZIP
                from 'SomeFile.class'
            }
            tasks.register("buildDarwinZip", Zip) {
                from 'SomeFile.txt'
            }"""
    }

    @Unroll
    def "plain class files in distribution #archiveType archives are detected"() {
        given:
        file("SomeFile.class") << """
            some dummy class file 
        """
        buildFile << """
            tasks.withType(AbstractArchiveTask).configureEach {
                from 'SomeFile.class'
            }
        """
        when:
        def result = gradleRunner(":darwin-${archiveType}:check", '--stacktrace').buildAndFail()
        then:
        result.task(":darwin-${archiveType}:checkExtraction").outcome == TaskOutcome.FAILED
        result.output.contains("Detected class file in distribution ('SomeFile.class')")

        where:
        archiveType << ["zip", 'tar']
    }

    def "fails on unexpected license content"() {
        given:
        elasticLicense()
        file("LICENSE.txt") << """elastic license coorp stuff line 1
unknown license content line 2
        """
        buildFile << """
            tasks.withType(AbstractArchiveTask).configureEach {
                into("elasticsearch-${VersionProperties.getElasticsearch()}") {
                    from 'LICENSE.txt'
                    from 'SomeFile.txt'
                }
            }
        """

        when:
        def result = gradleRunner(":darwin-tar:check").buildAndFail()
        then:
        result.task(":darwin-tar:checkLicense").outcome == TaskOutcome.FAILED
        normalized(result.output).contains("> expected line [2] in " +
                "[./darwin-tar/build/tar-extracted/elasticsearch-${VersionProperties.getElasticsearch()}/LICENSE.txt] " +
                "to be [elastic license coorp stuff line 2] but was [unknown license content line 2]")
    }

    def "fails on unexpected notice content"() {
        given:
        elasticLicense()
        elasticLicense(file("LICENSE.txt"))
        file("NOTICE.txt").text = """Elasticsearch
Copyright 2009-2018 Acme Coorp"""
        buildFile << """
            apply plugin:'base'
            tasks.withType(AbstractArchiveTask).configureEach {
                into("elasticsearch-${VersionProperties.getElasticsearch()}") {
                    from 'LICENSE.txt'
                    from 'SomeFile.txt'
                    from 'NOTICE.txt'
                }
            }
        """

        when:
        def result = gradleRunner(":darwin-tar:checkNotice").buildAndFail()
        then:
        result.task(":darwin-tar:checkNotice").outcome == TaskOutcome.FAILED
        normalized(result.output).contains("> expected line [2] in " +
                "[./darwin-tar/build/tar-extracted/elasticsearch-${VersionProperties.getElasticsearch()}/NOTICE.txt] " +
                "to be [Copyright 2009-2018 Elasticsearch] but was [Copyright 2009-2018 Acme Coorp]")
    }

    def "fails on unexpected ml notice content"() {
        given:
        elasticLicense()
        elasticLicense(file("LICENSE.txt"))
        file("NOTICE.txt").text = """Elasticsearch
Copyright 2009-2018 Elasticsearch"""

        file("ml/NOTICE.txt").text = "Boost Software License - Version 1.0 - August 17th, 2003"
        file('darwin-tar/build.gradle') << """
            distributionArchiveCheck {
                expectedMlLicenses.add('foo license')
            }
        """
        buildFile << """
            apply plugin:'base'
            tasks.withType(AbstractArchiveTask).configureEach {
                into("elasticsearch-${VersionProperties.getElasticsearch()}") {
                    from 'LICENSE.txt'
                    from 'SomeFile.txt'
                    from 'NOTICE.txt'
                    into('modules/x-pack-ml') {
                        from 'ml/NOTICE.txt'
                    }
                }
            }
        """

        when:
        def result = gradleRunner(":darwin-tar:check").buildAndFail()
        then:
        result.task(":darwin-tar:checkMlCppNotice").outcome == TaskOutcome.FAILED
        normalized(result.output)
                .contains("> expected [./darwin-tar/build/tar-extracted/elasticsearch-" +
                        "${VersionProperties.getElasticsearch()}/modules/x-pack-ml/NOTICE.txt " +
                        "to contain [foo license] but it did not")
    }

    void elasticLicense(File file = file("licenses/ELASTIC-LICENSE.txt")) {
        file << """elastic license coorp stuff line 1
elastic license coorp stuff line 2
elastic license coorp stuff line 3
"""
    }

}
