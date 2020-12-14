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

package org.elasticsearch.gradle

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.elasticsearch.gradle.transform.SymbolicLinkPreservingUntarTransform
import org.gradle.testkit.runner.TaskOutcome
import spock.lang.Unroll

import static org.elasticsearch.gradle.fixtures.DistributionDownloadFixture.withMockedDistributionDownload

class DistributionDownloadPluginFuncTest extends AbstractGradleFuncTest {

    @Unroll
    def "#distType version can be resolved"() {
        given:
        buildFile << applyPluginAndSetupDistro(version, platform)

        when:
        def result = withMockedDistributionDownload(version, platform, gradleRunner('setupDistro', '-i')) {
            build()
        }

        then:
        result.task(":setupDistro").outcome == TaskOutcome.SUCCESS
        assertExtractedDistroCreated("build/distro")

        where:
        version                              | platform                                   | distType
        VersionProperties.getElasticsearch() | ElasticsearchDistribution.Platform.LINUX   | "current"
        "8.1.0-SNAPSHOT"                     | ElasticsearchDistribution.Platform.LINUX   | "bwc"
        "7.0.0"                              | ElasticsearchDistribution.Platform.WINDOWS | "released"
    }


    def "transformed versions are kept across builds"() {
        given:
        def version = VersionProperties.getElasticsearch()
        def platform = ElasticsearchDistribution.Platform.LINUX

        buildFile << applyPluginAndSetupDistro(version, platform)
        buildFile << """
            apply plugin:'base'
        """

        when:
        def runner = gradleRunner('clean', 'setupDistro', '-i')
        def result = withMockedDistributionDownload(version, platform, runner) {
            // initial run
            build()
            // 2nd invocation
            build()
        }

        then:
        result.task(":setupDistro").outcome == TaskOutcome.SUCCESS
        assertOutputContains(result.output, "Skipping ${SymbolicLinkPreservingUntarTransform.class.simpleName}")
    }

    def "transforms are reused across projects"() {
        given:
        def version = VersionProperties.getElasticsearch()
        def platform = ElasticsearchDistribution.Platform.LINUX

        3.times {
            settingsFile << """
                include ':sub-$it'
            """
        }
        buildFile.text = """
            import org.elasticsearch.gradle.Architecture

            plugins {
                id 'elasticsearch.distribution-download'
            }
            
            subprojects {
                apply plugin: 'elasticsearch.distribution-download'
    
                ${setupTestDistro(version, platform)}
                ${setupDistroTask()}
            }
        """

        when:
        def customGradleUserHome = testProjectDir.newFolder().absolutePath;
        def runner = gradleRunner('setupDistro', '-i', '-g', customGradleUserHome)
        def result = withMockedDistributionDownload(version, platform, runner) {
            build()
        }

        then:
        result.tasks.size() == 3
        result.output.count("Unpacking elasticsearch-${version}-linux-x86_64.tar.gz " +
                "using SymbolicLinkPreservingUntarTransform.") == 1
    }

    private boolean assertExtractedDistroCreated(String relativePath) {
        File distroExtracted = new File(testProjectDir.root, relativePath)
        assert distroExtracted.exists()
        assert distroExtracted.isDirectory()
        assert new File(distroExtracted, "elasticsearch-1.2.3/bin/elasticsearch").exists()
        true
    }

    private static String applyPluginAndSetupDistro(String version, ElasticsearchDistribution.Platform platform) {
        """
            import org.elasticsearch.gradle.Architecture

            plugins {
                id 'elasticsearch.distribution-download'
            }

            ${setupTestDistro(version, platform)}
            ${setupDistroTask()}
            
        """
    }

    private static String setupTestDistro(String version, ElasticsearchDistribution.Platform platform) {
        return """
            elasticsearch_distributions {
                test_distro {
                    version = "$version"
                    type = "archive"
                    platform = "$platform"
                    architecture = Architecture.current();
                }
            }
            """
    }

    private static String setupDistroTask() {
        return """
            tasks.register("setupDistro", Sync) {
                from(elasticsearch_distributions.test_distro)
                into("build/distro")
            }
            """
    }
}