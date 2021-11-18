/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome
import spock.lang.Unroll

import static org.elasticsearch.gradle.fixtures.DistributionDownloadFixture.withMockedDistributionDownload

class DistributionDownloadPluginFuncTest extends AbstractGradleFuncTest {

    @Unroll
    def "extracted #distType version can be resolved"() {
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
        def guh = new File(testProjectDir.getRoot(), "gradle-user-home").absolutePath;
        def runner = gradleRunner('clean', 'setupDistro', '-i', '-g', guh)
        def unpackingMessage = "Unpacking elasticsearch-${version}-linux-${Architecture.current().classifier}.tar.gz " +
                "using SymbolicLinkPreservingUntarTransform"
        def result = withMockedDistributionDownload(version, platform, runner) {
            // initial run
            def firstRun = build()
            assertOutputContains(firstRun.output, unpackingMessage)
            // 2nd invocation
            build()
        }

        then:
        result.task(":setupDistro").outcome == TaskOutcome.SUCCESS
        assertOutputMissing(result.output, unpackingMessage)
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
        result.output.count("Unpacking elasticsearch-${version}-linux-${Architecture.current().classifier}.tar.gz " +
                "using SymbolicLinkPreservingUntarTransform") == 1
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
