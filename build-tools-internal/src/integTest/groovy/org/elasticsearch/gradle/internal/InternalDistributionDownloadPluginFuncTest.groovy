/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal


import spock.lang.Unroll

import org.elasticsearch.gradle.Architecture
import org.elasticsearch.gradle.VersionProperties
import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome

@Unroll
class InternalDistributionDownloadPluginFuncTest extends AbstractGradleFuncTest {

    def "resolves current #arch version from local build"() {
        given:
        internalBuild()
        localDistroSetup(arch)
        def distroVersion = VersionProperties.getElasticsearch()
        buildFile << """
            apply plugin: 'elasticsearch.internal-distribution-download'

            elasticsearch_distributions {
              test_distro {
                  version = "$distroVersion"
                  type = "archive"
                  platform = "linux"
                  architecture = Architecture.current();
              }
            }
            tasks.register("setupDistro", Sync) {
                from(elasticsearch_distributions.test_distro)
                into("build/distro")
            }
        """

        when:
        def result = gradleRunner("-Dos.arch=${arch.classifier}", "setupDistro", '-g', gradleUserHome).build()

        then:
        result.task(":distribution:archives:${getTestArchiveProjectName(arch)}:buildExpanded").outcome == TaskOutcome.SUCCESS
        result.task(":setupDistro").outcome == TaskOutcome.SUCCESS
        assertExtractedDistroIsCreated("build/distro", 'current-marker.txt')
        where:
        arch << [Architecture.AARCH64, Architecture.AMD64]
    }

    def "resolves expanded bwc versions from source"() {
        given:
        internalBuild()
        bwcMinorProjectSetup()
        buildFile << """
            apply plugin: 'elasticsearch.internal-distribution-download'

            elasticsearch_distributions {
              test_distro {
                  version = "8.4.0"
                  type = "archive"
                  platform = "linux"
                  architecture = Architecture.current();
              }
            }
            tasks.register("setupDistro", Sync) {
                from(elasticsearch_distributions.test_distro)
                into("build/distro")
            }
        """
        when:

        def result = gradleRunner("setupDistro").build()
        then:
        result.task(":distribution:bwc:minor:buildBwcExpandedTask").outcome == TaskOutcome.SUCCESS
        result.task(":setupDistro").outcome == TaskOutcome.SUCCESS
        assertExtractedDistroIsCreated(
            "distribution/bwc/minor/build/install/elastic-distro",
            'bwc-marker.txt'
        )
    }

    def "fails on resolving bwc versions with no bundled jdk"() {
        given:
        internalBuild()
        bwcMinorProjectSetup()
        buildFile << """
            apply plugin: 'elasticsearch.internal-distribution-download'

            elasticsearch_distributions {
              test_distro {
                  version = "8.4.0"
                  type = "archive"
                  platform = "linux"
                  architecture = Architecture.current();
                  bundledJdk = false
              }
            }
            tasks.register("createExtractedTestDistro") {
                dependsOn elasticsearch_distributions.test_distro
            }
        """
        when:
        def result = gradleRunner("createExtractedTestDistro").buildAndFail()
        then:
        assertOutputContains(
            result.output, "Configuring a snapshot bwc distribution ('test_distro') " +
            "without a bundled JDK is not supported."
        )
    }

    private void bwcMinorProjectSetup() {
        settingsFile << """
        include ':distribution:bwc:minor'
        """
        def bwcSubProjectFolder = testProjectDir.newFolder("distribution", "bwc", "minor")
        new File(bwcSubProjectFolder, 'bwc-marker.txt') << "bwc=minor"
        new File(bwcSubProjectFolder, 'build.gradle') << """
            apply plugin:'base'

            // packed distro
            configurations.create("${testArchiveProjectName}")
            tasks.register("buildBwcTask", Tar) {
                from('bwc-marker.txt')
                archiveExtension = "tar.gz"
                compression = Compression.GZIP
            }
            artifacts {
                it.add("${testArchiveProjectName}", buildBwcTask)
            }

            // expanded distro
            configurations.create("expanded-${testArchiveProjectName}")
            def expandedTask = tasks.register("buildBwcExpandedTask", Copy) {
                from('bwc-marker.txt')
                into('build/install/elastic-distro')
            }
            artifacts {
                it.add("expanded-${testArchiveProjectName}", file('build/install')) {
                    builtBy expandedTask
                    type = 'directory'
                }
            }
        """
    }

    private void localDistroSetup(Architecture arch = Architecture.current()) {
        def archiveProjectName = getTestArchiveProjectName(arch)
        settingsFile << """
        include ":distribution:archives:${ archiveProjectName}"
        """
        def bwcSubProjectFolder = testProjectDir.newFolder("distribution", "archives", archiveProjectName)
        new File(bwcSubProjectFolder, 'current-marker.txt') << "current"
        new File(bwcSubProjectFolder, 'build.gradle') << """
            import org.gradle.api.internal.artifacts.ArtifactAttributes;

            apply plugin:'distribution'

            def buildTar = tasks.register("buildTar", Tar) {
                from('current-marker.txt')
                archiveExtension = "tar.gz"
                compression = Compression.GZIP
            }
            def buildExpanded = tasks.register("buildExpanded", Copy) {
                from('current-marker.txt')
                into("build/local")
            }
            configurations {
                extracted {
                    attributes {
                          attribute(ArtifactAttributes.ARTIFACT_FORMAT, "directory")
                    }
                }
            }
            artifacts {
                it.add("default", buildTar)
                it.add("extracted", buildExpanded)
            }
        """
    }

    String getTestArchiveProjectName(Architecture architecture = Architecture.current()) {
        def archSuffix = '-' +  architecture.classifier
        return "linux${archSuffix}-tar"
    }

    boolean assertExtractedDistroIsCreated(String relativeDistroPath, String markerFileName) {
        File extractedFolder = new File(testProjectDir.root, relativeDistroPath)
        assert extractedFolder.exists()
        assert new File(extractedFolder, markerFileName).exists()
        true
    }
}
