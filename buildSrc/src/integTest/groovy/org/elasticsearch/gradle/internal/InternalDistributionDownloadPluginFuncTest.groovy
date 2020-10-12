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
import org.gradle.testkit.runner.GradleRunner
import org.gradle.testkit.runner.TaskOutcome
import org.junit.Rule
import org.junit.rules.TemporaryFolder

import java.lang.management.ManagementFactory

class InternalDistributionDownloadPluginFuncTest extends AbstractGradleFuncTest {

    def "plugin application fails on non internal build"() {
        given:
        buildFile.text = """
            plugins {
             id 'elasticsearch.internal-distribution-download'
            }
        """

        when:
        def result = gradleRunner("tasks").buildAndFail()

        then:
        assertOutputContains(result.output, "Plugin 'elasticsearch.internal-distribution-download' is not supported. " +
            "Use 'elasticsearch.distribution-download' plugin instead")
    }

    def "resolves current version from local build"() {
        given:
        internalBuild()
        localDistroSetup()
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
                from(elasticsearch_distributions.test_distro.extracted)
                into("build/distro")
            }
        """

        when:
        def result = gradleRunner("setupDistro", '-g', testProjectDir.newFolder('GUH').path).build()

        then:
        result.task(":distribution:archives:linux-tar:buildExpanded").outcome == TaskOutcome.SUCCESS
        result.task(":setupDistro").outcome == TaskOutcome.SUCCESS
        assertExtractedDistroIsCreated("build/distro", 'current-marker.txt')
    }

    def "resolves expanded bwc versions from source"() {
        given:
        internalBuild()
        bwcMinorProjectSetup()
        buildFile << """
            apply plugin: 'elasticsearch.internal-distribution-download'

            elasticsearch_distributions {
              test_distro {
                  version = "8.1.0"
                  type = "archive"
                  platform = "linux"
                  architecture = Architecture.current();
              }
            }
            tasks.register("setupDistro", Sync) {
                from(elasticsearch_distributions.test_distro.extracted)
                into("build/distro")
            }
        """
        when:

        def result = gradleRunner("setupDistro").build()
        then:
        result.task(":distribution:bwc:minor:buildBwcExpandedTask").outcome == TaskOutcome.SUCCESS
        result.task(":setupDistro").outcome == TaskOutcome.SUCCESS
        assertExtractedDistroIsCreated("distribution/bwc/minor/build/install/elastic-distro",
                'bwc-marker.txt')
    }

    def "fails on resolving bwc versions with no bundled jdk"() {
        given:
        internalBuild()
        bwcMinorProjectSetup()
        buildFile << """
            apply plugin: 'elasticsearch.internal-distribution-download'

            elasticsearch_distributions {
              test_distro {
                  version = "8.1.0"
                  type = "archive"
                  platform = "linux"
                  architecture = Architecture.current();
                  bundledJdk = false
              }
            }
            tasks.register("createExtractedTestDistro") {
                dependsOn elasticsearch_distributions.test_distro.extracted
            }
        """
        when:
        def result = gradleRunner("createExtractedTestDistro").buildAndFail()
        then:
        assertOutputContains(result.output, "Configuring a snapshot bwc distribution ('test_distro') " +
                "without a bundled JDK is not supported.")
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
            configurations.create("linux-tar")
            tasks.register("buildBwcTask", Tar) {
                from('bwc-marker.txt')
                archiveExtension = "tar.gz"
                compression = Compression.GZIP
            }
            artifacts {
                it.add("linux-tar", buildBwcTask)
            }
            
            // expanded distro
            configurations.create("expanded-linux-tar")
            def expandedTask = tasks.register("buildBwcExpandedTask", Copy) {
                from('bwc-marker.txt')
                into('build/install/elastic-distro')
            }
            artifacts {
                it.add("expanded-linux-tar", file('build/install')) {
                    builtBy expandedTask
                    type = 'directory'
                }
            }
        """
    }

    private void localDistroSetup() {
        settingsFile << """
        include ":distribution:archives:linux-tar"
        """
        def bwcSubProjectFolder = testProjectDir.newFolder("distribution", "archives", "linux-tar")
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
        buildFile << """
        """
    }

    boolean assertExtractedDistroIsCreated(String relativeDistroPath, String markerFileName) {
        File extractedFolder = new File(testProjectDir.root, relativeDistroPath)
        assert extractedFolder.exists()
        assert new File(extractedFolder, markerFileName).exists()
        true
    }
}
