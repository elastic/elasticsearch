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
        def result = gradleRunner("createExtractedTestDistro").buildAndFail()

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
            tasks.register("createExtractedTestDistro") {
                dependsOn elasticsearch_distributions.test_distro.extracted
            }
        """

        when:
        def result = gradleRunner("createExtractedTestDistro").build()

        then:
        result.task(":distribution:archives:linux-tar:buildTar").outcome == TaskOutcome.SUCCESS
        result.task(":extractElasticsearchLinux$distroVersion").outcome == TaskOutcome.SUCCESS
        assertExtractedDistroIsCreated(distroVersion, 'current-marker.txt')
    }

    def "resolves bwc versions from source"() {
        given:
        internalBuild()
        bwcMinorProjectSetup()
        def distroVersion = "8.1.0"
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
            tasks.register("createExtractedTestDistro") {
                dependsOn elasticsearch_distributions.test_distro.extracted
            }
        """
        when:
        def result = gradleRunner("createExtractedTestDistro").build()
        then:
        result.task(":distribution:bwc:minor:buildBwcTask").outcome == TaskOutcome.SUCCESS
        result.task(":extractElasticsearchLinux8.1.0").outcome == TaskOutcome.SUCCESS
        assertExtractedDistroIsCreated(distroVersion,'bwc-marker.txt')
    }

    def "fails on resolving bwc versions with no bundled jdk"() {
        given:
        internalBuild()
        bwcMinorProjectSetup()
        def distroVersion = "8.1.0"
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

    private File internalBuild() {
        buildFile << """plugins {
          id 'elasticsearch.global-build-info'
        }
        import org.elasticsearch.gradle.Architecture
        import org.elasticsearch.gradle.info.BuildParams

        BuildParams.init { it.setIsInternal(true) }

        import org.elasticsearch.gradle.BwcVersions
        import org.elasticsearch.gradle.Version

        Version currentVersion = Version.fromString("9.0.0")
        BwcVersions versions = new BwcVersions(new TreeSet<>(
        Arrays.asList(Version.fromString("8.0.0"), Version.fromString("8.0.1"), Version.fromString("8.1.0"), currentVersion)),
            currentVersion)

        BuildParams.init { it.setBwcVersions(versions) }
        """
    }


    private void bwcMinorProjectSetup() {
        settingsFile << """
        include ':distribution:bwc:minor'
        """
        def bwcSubProjectFolder = testProjectDir.newFolder("distribution", "bwc", "minor")
        new File(bwcSubProjectFolder, 'bwc-marker.txt') << "bwc=minor"
        new File(bwcSubProjectFolder, 'build.gradle') << """
            apply plugin:'base'
            configurations.create("linux-tar")
            tasks.register("buildBwcTask", Tar) {
                from('bwc-marker.txt')
                archiveExtension = "tar.gz"
                compression = Compression.GZIP
            }
            artifacts {
                it.add("linux-tar", buildBwcTask)
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
            apply plugin:'distribution'
            tasks.register("buildTar", Tar) {
                from('current-marker.txt')
                archiveExtension = "tar.gz"
                compression = Compression.GZIP
            }
            artifacts {
                it.add("default", buildTar)
            }
        """
        buildFile << """
        """

    }

    boolean assertExtractedDistroIsCreated(String version, String markerFileName) {
        File extractedFolder = new File(testProjectDir.root, "build/elasticsearch-distros/extracted_elasticsearch_${version}_archive_linux_default")
        assert extractedFolder.exists()
        assert new File(extractedFolder, markerFileName).exists()
        true
    }
}
