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

import org.gradle.testkit.runner.GradleRunner
import org.gradle.testkit.runner.TaskOutcome
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import spock.lang.Specification

import java.lang.management.ManagementFactory

class InternalDistributionDownloadPluginFuncTest extends Specification {

    @Rule
    TemporaryFolder testProjectDir = new TemporaryFolder()

    File settingsFile
    File buildFile

    def setup() {
        settingsFile = testProjectDir.newFile('settings.gradle')
        settingsFile << "rootProject.name = 'hello-world'"
        buildFile = testProjectDir.newFile('build.gradle')
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

    def testCurrent() {
        given:
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
        def buildOutput = gradleRunner("createExtractedTestDistro").build()
        then:
        buildOutput.task(":distribution:archives:linux-tar:buildTar").outcome == TaskOutcome.SUCCESS
        buildOutput.task(":extractElasticsearchLinux$distroVersion").outcome == TaskOutcome.SUCCESS
        assertExtractedDistroIsCreated(distroVersion, 'current-marker.txt')
    }

    def testBwc() {
        given:
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
        def buildOutput = gradleRunner("createExtractedTestDistro").build()
        then:
        buildOutput.task(":distribution:bwc:minor:buildBwcTask").outcome == TaskOutcome.SUCCESS
        buildOutput.task(":extractElasticsearchLinux8.1.0").outcome == TaskOutcome.SUCCESS
        assertExtractedDistroIsCreated(distroVersion,'bwc-marker.txt')
    }

    private GradleRunner gradleRunner(String... arguments) {
        GradleRunner.create()
            .withDebug(ManagementFactory.getRuntimeMXBean().getInputArguments().toString().indexOf("-agentlib:jdwp") > 0)
            .withProjectDir(testProjectDir.root)
            .withArguments(arguments)
            .withPluginClasspath()
            .forwardOutput()
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
