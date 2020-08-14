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

import com.github.tomakehurst.wiremock.WireMockServer
import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.elasticsearch.gradle.fixtures.WiremockFixture
import org.elasticsearch.gradle.transform.JdkSymbolicLinkPreservingUntarTransform
import org.elasticsearch.gradle.transform.JdkUnzipTransform
import org.elasticsearch.gradle.transform.SymbolicLinkPreservingUntarTransform
import org.gradle.testkit.runner.TaskOutcome
import spock.lang.Unroll

import java.nio.file.Files
import java.nio.file.Paths

import static org.elasticsearch.gradle.JdkDownloadPlugin.VENDOR_ADOPTOPENJDK
import static org.elasticsearch.gradle.JdkDownloadPlugin.VENDOR_ADOPTOPENJDK
import static org.elasticsearch.gradle.JdkDownloadPlugin.VENDOR_ADOPTOPENJDK
import static org.elasticsearch.gradle.JdkDownloadPlugin.VENDOR_ADOPTOPENJDK

class DistributionDownloadPluginFuncTest extends AbstractGradleFuncTest {

    @Unroll
    def "#distType version can be resolved"() {
        given:
        def mockRepoUrl = urlPath(version, platform)
        def mockedContent = filebytes(mockRepoUrl)

        buildFile << """
            import org.elasticsearch.gradle.Architecture

            plugins {
                id 'elasticsearch.distribution-download'
            }

            elasticsearch_distributions {
              test_distro {
                  version = "$version"
                  type = "archive"
                  platform = "$platform"
                  architecture = Architecture.current();
              }
            }
            
            tasks.register("setupDistro", Sync) {
                from(elasticsearch_distributions.test_distro.extracted)
                into("build/distro")
            }
        """

        when:
        def result = WiremockFixture.withWireMock(mockRepoUrl, mockedContent) { server ->
            buildFile << repositoryMockSetup(server)
            gradleRunner('setupDistro').build()
        }

        then:
        result.task(":setupDistro").outcome == TaskOutcome.SUCCESS
        assertExtractedDistroCreated("build/distro")

        where:
        version                              | platform  | distType
        VersionProperties.getElasticsearch() | "linux"   | "current"
        "8.1.0-SNAPSHOT"                     | "linux"   | "bwc"
        "7.0.0"                              | "windows" | "released"
    }

    def "transformed versions are kept across builds"() {
        given:
        def distroVersion = VersionProperties.getElasticsearch()
        def mockRepoUrl = urlPath(distroVersion)
        def mockedContent = filebytes(mockRepoUrl)

        buildFile << """
            import org.elasticsearch.gradle.Architecture

            plugins {
                id 'elasticsearch.distribution-download'
            }
            apply plugin: 'base'

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
        def result = WiremockFixture.withWireMock(mockRepoUrl, mockedContent) { server ->
            buildFile << repositoryMockSetup(server)
            gradleRunner('clean', 'setupDistro').build()
            gradleRunner('clean', 'setupDistro', '-i').build()
        }

        then:
        result.task(":setupDistro").outcome == TaskOutcome.SUCCESS
        assertOutputContains(result.output, "Skipping ${SymbolicLinkPreservingUntarTransform.class.simpleName}")
    }

    def "transforms are reused across projects"() {
        given:
        def distroVersion = VersionProperties.getElasticsearch()
        def mockRepoUrl = urlPath(distroVersion)
        def mockedContent = filebytes(mockRepoUrl)

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
            }
        """

        when:
        def result = WiremockFixture.withWireMock(mockRepoUrl, mockedContent) { server ->
            buildFile << repositoryMockSetup(server)
            gradleRunner('setupDistro', '-i', '-g', testProjectDir.newFolder().toString()).build()
        }

        then:
        result.tasks.size() == 3
        result.output.count("Unpacking elasticsearch-${distroVersion}-linux-x86_64.tar.gz using SymbolicLinkPreservingUntarTransform.") == 1
    }

    private boolean assertExtractedDistroCreated(String relativePath) {
        File distroExtracted = new File(testProjectDir.root, relativePath)
        assert distroExtracted.exists()
        assert distroExtracted.isDirectory()
        assert new File(distroExtracted, "elasticsearch-1.2.3/bin/elasticsearch").exists()
        true
    }

    String urlPath(String version, String platform = 'linux') {
        String fileType = platform == "linux" ? "tar.gz" : "zip"
        "/downloads/elasticsearch/elasticsearch-${version}-${platform}-x86_64.$fileType"
    }

    private static String repositoryMockSetup(WireMockServer server) {
        """allprojects{ p ->
                p.repositories.all { repo ->
                    repo.setUrl('${server.baseUrl()}')
                }
           }"""
    }

    private static byte[] filebytes(String urlPath) throws IOException {
        String suffix = urlPath.endsWith("zip") ? "zip" : "tar.gz";
        return DistributionDownloadPluginFuncTest.getResourceAsStream("fake_elasticsearch." + suffix).getBytes()
    }
}
