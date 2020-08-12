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
import org.gradle.testkit.runner.TaskOutcome

import java.nio.file.Files
import java.nio.file.Paths

class DistributionDownloadPluginFuncTest extends AbstractGradleFuncTest {

    def "resolves current version from external"() {
        given:
        def distroVersion = VersionProperties.getElasticsearch()

        def mockRepoUrl = urlPath(distroVersion)
        def mockedContent = filebytes(mockRepoUrl)

        buildFile << """
            import org.elasticsearch.gradle.Architecture

            plugins {
                id 'elasticsearch.distribution-download'
            }

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
            gradleRunner('setupDistro').build()
        }

        then:
        result.task(":setupDistro").outcome == TaskOutcome.SUCCESS
        assertExtractedDistroCreated("build/distro")
    }

    private boolean assertExtractedDistroCreated(String relativePath) {
        File distroExtracted = new File(testProjectDir.root, relativePath)
        assert distroExtracted.exists()
        assert distroExtracted.isDirectory()
        true
    }

    String urlPath(String version) {
        "/downloads/elasticsearch/elasticsearch-${version}-linux-x86_64.tar.gz"
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
        String sourceFile = "src/testKit/distribution-download/distribution/files/fake_elasticsearch." + suffix;
        return Files.newInputStream(Paths.get(sourceFile)).getBytes()
    }
}
