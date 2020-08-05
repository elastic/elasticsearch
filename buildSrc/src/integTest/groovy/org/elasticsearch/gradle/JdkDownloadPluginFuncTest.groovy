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
import org.elasticsearch.gradle.transform.SymbolicLinkPreservingUntarTransform
import org.elasticsearch.gradle.transform.UnzipTransform
import spock.lang.Unroll

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.regex.Matcher
import java.util.regex.Pattern

import static org.elasticsearch.gradle.JdkDownloadPlugin.VENDOR_ADOPTOPENJDK
import static org.elasticsearch.gradle.JdkDownloadPlugin.VENDOR_OPENJDK

class JdkDownloadPluginFuncTest extends AbstractGradleFuncTest {

    private static final String OPENJDK_VERSION_OLD = "1+99"
    private static final String ADOPT_JDK_VERSION = "12.0.2+10"
    private static final String OPEN_JDK_VERSION = "12.0.1+99@123456789123456789123456789abcde"
    private static final Pattern JDK_HOME_LOGLINE = Pattern.compile("JDK HOME: (.*)");

    @Unroll
    def "jdk #jdkVendor for #platform#suffix are downloaded and extracted"() {
        given:
        def mockRepoUrl = urlPath(jdkVendor, jdkVersion, platform);
        def mockedContent = filebytes(jdkVendor, platform)
        buildFile.text = """
            plugins {
             id 'elasticsearch.jdk-download'
            }
            
            jdks {
              myJdk {
                vendor = '$jdkVendor'
                version = '$jdkVersion'
                platform = "$platform"
                architecture = "x64"
              }
            }

            tasks.register("getJdk") {
                dependsOn jdks.myJdk
                doLast {
                    println "JDK HOME: " + jdks.myJdk
                }
            }
        """

        when:
        def result = WiremockFixture.withWireMock(mockRepoUrl, mockedContent) { server ->
            buildFile << repositoryMockSetup(server, jdkVendor, jdkVersion)
            gradleRunner("getJdk").build()
        }

        then:
        assertExtraction(result.output, expectedJavaBin);

        where:
        platform  | jdkVendor           | jdkVersion          | expectedJavaBin          | suffix
        "linux"   | VENDOR_ADOPTOPENJDK | ADOPT_JDK_VERSION   | "bin/java"               | ""
        "linux"   | VENDOR_OPENJDK      | OPEN_JDK_VERSION    | "bin/java"               | ""
        "linux"   | VENDOR_OPENJDK      | OPENJDK_VERSION_OLD | "bin/java"               | "(old version)"
        "windows" | VENDOR_ADOPTOPENJDK | ADOPT_JDK_VERSION   | "bin/java"               | ""
        "windows" | VENDOR_OPENJDK      | OPEN_JDK_VERSION    | "bin/java"               | ""
        "windows" | VENDOR_OPENJDK      | OPENJDK_VERSION_OLD | "bin/java"               | "(old version)"
        "darwin"  | VENDOR_ADOPTOPENJDK | ADOPT_JDK_VERSION   | "Contents/Home/bin/java" | ""
        "darwin"  | VENDOR_OPENJDK      | OPEN_JDK_VERSION    | "Contents/Home/bin/java" | ""
        "darwin"  | VENDOR_OPENJDK      | OPENJDK_VERSION_OLD | "Contents/Home/bin/java" | "(old version)"
    }

    def "transforms are reused across projects"() {
        given:
        def mockRepoUrl = urlPath(jdkVendor, jdkVersion, platform)
        def mockedContent = filebytes(jdkVendor, platform)
        10.times {
            settingsFile << """
                include ':sub-$it'
            """
        }
        buildFile.text = """
            plugins {
             id 'elasticsearch.jdk-download' apply false
            }
            
            subprojects {
                apply plugin: 'elasticsearch.jdk-download'
    
                jdks {
                  myJdk {
                    vendor = '$jdkVendor'
                    version = '$jdkVersion'
                    platform = "$platform"
                    architecture = "x64"
                  }
                }
                tasks.register("getJdk") {
                    dependsOn jdks.myJdk
                    doLast {
                        println "JDK HOME: " + jdks.myJdk
                    }
                }
            }
        """

        when:
        def result = WiremockFixture.withWireMock(mockRepoUrl, mockedContent) { server ->
            buildFile << repositoryMockSetup(server, jdkVendor, jdkVersion)
            gradleRunner('getJdk', '-i', '-g', testProjectDir.newFolder().toString()).build()
        }

        then:
        result.tasks.size() == 10
        result.output.count("Unpacking linux-12.0.2-x64.tar.gz using SymbolicLinkPreservingUntarTransform.") == 1

        where:
        platform | jdkVendor           | jdkVersion        | expectedJavaBin
        "linux"  | VENDOR_ADOPTOPENJDK | ADOPT_JDK_VERSION | "bin/java"
    }

    @Unroll
    def "transforms of type #transformType are kept across builds"() {
        given:
        def mockRepoUrl = urlPath(VENDOR_ADOPTOPENJDK, ADOPT_JDK_VERSION, platform)
        def mockedContent = filebytes(VENDOR_ADOPTOPENJDK, platform)
        buildFile.text = """
            plugins {
             id 'elasticsearch.jdk-download'
            }
            
            apply plugin: 'elasticsearch.jdk-download'

            jdks {
              myJdk {
                vendor = '$VENDOR_ADOPTOPENJDK'
                version = '$ADOPT_JDK_VERSION'
                platform = "$platform"
                architecture = "x64"
              }
            }
            
            tasks.register("getJdk") {
                dependsOn jdks.myJdk
                doLast {
                    println "JDK HOME: " + jdks.myJdk
                }
            }
        """

        when:
        def result = WiremockFixture.withWireMock(mockRepoUrl, mockedContent) { server ->
            buildFile << repositoryMockSetup(server, VENDOR_ADOPTOPENJDK, ADOPT_JDK_VERSION)

            def commonGradleUserHome = testProjectDir.newFolder().toString()
            // initial run
            gradleRunner('getJdk', '-g', commonGradleUserHome).build()
            // run against up-to-date transformations
            gradleRunner('getJdk', '-i', '-g', commonGradleUserHome).build()
        }

        then:
        assertOutputContains(result.output, "Skipping $transformType")

        where:
        platform  | transformType
        "linux"   | SymbolicLinkPreservingUntarTransform.class.simpleName
        "windows" | UnzipTransform.class.simpleName
    }

    static boolean assertExtraction(String output, String javaBin) {
        Matcher matcher = JDK_HOME_LOGLINE.matcher(output);
        assert matcher.find() == true;
        String jdkHome = matcher.group(1);
        Path javaPath = Paths.get(jdkHome, javaBin);
        assert Files.exists(javaPath) == true;
        true
    }

    private static String urlPath(final String vendor, final String version, final String platform) {
        if (vendor.equals(VENDOR_ADOPTOPENJDK)) {
            final String module = platform.equals("darwin") ? "mac" : platform;
            return "/jdk-12.0.2+10/" + module + "/x64/jdk/hotspot/normal/adoptopenjdk";
        } else if (vendor.equals(VENDOR_OPENJDK)) {
            final String effectivePlatform = platform.equals("darwin") ? "osx" : platform;
            final boolean isOld = version.equals(OPENJDK_VERSION_OLD);
            final String versionPath = isOld ? "jdk1/99" : "jdk12.0.1/123456789123456789123456789abcde/99";
            final String filename = "openjdk-" + (isOld ? "1" : "12.0.1") + "_" + effectivePlatform + "-x64_bin." + extension(platform);
            return "/java/GA/" + versionPath + "/GPL/" + filename;
        }
    }

    private static byte[] filebytes(final String vendor, final String platform) throws IOException {
        final String effectivePlatform = platform.equals("darwin") ? "osx" : platform;
        if (vendor.equals(VENDOR_ADOPTOPENJDK)) {
            return JdkDownloadPluginFuncTest.class.getResourceAsStream("fake_adoptopenjdk_" + effectivePlatform + "." + extension(platform)).getBytes()
        } else if (vendor.equals(VENDOR_OPENJDK)) {
            JdkDownloadPluginFuncTest.class.getResourceAsStream("fake_openjdk_" + effectivePlatform + "." + extension(platform)).getBytes()
        }
    }

    private static String extension(String platform) {
        platform.equals("windows") ? "zip" : "tar.gz";
    }

    private static String repositoryMockSetup(WireMockServer server, String jdkVendor, String jdkVersion) {
        """allprojects{ p ->
                // wire the jdk repo to wiremock
                p.repositories.all { repo ->
                    if(repo.name == "jdk_repo_${jdkVendor}_${jdkVersion}") {
                      repo.setUrl('${server.baseUrl()}')
                    }
                }
           }"""
    }
}
