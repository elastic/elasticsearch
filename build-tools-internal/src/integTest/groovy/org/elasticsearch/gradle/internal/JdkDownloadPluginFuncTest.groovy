/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal

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

import static org.elasticsearch.gradle.JdkDownloadPlugin.VENDOR_ADOPTIUM
import static org.elasticsearch.gradle.JdkDownloadPlugin.VENDOR_OPENJDK
import static org.elasticsearch.gradle.JdkDownloadPlugin.VENDOR_AZUL

class JdkDownloadPluginFuncTest extends AbstractGradleFuncTest {

    private static final String OPENJDK_VERSION_OLD = "1+99"
    private static final String ADOPT_JDK_VERSION = "12.0.2+10"
    private static final String ADOPT_JDK_VERSION_11 = "11.0.10+9"
    private static final String ADOPT_JDK_VERSION_15 = "15.0.2+7"
    private static final String OPEN_JDK_VERSION = "12.0.1+99@123456789123456789123456789abcde"
    private static final String AZUL_AARCH_VERSION = "16.0.1+99@123456789123456789123456789abcde"
    private static final Pattern JDK_HOME_LOGLINE = Pattern.compile("JDK HOME: (.*)");

    @Unroll
    def "jdk #jdkVendor for #platform#suffix are downloaded and extracted"() {
        given:
        def mockRepoUrl = urlPath(jdkVendor, jdkVersion, platform, arch);
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
                architecture = '$arch'
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
        platform  | arch      | jdkVendor           | jdkVersion           | expectedJavaBin          | suffix
        "linux"   | "x64"     | VENDOR_ADOPTIUM     | ADOPT_JDK_VERSION    | "bin/java"               | ""
        "linux"   | "x64"     | VENDOR_OPENJDK      | OPEN_JDK_VERSION     | "bin/java"               | ""
        "linux"   | "x64"     | VENDOR_OPENJDK      | OPENJDK_VERSION_OLD  | "bin/java"               | "(old version)"
        "windows" | "x64"     | VENDOR_ADOPTIUM     | ADOPT_JDK_VERSION    | "bin/java"               | ""
        "windows" | "x64"     | VENDOR_OPENJDK      | OPEN_JDK_VERSION     | "bin/java"               | ""
        "windows" | "x64"     | VENDOR_OPENJDK      | OPENJDK_VERSION_OLD  | "bin/java"               | "(old version)"
        "darwin"  | "x64"     | VENDOR_ADOPTIUM     | ADOPT_JDK_VERSION    | "Contents/Home/bin/java" | ""
        "darwin"  | "x64"     | VENDOR_OPENJDK      | OPEN_JDK_VERSION     | "Contents/Home/bin/java" | ""
        "darwin"  | "x64"     | VENDOR_OPENJDK      | OPENJDK_VERSION_OLD  | "Contents/Home/bin/java" | "(old version)"
        "mac"     | "x64"     | VENDOR_OPENJDK      | OPEN_JDK_VERSION     | "Contents/Home/bin/java" | ""
        "mac"     | "x64"     | VENDOR_OPENJDK      | OPENJDK_VERSION_OLD  | "Contents/Home/bin/java" | "(old version)"
        "darwin"  | "aarch64" | VENDOR_AZUL         | AZUL_AARCH_VERSION   | "Contents/Home/bin/java" | ""
        "linux"   | "aarch64" | VENDOR_AZUL         | AZUL_AARCH_VERSION   | "bin/java"               | ""
        "linux"   | "aarch64" | VENDOR_ADOPTIUM     | ADOPT_JDK_VERSION_11 | "bin/java"               | "(jdk 11)"
        "linux"   | "aarch64" | VENDOR_ADOPTIUM     | ADOPT_JDK_VERSION_15 | "bin/java"               | "(jdk 15)"
    }

    def "transforms are reused across projects"() {
        given:
        def mockRepoUrl = urlPath(jdkVendor, jdkVersion, platform)
        def mockedContent = filebytes(jdkVendor, platform)
        3.times {
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
        result.tasks.size() == 3
        result.output.count("Unpacking linux-12.0.2-x64.tar.gz using ${SymbolicLinkPreservingUntarTransform.simpleName}") == 1

        where:
        platform | jdkVendor           | jdkVersion        | expectedJavaBin
        "linux"  | VENDOR_ADOPTIUM | ADOPT_JDK_VERSION | "bin/java"
    }

    @Unroll
    def "transforms of type #transformType are kept across builds"() {
        given:
        def mockRepoUrl = urlPath(VENDOR_ADOPTIUM, ADOPT_JDK_VERSION, platform)
        def mockedContent = filebytes(VENDOR_ADOPTIUM, platform)
        buildFile.text = """
            plugins {
             id 'elasticsearch.jdk-download'
            }
            apply plugin: 'base'
            apply plugin: 'elasticsearch.jdk-download'

            jdks {
              myJdk {
                vendor = '$VENDOR_ADOPTIUM'
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
            buildFile << repositoryMockSetup(server, VENDOR_ADOPTIUM, ADOPT_JDK_VERSION)

            def commonGradleUserHome = testProjectDir.newFolder().toString()
            // initial run
            def firstResult = gradleRunner('clean', 'getJdk', '-i', '--warning-mode', 'all', '-g', commonGradleUserHome).build()
            // assert the output of an executed transform is shown
            assertOutputContains(firstResult.output, "Unpacking $expectedArchiveName using $transformType")
            // run against up-to-date transformations
            gradleRunner('clean', 'getJdk', '-i', '--warning-mode', 'all', '-g', commonGradleUserHome).build()
        }

        then:
        normalized(result.output).contains("Unpacking $expectedArchiveName using $transformType") == false

        where:
        platform  | expectedArchiveName       | transformType
        "linux"   | "linux-12.0.2-x64.tar.gz" | SymbolicLinkPreservingUntarTransform.class.simpleName
        "windows" | "windows-12.0.2-x64.zip"  | UnzipTransform.class.simpleName
    }

    static boolean assertExtraction(String output, String javaBin) {
        Matcher matcher = JDK_HOME_LOGLINE.matcher(output);
        assert matcher.find() == true;
        String jdkHome = matcher.group(1);
        Path javaPath = Paths.get(jdkHome, javaBin);
        assert Files.exists(javaPath) == true;
        true
    }

    private static String urlPath(final String vendor,
                                  final String version,
                                  final String platform,
                                  final String arch = 'x64') {
        if (vendor.equals(VENDOR_ADOPTIUM)) {
            final String module = isMac(platform) ? "mac" : platform;
            return "/jdk-" + version + "/" + module + "/${arch}/jdk/hotspot/normal/adoptium";
        } else if (vendor.equals(VENDOR_OPENJDK)) {
            final String effectivePlatform = isMac(platform) ? "osx" : platform;
            final boolean isOld = version.equals(OPENJDK_VERSION_OLD);
            final String versionPath = isOld ? "jdk1/99" : "jdk12.0.1/123456789123456789123456789abcde/99";
            final String filename = "openjdk-" + (isOld ? "1" : "12.0.1") + "_" + effectivePlatform + "-x64_bin." + extension(platform);
            return "/java/GA/" + versionPath + "/GPL/" + filename;
        } else if (vendor.equals(VENDOR_AZUL)) {
            final String module = isMac(platform) ? "macosx" : platform;
            // we only test zulu 16 darwin aarch64 for now
            return "/zulu${module.equals('linux') ? '-embedded' : ''}/bin/zulu16.32.15-ca-jdk16.0.2-${module}_${arch}.tar.gz";
        }
    }

    private static byte[] filebytes(final String vendor, final String platform) throws IOException {
        final String effectivePlatform = isMac(platform) ? "osx" : platform;
        if (vendor.equals(VENDOR_ADOPTIUM)) {
            return JdkDownloadPluginFuncTest.class.getResourceAsStream("fake_adoptium_" + effectivePlatform + "." + extension(platform)).getBytes()
        } else if (vendor.equals(VENDOR_OPENJDK)) {
            JdkDownloadPluginFuncTest.class.getResourceAsStream("fake_openjdk_" + effectivePlatform + "." + extension(platform)).getBytes()
        } else if (vendor.equals(VENDOR_AZUL)) {
            String resourcePath = "fake_azuljdk_" + effectivePlatform + "_aarch64." + extension(platform)
            JdkDownloadPluginFuncTest.class.getResourceAsStream(resourcePath).getBytes()
        }
    }

    private static boolean isMac(String platform) {
        platform.equals("darwin") || platform.equals("mac")
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
                    allowInsecureProtocol = true
                }
           }"""
    }
}
