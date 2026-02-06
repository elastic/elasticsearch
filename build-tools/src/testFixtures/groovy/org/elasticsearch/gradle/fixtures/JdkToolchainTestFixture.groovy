/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.fixtures


import org.gradle.testkit.runner.BuildResult
import org.gradle.testkit.runner.GradleRunner

class JdkToolchainTestFixture {

    public static final String TOOLCHAIN_INIT_SCRIPT = "toolchain-init.gradle"

    static BuildResult withMockedJdkDownload(GradleRunner gradleRunner, Closure<BuildResult> buildRunClosure, int javaVersion, String vendor) {
        String urlPath = urlPath(javaVersion, vendor)
        return WiremockFixture.withWireMock(urlPath, filebytes(javaVersion)) { server ->
            File initFile = new File(gradleRunner.getProjectDir(), TOOLCHAIN_INIT_SCRIPT)
            initFile.text = """
                import org.gradle.jvm.toolchain.JavaToolchainDownload
                import org.gradle.jvm.toolchain.JavaToolchainRequest
                import org.gradle.jvm.toolchain.JavaToolchainResolver
                import org.gradle.platform.OperatingSystem

                abstract class MockToolchainResolver implements JavaToolchainResolver {
                    @Override
                    java.util.Optional resolve(JavaToolchainRequest request) {
                        println("########## JVM TOOLCHAIN MOCK ############")
                        java.net.URI uri = java.net.URI.create('${server.baseUrl()}${urlPath}')
                        return java.util.Optional.of(JavaToolchainDownload.fromUri(uri))
                    }
                }

                toolchainManagement {
                    jvm {
                        javaRepositories {
                            repository('mock') {
                                resolverClass = MockToolchainResolver
                            }
                        }
                    }
                }
            """
            List<String> givenArguments = gradleRunner.getArguments()
            GradleRunner effectiveRunner = gradleRunner.withArguments(
                givenArguments + ['--init-script', initFile.getAbsolutePath()]
            )
            buildRunClosure.delegate = effectiveRunner
            return buildRunClosure.call(effectiveRunner)
        }
    }

    static BuildResult withMockedJdkDownload(GradleRunner gradleRunner, int javaVersion, String vendor) {
        return withMockedJdkDownload(gradleRunner, { it.build() }, javaVersion, vendor)
    }

    private static String urlPath(int javaVersion, String vendor) {
        String os = System.getProperty("os.name").toLowerCase()
        String platform = os.contains("win") ? "windows" : os.contains("mac") ? "mac" : "linux"
        String arch = System.getProperty("os.arch").contains("aarch64") ? "aarch64" : "x64"
        return "/jdk/${vendor}/${javaVersion}/${platform}/${arch}/jdk-${javaVersion}-${vendor}.tar.gz"
    }

    private static byte[] filebytes(int javaVersion) throws IOException {
        return JdkToolchainTestFixture.getResourceAsStream(
            "/org/elasticsearch/gradle/fake_jdk_${javaVersion}.tar.gz"
        )?.getBytes() ?: new byte[0]
    }
}
