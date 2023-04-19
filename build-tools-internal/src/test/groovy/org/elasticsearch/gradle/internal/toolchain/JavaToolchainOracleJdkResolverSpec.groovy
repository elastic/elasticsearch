/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.toolchain


import org.gradle.api.provider.Property
import org.gradle.api.services.BuildServiceParameters
import org.gradle.jvm.toolchain.JavaLanguageVersion
import org.gradle.jvm.toolchain.JavaToolchainDownload
import org.gradle.jvm.toolchain.JavaToolchainRequest
import org.gradle.jvm.toolchain.JavaToolchainSpec
import org.gradle.jvm.toolchain.JvmVendorSpec
import org.gradle.platform.Architecture
import org.gradle.platform.BuildPlatform
import org.gradle.platform.OperatingSystem
import spock.lang.Specification

import static org.gradle.jvm.toolchain.JvmVendorSpec.ORACLE
import static org.gradle.platform.Architecture.*
import static org.gradle.platform.OperatingSystem.*

class JavaToolchainOracleJdkResolverSpec extends Specification {

    def "resolves #os #arch #vendor jdk #langVersion"() {
        given:
        def resolver = resolverImplementation()

        when:
        Optional<JavaToolchainDownload> download = resolver.resolve(request(JavaLanguageVersion.of(langVersion), vendor, platform(os, arch)))

        then:
        download.get().uri == URI.create(expectedUrl)
        where:
        langVersion | vendor   | os      | arch    | expectedUrl
        20          | ORACLE   | MAC_OS  | X86_64  | "https://download.oracle.com/java/GA/jdk20/bdc68b4b9cbc4ebcb30745c85038d91d/36/GPL/openjdk-20_macos-x64_bin.tar.gz"
        20          | ORACLE   | MAC_OS  | AARCH64 | "https://download.oracle.com/java/GA/jdk20/bdc68b4b9cbc4ebcb30745c85038d91d/36/GPL/openjdk-20_macos-aarch64_bin.tar.gz"
        20          | ORACLE   | LINUX   | X86_64  | "https://download.oracle.com/java/GA/jdk20/bdc68b4b9cbc4ebcb30745c85038d91d/36/GPL/openjdk-20_linux-x64_bin.tar.gz"
        20          | ORACLE   | LINUX   | AARCH64 | "https://download.oracle.com/java/GA/jdk20/bdc68b4b9cbc4ebcb30745c85038d91d/36/GPL/openjdk-20_linux-aarch64_bin.tar.gz"
        20          | ORACLE   | WINDOWS | X86_64  | "https://download.oracle.com/java/GA/jdk20/bdc68b4b9cbc4ebcb30745c85038d91d/36/GPL/openjdk-20_windows-x64_bin.zip"

        19          | ORACLE   | MAC_OS  | X86_64  | "https://download.oracle.com/java/19/archive/jdk-19.0.2_macos-x64_bin.tar.gz"
        19          | ORACLE   | MAC_OS  | AARCH64 | "https://download.oracle.com/java/19/archive/jdk-19.0.2_macos-aarch64_bin.tar.gz"
        19          | ORACLE   | LINUX   | X86_64  | "https://download.oracle.com/java/19/archive/jdk-19.0.2_linux-x64_bin.tar.gz"
        19          | ORACLE   | LINUX   | AARCH64 | "https://download.oracle.com/java/19/archive/jdk-19.0.2_linux-aarch64_bin.tar.gz"
        19          | ORACLE   | WINDOWS | X86_64  | "https://download.oracle.com/java/19/archive/jdk-19.0.2_windows-x64_bin.zip"

        18          | ORACLE   | MAC_OS  | X86_64  | "https://download.oracle.com/java/18/archive/jdk-18.0.2.1_macos-x64_bin.tar.gz"
        18          | ORACLE   | MAC_OS  | AARCH64 | "https://download.oracle.com/java/18/archive/jdk-18.0.2.1_macos-aarch64_bin.tar.gz"
        18          | ORACLE   | LINUX   | X86_64  | "https://download.oracle.com/java/18/archive/jdk-18.0.2.1_linux-x64_bin.tar.gz"
        18          | ORACLE   | LINUX   | AARCH64 | "https://download.oracle.com/java/18/archive/jdk-18.0.2.1_linux-aarch64_bin.tar.gz"
        18          | ORACLE   | WINDOWS | X86_64  | "https://download.oracle.com/java/18/archive/jdk-18.0.2.1_windows-x64_bin.zip"

        17          | ORACLE   | MAC_OS  | X86_64  | "https://download.oracle.com/java/17/archive/jdk-17.0.7_macos-x64_bin.tar.gz"
        17          | ORACLE   | MAC_OS  | AARCH64 | "https://download.oracle.com/java/17/archive/jdk-17.0.7_macos-aarch64_bin.tar.gz"
        17          | ORACLE   | LINUX   | X86_64  | "https://download.oracle.com/java/17/archive/jdk-17.0.7_linux-x64_bin.tar.gz"
        17          | ORACLE   | LINUX   | AARCH64 | "https://download.oracle.com/java/17/archive/jdk-17.0.7_linux-aarch64_bin.tar.gz"
        17          | ORACLE   | WINDOWS | X86_64  | "https://download.oracle.com/java/17/archive/jdk-17.0.7_windows-x64_bin.zip"
    }

    def "does not resolve #os #arch #vendor jdk #langVersion"() {
        given:
        def resolver = resolverImplementation()

        when:
        Optional<JavaToolchainDownload> download = resolver.resolve(request(JavaLanguageVersion.of(langVersion), vendor, platform(os, arch)))

        then:
        download.isEmpty()
        where:
        langVersion | vendor   | os      | arch
        20          | ORACLE   | WINDOWS | AARCH64
        19          | ORACLE   | WINDOWS | AARCH64
        18          | ORACLE   | WINDOWS | AARCH64
        17          | ORACLE   | WINDOWS | AARCH64
    }

    JavaToolchainRequest request(JavaLanguageVersion languageVersion = null,
                                 JvmVendorSpec vendorSpec = vendor(),
                                 BuildPlatform platform = platform()) {
        JavaToolchainSpec toolchainSpec = Mock()
        Property<JavaLanguageVersion> languageVersionProperty = Mock()
        _ * toolchainSpec.getLanguageVersion() >> languageVersionProperty
        _ * languageVersionProperty.get() >> languageVersion

        Property<JvmVendorSpec> vendorSpecProperty = Mock()
        _ * vendorSpecProperty.get() >> vendorSpec
        _ * toolchainSpec.getVendor() >> vendorSpecProperty

        JavaToolchainRequest request = Mock()

        _ * request.getJavaToolchainSpec() >> toolchainSpec
        _ * request.getBuildPlatform() >> platform
        return request
    }

    JvmVendorSpec vendor() {
        return new AnyJvmVendorSpec();
    }

    BuildPlatform platform(OperatingSystem os = MAC_OS, Architecture arch = X86_64) {
        return new TestBuildPlatform(operatingSystem: os, architecture: arch)
    }

    JavaToolchainOracleJdkResolver resolverImplementation() {
        new JavaToolchainOracleJdkResolver() {
            @Override
            BuildServiceParameters.None getParameters() {
                return null
            }
        }
    }

    static class TestBuildPlatform implements BuildPlatform {
        OperatingSystem operatingSystem
        Architecture architecture
    }

    static class AnyJvmVendorSpec extends JvmVendorSpec {

        @Override
        boolean matches(String vendor) {
            return vendor.equals("any")
        }
    }
}
