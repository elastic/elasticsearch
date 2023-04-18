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

import static org.gradle.jvm.toolchain.JvmVendorSpec.ADOPTIUM
import static org.gradle.jvm.toolchain.JvmVendorSpec.ADOPTOPENJDK
import static org.gradle.jvm.toolchain.JvmVendorSpec.AMAZON
import static org.gradle.jvm.toolchain.JvmVendorSpec.APPLE
import static org.gradle.jvm.toolchain.JvmVendorSpec.AZUL
import static org.gradle.jvm.toolchain.JvmVendorSpec.BELLSOFT
import static org.gradle.jvm.toolchain.JvmVendorSpec.GRAAL_VM
import static org.gradle.jvm.toolchain.JvmVendorSpec.HEWLETT_PACKARD
import static org.gradle.jvm.toolchain.JvmVendorSpec.IBM
import static org.gradle.jvm.toolchain.JvmVendorSpec.MICROSOFT
import static org.gradle.jvm.toolchain.JvmVendorSpec.ORACLE
import static org.gradle.jvm.toolchain.JvmVendorSpec.SAP
import static org.gradle.platform.Architecture.*
import static org.gradle.platform.OperatingSystem.*

class JavaToolchainResolverImplementationSpec extends Specification {

    def "can match version pattern"() {
        when:
        def matcher = JavaToolchainResolverImplementation.VERSION_PATTERN.matcher("20+36@bdc68b4b9cbc4ebcb30745c85038d91d")
        then:
        matcher.matches()
        matcher.group(0) == "20+36@bdc68b4b9cbc4ebcb30745c85038d91d"
        matcher.group(1) == "20"
        matcher.group(2) == null
        matcher.group(3) == "36"
        matcher.group(5) == "bdc68b4b9cbc4ebcb30745c85038d91d"
    }

    def "fails on non  match version pattern"() {
        when:
        def matcher = JavaToolchainResolverImplementation.VERSION_PATTERN.matcher("20+36@bdc68b4b9cbc4ebcb30745c85038d91d")
        then:
        matcher.matches()
        matcher.group(0) == "20+36@bdc68b4b9cbc4ebcb30745c85038d91d"
        matcher.group(1) == "20"
        matcher.group(2) == null
        matcher.group(3) == "36"
        matcher.group(5) == "bdc68b4b9cbc4ebcb30745c85038d91d"
    }

    def "resolves #os #arch oracle openjdk"() {
        given:
        def resolver = resolverImplementation()

        when:
        Optional<JavaToolchainDownload> download = resolver.resolve(request(null, ORACLE, platform(os, arch)))

        then:
        download.get().uri == URI.create(expectedUrl)
        where:
        os      | arch    | expectedUrl
        MAC_OS  | X86_64  | "https://download.oracle.com/java/GA/jdk20/bdc68b4b9cbc4ebcb30745c85038d91d/36/GPL/openjdk-20_macos-x64_bin.tar.gz"
        MAC_OS  | AARCH64 | "https://download.oracle.com/java/GA/jdk20/bdc68b4b9cbc4ebcb30745c85038d91d/36/GPL/openjdk-20_macos-aarch64_bin.tar.gz"
        LINUX   | X86_64  | "https://download.oracle.com/java/GA/jdk20/bdc68b4b9cbc4ebcb30745c85038d91d/36/GPL/openjdk-20_linux-x64_bin.tar.gz"
        LINUX   | AARCH64 | "https://download.oracle.com/java/GA/jdk20/bdc68b4b9cbc4ebcb30745c85038d91d/36/GPL/openjdk-20_linux-aarch64_bin.tar.gz"
        WINDOWS | X86_64  | "https://download.oracle.com/java/GA/jdk20/bdc68b4b9cbc4ebcb30745c85038d91d/36/GPL/openjdk-20_windows-x64_bin.zip"
        WINDOWS | AARCH64 | "https://download.oracle.com/java/GA/jdk20/bdc68b4b9cbc4ebcb30745c85038d91d/36/GPL/openjdk-20_windows-aarch64_bin.zip"
    }

    def "does not provide jdk vendor #vendor"() {
        given:
        def resolver = resolverImplementation()

        when:
        Optional<JavaToolchainDownload> download = resolver.resolve(request(JavaLanguageVersion.of(20), vendor))

        then:
        download.isEmpty()

        where:
        vendor << [ADOPTIUM, AMAZON, ADOPTOPENJDK, APPLE, AZUL, BELLSOFT, GRAAL_VM, HEWLETT_PACKARD, IBM, MICROSOFT, SAP]
    }

    def "does not provide unsupported jdk versions"() {
        given:
        def resolver = resolverImplementation()
        when:
        Optional<JavaToolchainDownload> download = resolver.resolve(request(JavaLanguageVersion.of(19)))
        then:
        download.isEmpty()
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

    JavaToolchainResolverImplementation resolverImplementation() {
        new JavaToolchainResolverImplementation() {
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
