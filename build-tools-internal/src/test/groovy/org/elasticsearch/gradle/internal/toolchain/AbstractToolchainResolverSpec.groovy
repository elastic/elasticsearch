/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.toolchain

import org.gradle.api.provider.Property
import org.gradle.jvm.toolchain.JavaLanguageVersion
import org.gradle.jvm.toolchain.JavaToolchainDownload
import org.gradle.jvm.toolchain.JavaToolchainRequest
import org.gradle.jvm.toolchain.JavaToolchainResolver
import org.gradle.jvm.toolchain.JavaToolchainSpec
import org.gradle.jvm.toolchain.JvmVendorSpec
import org.gradle.platform.Architecture
import org.gradle.platform.BuildPlatform
import org.gradle.platform.OperatingSystem
import spock.lang.Specification

import static org.gradle.platform.Architecture.X86_64
import static org.gradle.platform.OperatingSystem.MAC_OS

abstract class AbstractToolchainResolverSpec extends Specification {

    def "resolves #os #arch #vendor jdk #langVersion"() {
        given:
        def resolver = resolverImplementation()

        when:
        Optional<JavaToolchainDownload> download = resolver.resolve(request(JavaLanguageVersion.of(langVersion), vendor, platform(os, arch)))

        then:
        download.get().uri == URI.create(expectedUrl)
        where:

        [langVersion, vendor, os, arch, expectedUrl] << supportedRequests()
    }


    def "does not resolve #os #arch #vendor jdk #langVersion"() {
        given:
        def resolver = resolverImplementation()

        when:
        Optional<JavaToolchainDownload> download = resolver.resolve(request(JavaLanguageVersion.of(langVersion), vendor, platform(os, arch)))

        then:
        download.isEmpty()
        where:
        [langVersion, vendor, os, arch] << unsupportedRequests()
    }

    abstract JavaToolchainResolver resolverImplementation();

    abstract supportedRequests();

    abstract unsupportedRequests();

    JavaToolchainRequest request(JavaLanguageVersion languageVersion = null,
                                 JvmVendorSpec vendorSpec = anyVendor(),
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

    JvmVendorSpec anyVendor() {
        return new AnyJvmVendorSpec();
    }

    BuildPlatform platform(OperatingSystem os = MAC_OS, Architecture arch = X86_64) {
        return new TestBuildPlatform(operatingSystem: os, architecture: arch)
    }


    static class TestBuildPlatform implements BuildPlatform {
        OperatingSystem operatingSystem
        Architecture architecture
    }

    static class AnyJvmVendorSpec extends JvmVendorSpec {
        @Override
        boolean matches(String vendor) {
            return vendor == "any"
        }

        @Override
        String toString() {
            return "any"
        }
    }
}
