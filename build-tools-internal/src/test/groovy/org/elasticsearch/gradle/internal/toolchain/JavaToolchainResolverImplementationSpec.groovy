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
import org.gradle.platform.BuildPlatform
import spock.lang.Specification

class JavaToolchainResolverImplementationSpec extends Specification {

    def "resolves macos oralce openjdk"() {
        given:
        def resolver = resolverImplementation()

        when:
        Optional<JavaToolchainDownload> download = resolver.resolve(request())

        then:
        download.get().uri == "blubb"
    }

// Downloading http://localhost:63544/jdk-12.0.2+10/linux/x64/jdk/hotspot/normal/adoptium to /private/var/folders/27/7xdfg70s6yj2r1kb5q_6v8000000gn/T/spock_transforms_are_reus_0_gradleUserHome769490696023225598/.tmp/gradle_download16413814612507170969bin
    JavaToolchainRequest request(JvmVendorSpec vendorSpec =  vendor()) {
        JavaToolchainSpec toolchainSpec = Mock()
        Property<JavaLanguageVersion> languageVersionProperty = Mock()
        _ * toolchainSpec.getLanguageVersion() >> languageVersionProperty

        Property<JvmVendorSpec> vendorSpecProperty = Mock()
        _ * vendorSpecProperty.get() >> vendorSpec
        _ * toolchainSpec.getVendor() >> vendorSpecProperty

        BuildPlatform platform = Mock()

        JavaToolchainRequest request = Mock()

        _ * request.getJavaToolchainSpec() >> toolchainSpec
        _ * request.getBuildPlatform() >> platform
        return request

    }

    JvmVendorSpec vendor() {
        return null
    }

    JavaToolchainResolverImplementation resolverImplementation() {
        new JavaToolchainResolverImplementation() {
            @Override
            BuildServiceParameters.None getParameters() {
                return null
            }
        }
    }
}
