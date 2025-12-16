/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.toolchain

import org.elasticsearch.gradle.VersionProperties
import org.gradle.api.services.BuildServiceParameters
import org.gradle.jvm.toolchain.JavaLanguageVersion
import org.gradle.jvm.toolchain.JavaToolchainDownload
import org.gradle.jvm.toolchain.JavaToolchainResolver

import static org.gradle.platform.Architecture.AARCH64
import static org.gradle.platform.Architecture.X86_64
import static org.gradle.platform.OperatingSystem.*

class EarlyAccessCatalogJdkToolchainResolverSpec extends AbstractToolchainResolverSpec {
    @Override
    JavaToolchainResolver resolverImplementation() {
        def resolver = new EarlyAccessCatalogJdkToolchainResolver() {
            @Override
            BuildServiceParameters.None getParameters() {
                return null
            }
        }
        resolver.earlyAccessJdkBuildResolver = (JavaLanguageVersion version) -> {
            new EarlyAccessCatalogJdkToolchainResolver.PreReleaseJdkBuild(version, 30, 'ea')
        }
        return resolver
    }

    def "resolves rc versions #os #arch #vendor jdk #langVersion"() {
        given:
        def resolver = new EarlyAccessCatalogJdkToolchainResolver() {
            @Override
            BuildServiceParameters.None getParameters() {
                return null
            }
        }
        resolver.earlyAccessJdkBuildResolver = (JavaLanguageVersion version) -> {
            new EarlyAccessCatalogJdkToolchainResolver.PreReleaseJdkBuild(version, 30, 'rc')
        }
        when:
        Optional<JavaToolchainDownload> download = resolver.resolve(request(JavaLanguageVersion.of(langVersion), vendor, platform(os, arch)))

        then:
        download.get().uri == URI.create(expectedUrl)
        where:

        [langVersion, vendor, os, arch, expectedUrl] << [
            [25, anyVendor(), LINUX, X86_64, "https://builds.es-jdk-archive.com/jdks/openjdk/25/openjdk-25-rc+30/openjdk-25_linux-x64_bin.tar.gz"],
            [25, anyVendor(), LINUX, AARCH64, "https://builds.es-jdk-archive.com/jdks/openjdk/25/openjdk-25-rc+30/openjdk-25_linux-aarch64_bin.tar.gz"],
            [25, anyVendor(), MAC_OS, X86_64, "https://builds.es-jdk-archive.com/jdks/openjdk/25/openjdk-25-rc+30/openjdk-25_macos-x64_bin.tar.gz"],
            [25, anyVendor(), MAC_OS, AARCH64, "https://builds.es-jdk-archive.com/jdks/openjdk/25/openjdk-25-rc+30/openjdk-25_macos-aarch64_bin.tar.gz"],
            [25, anyVendor(), WINDOWS, X86_64, "https://builds.es-jdk-archive.com/jdks/openjdk/25/openjdk-25-rc+30/openjdk-25_windows-x64_bin.zip"],

            [26, anyVendor(), LINUX, X86_64, "https://builds.es-jdk-archive.com/jdks/openjdk/26/openjdk-26-rc+30/openjdk-26_linux-x64_bin.tar.gz"],
            [26, anyVendor(), LINUX, AARCH64, "https://builds.es-jdk-archive.com/jdks/openjdk/26/openjdk-26-rc+30/openjdk-26_linux-aarch64_bin.tar.gz"],
            [26, anyVendor(), MAC_OS, X86_64, "https://builds.es-jdk-archive.com/jdks/openjdk/26/openjdk-26-rc+30/openjdk-26_macos-x64_bin.tar.gz"],
            [26, anyVendor(), MAC_OS, AARCH64, "https://builds.es-jdk-archive.com/jdks/openjdk/26/openjdk-26-rc+30/openjdk-26_macos-aarch64_bin.tar.gz"],
            [26, anyVendor(), WINDOWS, X86_64, "https://builds.es-jdk-archive.com/jdks/openjdk/26/openjdk-26-rc+30/openjdk-26_windows-x64_bin.zip"]
        ]
    }


    @Override
    def supportedRequests() {
        return [
            [25, anyVendor(), LINUX, X86_64, "https://builds.es-jdk-archive.com/jdks/openjdk/25/openjdk-25-ea+30/openjdk-25-ea+30_linux-x64_bin.tar.gz"],
            [25, anyVendor(), LINUX, AARCH64, "https://builds.es-jdk-archive.com/jdks/openjdk/25/openjdk-25-ea+30/openjdk-25-ea+30_linux-aarch64_bin.tar.gz"],
            [25, anyVendor(), MAC_OS, X86_64, "https://builds.es-jdk-archive.com/jdks/openjdk/25/openjdk-25-ea+30/openjdk-25-ea+30_macos-x64_bin.tar.gz"],
            [25, anyVendor(), MAC_OS, AARCH64, "https://builds.es-jdk-archive.com/jdks/openjdk/25/openjdk-25-ea+30/openjdk-25-ea+30_macos-aarch64_bin.tar.gz"],
            [25, anyVendor(), WINDOWS, X86_64, "https://builds.es-jdk-archive.com/jdks/openjdk/25/openjdk-25-ea+30/openjdk-25-ea+30_windows-x64_bin.zip"],

            [26, anyVendor(), LINUX, X86_64, "https://builds.es-jdk-archive.com/jdks/openjdk/26/openjdk-26-ea+30/openjdk-26-ea+30_linux-x64_bin.tar.gz"],
            [26, anyVendor(), LINUX, AARCH64, "https://builds.es-jdk-archive.com/jdks/openjdk/26/openjdk-26-ea+30/openjdk-26-ea+30_linux-aarch64_bin.tar.gz"],
            [26, anyVendor(), MAC_OS, X86_64, "https://builds.es-jdk-archive.com/jdks/openjdk/26/openjdk-26-ea+30/openjdk-26-ea+30_macos-x64_bin.tar.gz"],
            [26, anyVendor(), MAC_OS, AARCH64, "https://builds.es-jdk-archive.com/jdks/openjdk/26/openjdk-26-ea+30/openjdk-26-ea+30_macos-aarch64_bin.tar.gz"],
            [26, anyVendor(), WINDOWS, X86_64, "https://builds.es-jdk-archive.com/jdks/openjdk/26/openjdk-26-ea+30/openjdk-26-ea+30_windows-x64_bin.zip"]
        ]
    }

    @Override
    def unsupportedRequests() {
        [
            [Integer.parseInt(VersionProperties.bundledJdkMajorVersion) + 1, anyVendor(), WINDOWS, AARCH64],
        ]
    }
}
