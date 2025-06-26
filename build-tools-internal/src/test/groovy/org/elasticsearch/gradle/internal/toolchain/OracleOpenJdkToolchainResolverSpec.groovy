/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.toolchain

import spock.util.environment.RestoreSystemProperties

import org.gradle.api.services.BuildServiceParameters
import org.gradle.jvm.toolchain.JavaLanguageVersion
import org.gradle.jvm.toolchain.JavaToolchainDownload

import static org.gradle.jvm.toolchain.JvmVendorSpec.ORACLE
import static org.gradle.platform.Architecture.AARCH64
import static org.gradle.platform.Architecture.X86_64
import static org.gradle.platform.OperatingSystem.*

class OracleOpenJdkToolchainResolverSpec extends AbstractToolchainResolverSpec {


    OracleOpenJdkToolchainResolver resolverImplementation() {
        var toolChain = new OracleOpenJdkToolchainResolver() {
            @Override
            BuildServiceParameters.None getParameters() {
                return null
            }
        }
        toolChain.builds = [
            new OracleOpenJdkToolchainResolver.ReleaseJdkBuild(
                JavaLanguageVersion.of(20),
                "download.oracle.com",
                "20",
                "36",
                "bdc68b4b9cbc4ebcb30745c85038d91d"
            ),
            OracleOpenJdkToolchainResolver.getBundledJdkBuild("24+36@1f9ff9062db4449d8ca828c504ffae90", "24"),
            OracleOpenJdkToolchainResolver.getEarlyAccessBuild(JavaLanguageVersion.of(25), "3")
        ]
        toolChain
    }

    def supportedRequests() {
        [[20, ORACLE, MAC_OS, X86_64, "https://download.oracle.com/java/GA/jdk20/bdc68b4b9cbc4ebcb30745c85038d91d/36/GPL/openjdk-20_macos-x64_bin.tar.gz"],
         [20, ORACLE, MAC_OS, AARCH64, "https://download.oracle.com/java/GA/jdk20/bdc68b4b9cbc4ebcb30745c85038d91d/36/GPL/openjdk-20_macos-aarch64_bin.tar.gz"],
         [20, ORACLE, LINUX, X86_64, "https://download.oracle.com/java/GA/jdk20/bdc68b4b9cbc4ebcb30745c85038d91d/36/GPL/openjdk-20_linux-x64_bin.tar.gz"],
         [20, ORACLE, LINUX, AARCH64, "https://download.oracle.com/java/GA/jdk20/bdc68b4b9cbc4ebcb30745c85038d91d/36/GPL/openjdk-20_linux-aarch64_bin.tar.gz"],
         [20, ORACLE, WINDOWS, X86_64, "https://download.oracle.com/java/GA/jdk20/bdc68b4b9cbc4ebcb30745c85038d91d/36/GPL/openjdk-20_windows-x64_bin.zip"],
         [20, anyVendor(), MAC_OS, X86_64, "https://download.oracle.com/java/GA/jdk20/bdc68b4b9cbc4ebcb30745c85038d91d/36/GPL/openjdk-20_macos-x64_bin.tar.gz"],
         [20, anyVendor(), MAC_OS, AARCH64, "https://download.oracle.com/java/GA/jdk20/bdc68b4b9cbc4ebcb30745c85038d91d/36/GPL/openjdk-20_macos-aarch64_bin.tar.gz"],
         [20, anyVendor(), LINUX, X86_64, "https://download.oracle.com/java/GA/jdk20/bdc68b4b9cbc4ebcb30745c85038d91d/36/GPL/openjdk-20_linux-x64_bin.tar.gz"],
         [20, anyVendor(), LINUX, AARCH64, "https://download.oracle.com/java/GA/jdk20/bdc68b4b9cbc4ebcb30745c85038d91d/36/GPL/openjdk-20_linux-aarch64_bin.tar.gz"],
         [20, anyVendor(), WINDOWS, X86_64, "https://download.oracle.com/java/GA/jdk20/bdc68b4b9cbc4ebcb30745c85038d91d/36/GPL/openjdk-20_windows-x64_bin.zip"],
         // bundled jdk
         [24, ORACLE, MAC_OS, X86_64, "https://download.oracle.com/java/GA/jdk24/1f9ff9062db4449d8ca828c504ffae90/36/GPL/openjdk-24_macos-x64_bin.tar.gz"],
         [24, ORACLE, MAC_OS, AARCH64, "https://download.oracle.com/java/GA/jdk24/1f9ff9062db4449d8ca828c504ffae90/36/GPL/openjdk-24_macos-aarch64_bin.tar.gz"],
         [24, ORACLE, LINUX, X86_64, "https://download.oracle.com/java/GA/jdk24/1f9ff9062db4449d8ca828c504ffae90/36/GPL/openjdk-24_linux-x64_bin.tar.gz"],
         [24, ORACLE, LINUX, AARCH64, "https://download.oracle.com/java/GA/jdk24/1f9ff9062db4449d8ca828c504ffae90/36/GPL/openjdk-24_linux-aarch64_bin.tar.gz"],
         [24, ORACLE, WINDOWS, X86_64, "https://download.oracle.com/java/GA/jdk24/1f9ff9062db4449d8ca828c504ffae90/36/GPL/openjdk-24_windows-x64_bin.zip"],
         [24, anyVendor(), MAC_OS, X86_64, "https://download.oracle.com/java/GA/jdk24/1f9ff9062db4449d8ca828c504ffae90/36/GPL/openjdk-24_macos-x64_bin.tar.gz"],
         [24, anyVendor(), MAC_OS, AARCH64, "https://download.oracle.com/java/GA/jdk24/1f9ff9062db4449d8ca828c504ffae90/36/GPL/openjdk-24_macos-aarch64_bin.tar.gz"],
         [24, anyVendor(), LINUX, X86_64, "https://download.oracle.com/java/GA/jdk24/1f9ff9062db4449d8ca828c504ffae90/36/GPL/openjdk-24_linux-x64_bin.tar.gz"],
         [24, anyVendor(), LINUX, AARCH64, "https://download.oracle.com/java/GA/jdk24/1f9ff9062db4449d8ca828c504ffae90/36/GPL/openjdk-24_linux-aarch64_bin.tar.gz"],
         [24, anyVendor(), WINDOWS, X86_64, "https://download.oracle.com/java/GA/jdk24/1f9ff9062db4449d8ca828c504ffae90/36/GPL/openjdk-24_windows-x64_bin.zip"],
         // EA build
         [25, ORACLE, MAC_OS, X86_64, "https://download.java.net/java/early_access/jdk25/3/GPL/openjdk-25-ea+3_macos-x64_bin.tar.gz"],
         [25, ORACLE, MAC_OS, AARCH64, "https://download.java.net/java/early_access/jdk25/3/GPL/openjdk-25-ea+3_macos-aarch64_bin.tar.gz"],
         [25, ORACLE, LINUX, X86_64, "https://download.java.net/java/early_access/jdk25/3/GPL/openjdk-25-ea+3_linux-x64_bin.tar.gz"],
         [25, ORACLE, LINUX, AARCH64, "https://download.java.net/java/early_access/jdk25/3/GPL/openjdk-25-ea+3_linux-aarch64_bin.tar.gz"],
         [25, ORACLE, WINDOWS, X86_64, "https://download.java.net/java/early_access/jdk25/3/GPL/openjdk-25-ea+3_windows-x64_bin.zip"],
         [25, anyVendor(), MAC_OS, X86_64, "https://download.java.net/java/early_access/jdk25/3/GPL/openjdk-25-ea+3_macos-x64_bin.tar.gz"],
         [25, anyVendor(), MAC_OS, AARCH64, "https://download.java.net/java/early_access/jdk25/3/GPL/openjdk-25-ea+3_macos-aarch64_bin.tar.gz"],
         [25, anyVendor(), LINUX, X86_64, "https://download.java.net/java/early_access/jdk25/3/GPL/openjdk-25-ea+3_linux-x64_bin.tar.gz"],
         [25, anyVendor(), LINUX, AARCH64, "https://download.java.net/java/early_access/jdk25/3/GPL/openjdk-25-ea+3_linux-aarch64_bin.tar.gz"],
         [25, anyVendor(), WINDOWS, X86_64, "https://download.java.net/java/early_access/jdk25/3/GPL/openjdk-25-ea+3_windows-x64_bin.zip"]]
    }

    @RestoreSystemProperties
    def "can provide build number for ea versions"() {
        given:
        System.setProperty('runtime.java.build', "42")
        System.setProperty('runtime.java.25.build', "13")
        def resolver = resolverImplementation()

        when:
        Optional<JavaToolchainDownload> download = resolver.resolve(
            request(
                JavaLanguageVersion.of(version),
                vendor,
                platform(os, arch)
            )
        )

        then:
        download.get().uri == URI.create(expectedUrl)

        where:
        version | vendor      | os      | arch    | expectedUrl
        25      | ORACLE      | MAC_OS  | X86_64  | urlPrefix(25) + "13/GPL/openjdk-25-ea+13_macos-x64_bin.tar.gz"
        25      | ORACLE      | MAC_OS  | AARCH64 | urlPrefix(25) + "13/GPL/openjdk-25-ea+13_macos-aarch64_bin.tar.gz"
        25      | ORACLE      | LINUX   | X86_64  | urlPrefix(25) + "13/GPL/openjdk-25-ea+13_linux-x64_bin.tar.gz"
        25      | ORACLE      | LINUX   | AARCH64 | urlPrefix(25) + "13/GPL/openjdk-25-ea+13_linux-aarch64_bin.tar.gz"
        25      | ORACLE      | WINDOWS | X86_64  | urlPrefix(25) + "13/GPL/openjdk-25-ea+13_windows-x64_bin.zip"
        25      | anyVendor() | MAC_OS  | X86_64  | urlPrefix(25) + "13/GPL/openjdk-25-ea+13_macos-x64_bin.tar.gz"
        25      | anyVendor() | MAC_OS  | AARCH64 | urlPrefix(25) + "13/GPL/openjdk-25-ea+13_macos-aarch64_bin.tar.gz"
        25      | anyVendor() | LINUX   | X86_64  | urlPrefix(25) + "13/GPL/openjdk-25-ea+13_linux-x64_bin.tar.gz"
        25      | anyVendor() | LINUX   | AARCH64 | urlPrefix(25) + "13/GPL/openjdk-25-ea+13_linux-aarch64_bin.tar.gz"
        25      | anyVendor() | WINDOWS | X86_64  | urlPrefix(25) + "13/GPL/openjdk-25-ea+13_windows-x64_bin.zip"
    }

    private static String urlPrefix(int i) {
        return "https://download.java.net/java/early_access/jdk" + i + "/"
    }

    def unsupportedRequests() {
        [[20, ORACLE, WINDOWS, AARCH64]]
    }

}
