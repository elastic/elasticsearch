/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.toolchain


import org.gradle.api.services.BuildServiceParameters
import org.gradle.jvm.toolchain.JavaLanguageVersion
import static org.gradle.jvm.toolchain.JvmVendorSpec.ORACLE
import static org.gradle.platform.Architecture.*
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
            new OracleOpenJdkToolchainResolver.ReleasedJdkBuild(JavaLanguageVersion.of(20), "20", "36", "bdc68b4b9cbc4ebcb30745c85038d91d"),
            new OracleOpenJdkToolchainResolver.EarlyAccessJdkBuild(JavaLanguageVersion.of(21), "21", "6")
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
         // https://download.java.net/java/early_access/jdk23/23/GPL/openjdk-23-ea+23_macos-aarch64_bin.tar.gz
         [21, ORACLE, MAC_OS, X86_64, "https://download.java.net/java/early_access/jdk21/21/GPL/openjdk-21-ea+6_macos-x64_bin.tar.gz"],
         [21, ORACLE, MAC_OS, AARCH64, "https://download.java.net/java/early_access/jdk21/21/GPL/openjdk-21-ea+6_macos-aarch64_bin.tar.gz"],
         [21, ORACLE, LINUX, X86_64, "https://download.java.net/java/early_access/jdk21/21/GPL/openjdk-21-ea+6_linux-x64_bin.tar.gz"],
         [21, ORACLE, LINUX, AARCH64, "https://download.java.net/java/early_access/jdk21/21/GPL/openjdk-21-ea+6_linux-aarch64_bin.tar.gz"],
         [21, ORACLE, WINDOWS, X86_64, "https://download.java.net/java/early_access/jdk21/21/GPL/openjdk-21-ea+6_windows-x64_bin.zip"],
         [21, anyVendor(), MAC_OS, X86_64, "https://download.java.net/java/early_access/jdk21/21/GPL/openjdk-21-ea+6_macos-x64_bin.tar.gz"],
         [21, anyVendor(), MAC_OS, AARCH64, "https://download.java.net/java/early_access/jdk21/21/GPL/openjdk-21-ea+6_macos-aarch64_bin.tar.gz"],
         [21, anyVendor(), LINUX, X86_64, "https://download.java.net/java/early_access/jdk21/21/GPL/openjdk-21-ea+6_linux-x64_bin.tar.gz"],
         [21, anyVendor(), LINUX, AARCH64, "https://download.java.net/java/early_access/jdk21/21/GPL/openjdk-21-ea+6_linux-aarch64_bin.tar.gz"],
         [21, anyVendor(), WINDOWS, X86_64, "https://download.java.net/java/early_access/jdk21/21/GPL/openjdk-21-ea+6_windows-x64_bin.zip"]
        ]
    }

    def unsupportedRequests() {
        [
                [20, ORACLE, WINDOWS, AARCH64]
        ]
    }

}
