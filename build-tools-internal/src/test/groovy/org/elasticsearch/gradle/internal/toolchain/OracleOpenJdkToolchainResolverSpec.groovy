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
            OracleOpenJdkToolchainResolver.getBundledJdkBuild("24.0.2+12@fdc5d0102fe0414db21410ad5834341f", "24"),
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
         [24, ORACLE, MAC_OS, X86_64, "https://download.oracle.com/java/GA/jdk24.0.2/fdc5d0102fe0414db21410ad5834341f/12/GPL/openjdk-24.0.2_macos-x64_bin.tar.gz"],
         [24, ORACLE, MAC_OS, AARCH64, "https://download.oracle.com/java/GA/jdk24.0.2/fdc5d0102fe0414db21410ad5834341f/12/GPL/openjdk-24.0.2_macos-aarch64_bin.tar.gz"],
         [24, ORACLE, LINUX, X86_64, "https://download.oracle.com/java/GA/jdk24.0.2/fdc5d0102fe0414db21410ad5834341f/12/GPL/openjdk-24.0.2_linux-x64_bin.tar.gz"],
         [24, ORACLE, LINUX, AARCH64, "https://download.oracle.com/java/GA/jdk24.0.2/fdc5d0102fe0414db21410ad5834341f/12/GPL/openjdk-24.0.2_linux-aarch64_bin.tar.gz"],
         [24, ORACLE, WINDOWS, X86_64, "https://download.oracle.com/java/GA/jdk24.0.2/fdc5d0102fe0414db21410ad5834341f/12/GPL/openjdk-24.0.2_windows-x64_bin.zip"],
         [24, anyVendor(), MAC_OS, X86_64, "https://download.oracle.com/java/GA/jdk24.0.2/fdc5d0102fe0414db21410ad5834341f/12/GPL/openjdk-24.0.2_macos-x64_bin.tar.gz"],
         [24, anyVendor(), MAC_OS, AARCH64, "https://download.oracle.com/java/GA/jdk24.0.2/fdc5d0102fe0414db21410ad5834341f/12/GPL/openjdk-24.0.2_macos-aarch64_bin.tar.gz"],
         [24, anyVendor(), LINUX, X86_64, "https://download.oracle.com/java/GA/jdk24.0.2/fdc5d0102fe0414db21410ad5834341f/12/GPL/openjdk-24.0.2_linux-x64_bin.tar.gz"],
         [24, anyVendor(), LINUX, AARCH64, "https://download.oracle.com/java/GA/jdk24.0.2/fdc5d0102fe0414db21410ad5834341f/12/GPL/openjdk-24.0.2_linux-aarch64_bin.tar.gz"],
         [24, anyVendor(), WINDOWS, X86_64, "https://download.oracle.com/java/GA/jdk24.0.2/fdc5d0102fe0414db21410ad5834341f/12/GPL/openjdk-24.0.2_windows-x64_bin.zip"]
        ]
    }

    private static String urlPrefix(int i) {
        return "https://builds.es-jdk-archive.com/jdks/openjdk/" + i + "/"
    }

    def unsupportedRequests() {
        [[20, ORACLE, WINDOWS, AARCH64]]
    }

}
