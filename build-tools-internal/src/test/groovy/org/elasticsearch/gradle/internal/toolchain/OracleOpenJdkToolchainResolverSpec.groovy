/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
        toolChain.bundledJdkVersion = "20+36@bdc68b4b9cbc4ebcb30745c85038d91d"
        toolChain.bundledJdkMajorVersion = JavaLanguageVersion.of(20)
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
         [20, anyVendor(), WINDOWS, X86_64, "https://download.oracle.com/java/GA/jdk20/bdc68b4b9cbc4ebcb30745c85038d91d/36/GPL/openjdk-20_windows-x64_bin.zip"]]
    }

    def unsupportedRequests() {
        [
                [20, ORACLE, WINDOWS, AARCH64]
        ]
    }

}
