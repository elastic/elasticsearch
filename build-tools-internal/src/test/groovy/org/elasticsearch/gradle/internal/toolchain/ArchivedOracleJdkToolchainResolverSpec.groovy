/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.toolchain

import org.gradle.api.services.BuildServiceParameters
import org.gradle.jvm.toolchain.JavaToolchainResolver;

import static org.gradle.jvm.toolchain.JvmVendorSpec.ORACLE
import static org.gradle.platform.Architecture.AARCH64
import static org.gradle.platform.Architecture.X86_64
import static org.gradle.platform.OperatingSystem.LINUX
import static org.gradle.platform.OperatingSystem.MAC_OS
import static org.gradle.platform.OperatingSystem.WINDOWS;

class ArchivedOracleJdkToolchainResolverSpec extends AbstractToolchainResolverSpec {

    @Override
    def supportedRequests() {
        return [
                [19, ORACLE, MAC_OS, X86_64, "https://download.oracle.com/java/19/archive/jdk-19.0.2_macos-x64_bin.tar.gz"],
                [19, ORACLE, MAC_OS, AARCH64, "https://download.oracle.com/java/19/archive/jdk-19.0.2_macos-aarch64_bin.tar.gz"],
                [19, ORACLE, LINUX, X86_64, "https://download.oracle.com/java/19/archive/jdk-19.0.2_linux-x64_bin.tar.gz"],
                [19, ORACLE, LINUX, AARCH64, "https://download.oracle.com/java/19/archive/jdk-19.0.2_linux-aarch64_bin.tar.gz"],
                [19, ORACLE, WINDOWS, X86_64, "https://download.oracle.com/java/19/archive/jdk-19.0.2_windows-x64_bin.zip"],

                [18, ORACLE, MAC_OS, X86_64, "https://download.oracle.com/java/18/archive/jdk-18.0.2.1_macos-x64_bin.tar.gz"],
                [18, ORACLE, MAC_OS, AARCH64, "https://download.oracle.com/java/18/archive/jdk-18.0.2.1_macos-aarch64_bin.tar.gz"],
                [18, ORACLE, LINUX, X86_64, "https://download.oracle.com/java/18/archive/jdk-18.0.2.1_linux-x64_bin.tar.gz"],
                [18, ORACLE, LINUX, AARCH64, "https://download.oracle.com/java/18/archive/jdk-18.0.2.1_linux-aarch64_bin.tar.gz"],
                [18, ORACLE, WINDOWS, X86_64, "https://download.oracle.com/java/18/archive/jdk-18.0.2.1_windows-x64_bin.zip"],

                [17, ORACLE, MAC_OS, X86_64, "https://download.oracle.com/java/17/archive/jdk-17.0.7_macos-x64_bin.tar.gz"],
                [17, ORACLE, MAC_OS, AARCH64, "https://download.oracle.com/java/17/archive/jdk-17.0.7_macos-aarch64_bin.tar.gz"],
                [17, ORACLE, LINUX, X86_64, "https://download.oracle.com/java/17/archive/jdk-17.0.7_linux-x64_bin.tar.gz"],
                [17, ORACLE, LINUX, AARCH64, "https://download.oracle.com/java/17/archive/jdk-17.0.7_linux-aarch64_bin.tar.gz"],
                [17, ORACLE, WINDOWS, X86_64, "https://download.oracle.com/java/17/archive/jdk-17.0.7_windows-x64_bin.zip"]
        ]
    }

    def unsupportedRequests() {
        [
                [19, ORACLE, WINDOWS, AARCH64],
                [18, ORACLE, WINDOWS, AARCH64],
                [17, ORACLE, WINDOWS, AARCH64]
        ]
    }

    JavaToolchainResolver resolverImplementation() {
        new ArchivedOracleJdkToolchainResolver() {
            @Override
            BuildServiceParameters.None getParameters() {
                return null
            }
        }
    }
}
