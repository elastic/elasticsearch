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
import org.gradle.jvm.toolchain.JavaToolchainResolver
import org.gradle.jvm.toolchain.JvmVendorSpec

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
        resolver.earlyAccessJdkBuildResolver = () -> {
            return Optional.of(
                new EarlyAccessCatalogJdkToolchainResolver.EarlyAccessJdkBuild(JavaLanguageVersion.of(25), 31),
                new EarlyAccessCatalogJdkToolchainResolver.EarlyAccessJdkBuild(JavaLanguageVersion.of(26), 6)
            )
        }
        return resolver
    }

    def anyVendorMatch() {
        return JvmVendorSpec.ANY
    }
    @Override
    def supportedRequests() {
        return [
            [25, vSpec(
                25,
                30
            ), LINUX, X86_64, "https://builds.es-jdk-archive.com/jdks/openjdk/25/openjdk-25-ea+30/openjdk-25-ea+30_linux-x64_bin.tar.gz"],
            [26, vSpec(
                26,
                6
            ), WINDOWS, X86_64, "https://builds.es-jdk-archive.com/jdks/openjdk/26/openjdk-26-ea+6/openjdk-26-ea+6_windows-x64_bin.zip"],
            [26, vSpec(
                26,
                6
            ), MAC_OS, AARCH64, "https://builds.es-jdk-archive.com/jdks/openjdk/26/openjdk-26-ea+6/openjdk-26-ea+6_macos-aarch64_bin.tar.gz"]
        ]
    }

    private JvmVendorSpec vSpec(int version, int build) {
        return org.gradle.jvm.toolchain.internal.DefaultJvmVendorSpec.of("EarlyAccessJvmCatalog[" + version + "/" + build + "]");
    }

    @Override
    def unsupportedRequests() {
        return []
    }
}
