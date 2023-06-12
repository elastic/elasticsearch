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
import org.gradle.jvm.toolchain.JavaToolchainResolver
import org.gradle.platform.OperatingSystem

import static org.elasticsearch.gradle.internal.toolchain.AbstractCustomJavaToolchainResolver.toArchString
import static org.elasticsearch.gradle.internal.toolchain.AbstractCustomJavaToolchainResolver.toOsString
import static org.gradle.jvm.toolchain.JvmVendorSpec.ADOPTIUM
import static org.gradle.platform.Architecture.AARCH64
import static org.gradle.platform.Architecture.X86_64
import static org.gradle.platform.OperatingSystem.LINUX
import static org.gradle.platform.OperatingSystem.MAC_OS
import static org.gradle.platform.OperatingSystem.WINDOWS

class AdoptiumJdkToolchainResolverSpec extends AbstractToolchainResolverSpec {

    @Override
    JavaToolchainResolver resolverImplementation() {
        def resolver = new AdoptiumJdkToolchainResolver() {
            @Override
            BuildServiceParameters.None getParameters() {
                return null
            }
        }
        supportedRequests().each {
            def languageVersion = JavaLanguageVersion.of(it[0])
            def request = new AdoptiumJdkToolchainResolver.AdoptiumVersionRequest(
                    toOsString(it[2], it[1]),
                    toArchString(it[3]),
                    languageVersion);
            resolver.CACHED_SEMVERS.put(request, Optional.of(new AdoptiumJdkToolchainResolver.AdoptiumVersionInfo(languageVersion.asInt(),
                    1,
                    1,
                    "" + languageVersion.asInt() + ".1.1.1+37",
                    0, "" + languageVersion.asInt() + ".1.1.1"
            )))

        }
        return resolver
    }

    @Override
    def supportedRequests() {
        return [
                [19, ADOPTIUM, MAC_OS, X86_64, "https://api.adoptium.net/v3/binary/version/jdk-19.1.1.1+37/mac/x64/jdk/hotspot/normal/eclipse?project=jdk"],
                [19, ADOPTIUM, LINUX, X86_64, "https://api.adoptium.net/v3/binary/version/jdk-19.1.1.1+37/linux/x64/jdk/hotspot/normal/eclipse?project=jdk"],
                [19, ADOPTIUM, WINDOWS, X86_64, "https://api.adoptium.net/v3/binary/version/jdk-19.1.1.1+37/windows/x64/jdk/hotspot/normal/eclipse?project=jdk"],
                [19, ADOPTIUM, MAC_OS, AARCH64, "https://api.adoptium.net/v3/binary/version/jdk-19.1.1.1+37/mac/aarch64/jdk/hotspot/normal/eclipse?project=jdk"],
                [19, ADOPTIUM, LINUX, AARCH64, "https://api.adoptium.net/v3/binary/version/jdk-19.1.1.1+37/linux/aarch64/jdk/hotspot/normal/eclipse?project=jdk"],

                [18, ADOPTIUM, MAC_OS, X86_64, "https://api.adoptium.net/v3/binary/version/jdk-18.1.1.1+37/mac/x64/jdk/hotspot/normal/eclipse?project=jdk"],
                [18, ADOPTIUM, LINUX, X86_64, "https://api.adoptium.net/v3/binary/version/jdk-18.1.1.1+37/linux/x64/jdk/hotspot/normal/eclipse?project=jdk"],
                [18, ADOPTIUM, WINDOWS, X86_64, "https://api.adoptium.net/v3/binary/version/jdk-18.1.1.1+37/windows/x64/jdk/hotspot/normal/eclipse?project=jdk"],
                [18, ADOPTIUM, MAC_OS, AARCH64, "https://api.adoptium.net/v3/binary/version/jdk-18.1.1.1+37/mac/aarch64/jdk/hotspot/normal/eclipse?project=jdk"],
                [18, ADOPTIUM, LINUX, AARCH64, "https://api.adoptium.net/v3/binary/version/jdk-18.1.1.1+37/linux/aarch64/jdk/hotspot/normal/eclipse?project=jdk"],
                [17, ADOPTIUM, MAC_OS, X86_64, "https://api.adoptium.net/v3/binary/version/jdk-17.1.1.1+37/mac/x64/jdk/hotspot/normal/eclipse?project=jdk"],
                [17, ADOPTIUM, LINUX, X86_64, "https://api.adoptium.net/v3/binary/version/jdk-17.1.1.1+37/linux/x64/jdk/hotspot/normal/eclipse?project=jdk"],
                [17, ADOPTIUM, WINDOWS, X86_64, "https://api.adoptium.net/v3/binary/version/jdk-17.1.1.1+37/windows/x64/jdk/hotspot/normal/eclipse?project=jdk"],
                [17, ADOPTIUM, MAC_OS, AARCH64, "https://api.adoptium.net/v3/binary/version/jdk-17.1.1.1+37/mac/aarch64/jdk/hotspot/normal/eclipse?project=jdk"],
                [17, ADOPTIUM, LINUX, AARCH64, "https://api.adoptium.net/v3/binary/version/jdk-17.1.1.1+37/linux/aarch64/jdk/hotspot/normal/eclipse?project=jdk"]
        ]
    }

    @Override
    def unsupportedRequests() {
        [
                [19, ADOPTIUM, WINDOWS, AARCH64],
                [18, ADOPTIUM, WINDOWS, AARCH64],
                [17, ADOPTIUM, WINDOWS, AARCH64]
        ]
    }

}
