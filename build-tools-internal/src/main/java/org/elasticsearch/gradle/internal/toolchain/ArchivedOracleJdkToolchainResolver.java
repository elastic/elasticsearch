/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.toolchain;

import org.apache.groovy.util.Maps;
import org.elasticsearch.gradle.VersionProperties;
import org.gradle.jvm.toolchain.JavaLanguageVersion;
import org.gradle.jvm.toolchain.JavaToolchainDownload;
import org.gradle.jvm.toolchain.JavaToolchainRequest;
import org.gradle.jvm.toolchain.JavaToolchainSpec;
import org.gradle.jvm.toolchain.JvmVendorSpec;
import org.gradle.platform.Architecture;
import org.gradle.platform.BuildPlatform;
import org.gradle.platform.OperatingSystem;

import java.net.URI;
import java.util.Map;
import java.util.Optional;

/**
 * Resolves released Oracle JDKs that are EOL.
 */
public abstract class ArchivedOracleJdkToolchainResolver extends AbstractCustomJavaToolchainResolver {

    private static final Map<Integer, String> ARCHIVED_BASE_VERSIONS = Maps.of(20, "20.0.2", 19, "19.0.2", 18, "18.0.2.1");

    @Override
    public Optional<JavaToolchainDownload> resolve(JavaToolchainRequest request) {
        if (requestIsSupported(request) == false) {
            return Optional.empty();
        }
        Integer majorVersion = request.getJavaToolchainSpec().getLanguageVersion().get().asInt();
        String baseVersion = ARCHIVED_BASE_VERSIONS.get(majorVersion);
        if (baseVersion == null) {
            return Optional.empty();
        }

        OperatingSystem operatingSystem = request.getBuildPlatform().getOperatingSystem();
        String extension = operatingSystem.equals(OperatingSystem.WINDOWS) ? "zip" : "tar.gz";
        String arch = toArchString(request.getBuildPlatform().getArchitecture());
        String os = toOsString(operatingSystem);
        return Optional.of(
            () -> URI.create(
                "https://download.oracle.com/java/"
                    + majorVersion
                    + "/archive/jdk-"
                    + baseVersion
                    + "_"
                    + os
                    + "-"
                    + arch
                    + "_bin."
                    + extension

            )
        );
    }

    /**
     * Check if request can be full-filled by this resolver:
     * 1. language version not matching bundled jdk version
     * 2. vendor must be any or oracle
     * 3. Aarch64 windows images are not supported
     */
    private boolean requestIsSupported(JavaToolchainRequest request) {
        JavaToolchainSpec javaToolchainSpec = request.getJavaToolchainSpec();
        JavaLanguageVersion bundledJdkMajorVersion = JavaLanguageVersion.of(VersionProperties.getBundledJdkMajorVersion());
        if (javaToolchainSpec.getLanguageVersion().get().equals(bundledJdkMajorVersion)) {
            return false;
        }
        if (anyVendorOr(javaToolchainSpec.getVendor().get(), JvmVendorSpec.ORACLE) == false) {
            return false;
        }
        BuildPlatform buildPlatform = request.getBuildPlatform();
        Architecture architecture = buildPlatform.getArchitecture();
        OperatingSystem operatingSystem = buildPlatform.getOperatingSystem();
        return Architecture.AARCH64 != architecture || OperatingSystem.WINDOWS != operatingSystem;
    }
}
