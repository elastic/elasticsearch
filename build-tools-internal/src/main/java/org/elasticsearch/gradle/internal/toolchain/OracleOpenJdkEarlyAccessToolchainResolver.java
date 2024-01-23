/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.toolchain;

import org.gradle.jvm.toolchain.JavaLanguageVersion;
import org.gradle.jvm.toolchain.JavaToolchainDownload;
import org.gradle.jvm.toolchain.JavaToolchainRequest;
import org.gradle.jvm.toolchain.JavaToolchainSpec;
import org.gradle.jvm.toolchain.JvmVendorSpec;
import org.gradle.platform.Architecture;
import org.gradle.platform.BuildPlatform;
import org.gradle.platform.OperatingSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Optional;

public abstract class OracleOpenJdkEarlyAccessToolchainResolver extends AbstractCustomJavaToolchainResolver {

    private static final Logger logger = LoggerFactory.getLogger(OracleOpenJdkEarlyAccessToolchainResolver.class);

    private static final String EA_VERSION = "22";
    private static final JavaLanguageVersion eaMajorVersion = JavaLanguageVersion.of(EA_VERSION);
    private static final String EA_BUILD = "31";

    /**
     * We need some place to map JavaLanguageVersion to build, minor version etc.
     * */
    @Override
    public Optional<JavaToolchainDownload> resolve(JavaToolchainRequest request) {
        if (requestIsSupported(request) == false) {
            return Optional.empty();
        }

        OperatingSystem operatingSystem = request.getBuildPlatform().getOperatingSystem();
        String extension = operatingSystem.equals(OperatingSystem.WINDOWS) ? "zip" : "tar.gz";
        String arch = toArchString(request.getBuildPlatform().getArchitecture());
        String os = toOsString(operatingSystem);
        return Optional.of(
            () -> URI.create(
                "https://download.oracle.com/java/early_access/jdk"
                    + EA_VERSION
                    + "/"
                    + EA_BUILD
                    + "/GPL/openjdk-"
                    + EA_VERSION
                    + "-ea+"
                    + EA_BUILD
                    + "_"
                    + os
                    + "-"
                    + arch
                    + "_bin."
                    + extension
            )
        );
    }

    private boolean requestIsSupported(JavaToolchainRequest request) {
        JavaToolchainSpec javaToolchainSpec = request.getJavaToolchainSpec();
        if (javaToolchainSpec.getLanguageVersion().get().equals(eaMajorVersion) == false) {
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
