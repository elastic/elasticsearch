/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.toolchain;

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
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class OracleOpenJdkToolchainResolver extends AbstractCustomJavaToolchainResolver {

    private static final Pattern VERSION_PATTERN = Pattern.compile(
        "(\\d+)(\\.\\d+\\.\\d+(?:\\.\\d+)?)?\\+(\\d+(?:\\.\\d+)?)(@([a-f0-9]{32}))?"
    );

    // for testing reasons we keep that a package private field
    String bundledJdkVersion = VersionProperties.getBundledJdkVersion();
    JavaLanguageVersion bundledJdkMajorVersion = JavaLanguageVersion.of(VersionProperties.getBundledJdkMajorVersion());

    /**
     * We need some place to map JavaLanguageVersion to build, minor version etc.
     * */
    @Override
    public Optional<JavaToolchainDownload> resolve(JavaToolchainRequest request) {
        if (requestIsSupported(request) == false) {
            return Optional.empty();
        }
        Matcher jdkVersionMatcher = VERSION_PATTERN.matcher(bundledJdkVersion);
        if (jdkVersionMatcher.matches() == false) {
            throw new IllegalStateException("Unable to parse bundled JDK version " + bundledJdkVersion);
        }
        String baseVersion = jdkVersionMatcher.group(1) + (jdkVersionMatcher.group(2) != null ? (jdkVersionMatcher.group(2)) : "");
        String build = jdkVersionMatcher.group(3);
        String hash = jdkVersionMatcher.group(5);

        OperatingSystem operatingSystem = request.getBuildPlatform().getOperatingSystem();
        String extension = operatingSystem.equals(OperatingSystem.WINDOWS) ? "zip" : "tar.gz";
        String arch = toArchString(request.getBuildPlatform().getArchitecture());
        String os = toOsString(operatingSystem);
        return Optional.of(
            () -> URI.create(
                "https://download.oracle.com/java/GA/jdk"
                    + baseVersion
                    + "/"
                    + hash
                    + "/"
                    + build
                    + "/GPL/openjdk-"
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
     * 1. BundledJdkVendor should match openjdk
     * 2. language version should match bundled jdk version
     * 3. vendor must be any or oracle
     * 4. Aarch64 windows images are not supported
     */
    private boolean requestIsSupported(JavaToolchainRequest request) {
        if (VersionProperties.getBundledJdkVendor().toLowerCase().equals("openjdk") == false) {
            return false;
        }
        JavaToolchainSpec javaToolchainSpec = request.getJavaToolchainSpec();
        if (javaToolchainSpec.getLanguageVersion().get().equals(bundledJdkMajorVersion) == false) {
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
