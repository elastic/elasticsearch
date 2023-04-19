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
import org.gradle.jvm.toolchain.JavaToolchainResolver;
import org.gradle.jvm.toolchain.JvmVendorSpec;
import org.gradle.platform.Architecture;
import org.gradle.platform.BuildPlatform;
import org.gradle.platform.OperatingSystem;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class JavaToolchainOracleJdkResolver implements JavaToolchainResolver {

    private static final Pattern VERSION_PATTERN = Pattern.compile(
        "(\\d+)(\\.\\d+\\.\\d+(?:\\.\\d+)?)?\\+(\\d+(?:\\.\\d+)?)(@([a-f0-9]{32}))?"
    );

    private static final Map<Integer, String> ARCHIVED_BASE_VERSIONS = Maps.of(19, "19.0.2", 18, "18.0.2.1", 17, "17.0.7");

    // for testing reasons we keep that a package private field
    String bundledJdkVersion = VersionProperties.getBundledJdkVersion();

    /**
     * We need some place to map JavaLanguageVersion to build, minor version etc.
     * */
    @Override
    public Optional<JavaToolchainDownload> resolve(JavaToolchainRequest request) {
        JvmVendorSpec vendorSpec = request.getJavaToolchainSpec().getVendor().get();
        if (vendorSpecMatchesOracleJdk(vendorSpec) == false) {
            return Optional.empty();
        }
        BuildPlatform buildPlatform = request.getBuildPlatform();
        // Windows aarch64 distributions are not supported
        if(buildPlatform.getOperatingSystem().equals(OperatingSystem.WINDOWS) &&
                buildPlatform.getArchitecture().equals(Architecture.AARCH64) ) {
            return Optional.empty();
        }
        JavaLanguageVersion requestedLanguageVersion = request.getJavaToolchainSpec().getLanguageVersion().get();
        boolean currentBundledVersionRequested = VersionProperties.getBundledJdkVersion()
            .startsWith(Integer.toString(requestedLanguageVersion.asInt()));
        return currentBundledVersionRequested ? resolveCurrentBundledJdkURI(request) : resolveArchivedOracleJdk(request);
    }

    /**
     * This resolves the oracle built openjdk
     * */
    private Optional<JavaToolchainDownload> resolveCurrentBundledJdkURI(JavaToolchainRequest request) {
        Matcher jdkVersionMatcher = VERSION_PATTERN.matcher(bundledJdkVersion);
        jdkVersionMatcher.matches();
        String bundledMajor = jdkVersionMatcher.group(1);
        String baseVersion = bundledMajor + (jdkVersionMatcher.group(2) != null ? (jdkVersionMatcher.group(2)) : "");
        String build = jdkVersionMatcher.group(3);
        String hash = jdkVersionMatcher.group(5);

        OperatingSystem operatingSystem = request.getBuildPlatform().getOperatingSystem();
        String extension = operatingSystem.equals(OperatingSystem.WINDOWS) ? "zip" : "tar.gz";
        String arch = toArchString(request.getBuildPlatform().getArchitecture());
        String os = toOsString(operatingSystem);
        return Optional.of(
            () -> URI.create(
                "https://download.oracle.com/java/GA/jdk"
                    + bundledMajor
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
     * This resolves archived oracle jdks.
     * */
    private Optional<JavaToolchainDownload> resolveArchivedOracleJdk(JavaToolchainRequest request) {
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

    private static boolean vendorSpecMatchesOracleJdk(JvmVendorSpec vendorSpec) {
        return vendorSpec.matches("any") || vendorSpec.equals(JvmVendorSpec.ORACLE);
    }

    private String toOsString(OperatingSystem operatingSystem) {
        return switch (operatingSystem) {
            case MAC_OS -> "macos";
            case LINUX -> "linux";
            case WINDOWS -> "windows";
            default -> throw new UnsupportedOperationException("Operating system " + operatingSystem);
        };
    }

    private String toArchString(Architecture architecture) {
        return switch (architecture) {
            case X86_64 -> "x64";
            case AARCH64 -> "aarch64";
            case X86 -> "x86";
        };
    }
}
