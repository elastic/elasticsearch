/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.toolchain;

import org.elasticsearch.gradle.VersionProperties;
import org.gradle.api.provider.Property;
import org.gradle.jvm.toolchain.JavaLanguageVersion;
import org.gradle.jvm.toolchain.JavaToolchainDownload;
import org.gradle.jvm.toolchain.JavaToolchainRequest;
import org.gradle.jvm.toolchain.JavaToolchainResolver;
import org.gradle.jvm.toolchain.JavaToolchainSpec;
import org.gradle.jvm.toolchain.JvmVendorSpec;
import org.gradle.platform.Architecture;
import org.gradle.platform.OperatingSystem;
import org.jetbrains.annotations.NotNull;

import java.net.URI;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class JavaToolchainResolverImplementation implements JavaToolchainResolver {

    public static final Pattern VERSION_PATTERN = Pattern.compile(
        "(\\d+)(\\.\\d+\\.\\d+(?:\\.\\d+)?)?\\+(\\d+(?:\\.\\d+)?)(@([a-f0-9]{32}))?"
    );
    private final JavaLanguageVersion supportedLanguageVersion;

    public JavaToolchainResolverImplementation() {
        this.supportedLanguageVersion = resolveSupportedLanguageVersion();
    }

    @NotNull
    private static JavaLanguageVersion resolveSupportedLanguageVersion() {
        String bundledJdkVersion = VersionProperties.getBundledJdkVersion();
        Matcher matcher = VERSION_PATTERN.matcher(bundledJdkVersion);
        System.out.println("matcher.matches() = " + matcher.matches());
        return JavaLanguageVersion.of(matcher.group(1));
    }

    /**
     * We need some place to map JavaLanguageVersion to build, minor version etc.
     * */
    @Override
    public Optional<JavaToolchainDownload> resolve(JavaToolchainRequest request) {
        if (isRequestSupported(request) == false) {
            return Optional.empty();
        }
        String bundledJdkVendor = VersionProperties.getBundledJdkVendor();
        String bundledJdkVersion = VersionProperties.getBundledJdkVersion();
        Matcher jdkVersionMatcher = VERSION_PATTERN.matcher(bundledJdkVersion);
        jdkVersionMatcher.matches();
        String major = jdkVersionMatcher.group(1);
        String baseVersion = major + (jdkVersionMatcher.group(2) != null ? (jdkVersionMatcher.group(2)) : "");

        String build = jdkVersionMatcher.group(3);
        String hash = jdkVersionMatcher.group(5);

        OperatingSystem requestOperatingSystem = request.getBuildPlatform().getOperatingSystem();
        OperatingSystem effectiveOs = requestOperatingSystem != null ? requestOperatingSystem : OperatingSystem.MAC_OS;
        String extension = effectiveOs.equals(OperatingSystem.WINDOWS) ? "zip" : "tar.gz";

        JavaToolchainSpec javaToolchainSpec = request.getJavaToolchainSpec();
        JvmVendorSpec jvmVendorSpec = javaToolchainSpec.getVendor().get();

        String repoUrl = null;
        String artifactPattern = null;
        String arch = toArchString(request.getBuildPlatform().getArchitecture());
        String os = toOsString(request.getBuildPlatform().getOperatingSystem());
        if (jvmVendorSpec.matches("any") || jvmVendorSpec.equals(JvmVendorSpec.ORACLE)) {
            repoUrl = "https://download.oracle.com/";
            if (hash != null) {
                // current pattern since 12.0.1
                artifactPattern = "java/GA/jdk"
                    + major
                    + "/"
                    + hash
                    + "/"
                    + build
                    + "/GPL/openjdk-"
                    + major
                    + "_"
                    + os
                    + "-"
                    + arch
                    + "_bin."
                    + extension;
            } else {
                // simpler legacy pattern from JDK 9 to JDK 12 that we are advocating to Oracle to bring back
                artifactPattern = "java/GA/jdk"
                    + "jdk.getMajor()"
                    + "/"
                    + "jdk.getBuild()"
                    + "/GPL/openjdk-[revision]_[module]-[classifier]_bin."
                    + extension;
            }
        }
        System.out.println("repoUrl = " + repoUrl + artifactPattern);
        String finalRepoUrl = repoUrl;
        // JavaToolchainDownload download = (JavaToolchainDownload) ;
        String finalArtifactPattern = artifactPattern;
        return Optional.of(() -> URI.create(finalRepoUrl + finalArtifactPattern));
    }

    private String toOsString(OperatingSystem operatingSystem) {
        return switch (operatingSystem) {
            case MAC_OS -> "macos"; // todo: different for vendor adoptium
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

    private boolean isRequestSupported(JavaToolchainRequest request) {
        return isRequestedVendorSupported(request.getJavaToolchainSpec().getVendor())
            && isMajorVersionSupported(request.getJavaToolchainSpec().getLanguageVersion());
    }

    private boolean isMajorVersionSupported(Property<JavaLanguageVersion> requestedLanguageVersion) {
        JavaLanguageVersion javaLanguageVersion = requestedLanguageVersion.get() != null
            ? requestedLanguageVersion.get()
            : supportedLanguageVersion;
        return javaLanguageVersion.equals(supportedLanguageVersion);
    }

    private static boolean isRequestedVendorSupported(Property<JvmVendorSpec> jvmVendorSpecProperty) {
        JvmVendorSpec jvmVendorSpec = jvmVendorSpecProperty.get();
        return jvmVendorSpec.matches("any") || jvmVendorSpec.equals(JvmVendorSpec.ORACLE);
    }
}
