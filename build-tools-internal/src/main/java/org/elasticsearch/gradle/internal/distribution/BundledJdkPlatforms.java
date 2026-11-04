/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.distribution;

import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.internal.info.BuildParameterExtension;

import java.util.List;

/**
 * Single source of truth for which OS / architecture combinations a bundled JDK, and therefore a
 * distribution, is available for. Both the bundled JDK definitions and the distribution archive
 * definitions must agree on this, otherwise a distribution can be registered for a platform that
 * has no bundled JDK.
 */
public final class BundledJdkPlatforms {

    /**
     * JDK 27 dropped support for macOS on x86_64.
     */
    static final int FIRST_MAJOR_WITHOUT_DARWIN_X64 = 27;

    /**
     * The platforms a JDK is bundled for, in the order the bundled JDKs are registered.
     */
    public static final List<String> PLATFORMS = List.of("darwin", "windows", "linux");

    private BundledJdkPlatforms() {}

    /**
     * The major version of the JDK bundled with the distributions. When building against a pre-release
     * runtime JDK, that JDK is bundled instead of the one defined in {@code version.properties}.
     */
    public static int bundledJdkMajor(BuildParameterExtension buildParams) {
        String major = buildParams.getRuntimeJava().isPreRelease()
            ? buildParams.getRuntimeJavaVersion().get().getMajorVersion()
            : VersionProperties.getBundledJdkMajorVersion();
        return Integer.parseInt(major);
    }

    /**
     * The architectures a JDK of the given major version is bundled for on the given platform.
     *
     * @param platform one of {@code linux}, {@code darwin} or {@code windows}
     */
    public static List<String> architecturesFor(String platform, int bundledJdkMajor) {
        return switch (platform) {
            case "linux" -> List.of("x64", "aarch64");
            case "darwin" -> bundledJdkMajor < FIRST_MAJOR_WITHOUT_DARWIN_X64 ? List.of("x64", "aarch64") : List.of("aarch64");
            case "windows" -> List.of("x64");
            default -> throw new IllegalArgumentException("Unknown platform [" + platform + "]");
        };
    }

    /**
     * Whether a JDK of the given major version is bundled for the given platform and architecture.
     */
    public static boolean isSupported(String platform, String architecture, int bundledJdkMajor) {
        return architecturesFor(platform, bundledJdkMajor).contains(architecture);
    }

    /**
     * The name the bundled JDK for the given platform and architecture is registered under in the {@code jdks} container.
     */
    public static String jdkName(String platform, String architecture) {
        return "bundled_" + platform + "_" + architecture;
    }
}
