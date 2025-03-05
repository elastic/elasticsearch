/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class OracleOpenJdkToolchainResolver extends AbstractCustomJavaToolchainResolver {

    interface JdkBuild {
        JavaLanguageVersion languageVersion();

        String url(String os, String arch, String extension);
    }

    record ReleaseJdkBuild(JavaLanguageVersion languageVersion, String host, String version, String buildNumber, String hash)
        implements
            JdkBuild {

        @Override
        public String url(String os, String arch, String extension) {
            return "https://"
                + host
                + "/java/GA/jdk"
                + version
                + "/"
                + hash
                + "/"
                + buildNumber
                + "/GPL/openjdk-"
                + version
                + "_"
                + os
                + "-"
                + arch
                + "_bin."
                + extension;
        }
    }

    record EarlyAccessJdkBuild(JavaLanguageVersion languageVersion) implements JdkBuild {
        @Override
        public String url(String os, String arch, String extension) {
            String buildNumber = resolveBuildNumber(languageVersion.asInt());
            return "https://download.java.net/java/early_access/jdk"
                + languageVersion.asInt()
                + "/"
                + buildNumber
                + "/GPL/openjdk-"
                + languageVersion.asInt()
                + "-ea+"
                + buildNumber
                + "_"
                + os
                + "-"
                + arch
                + "_bin."
                + extension;
        }

        private static String resolveBuildNumber(int version) {
            String buildNumber = System.getProperty("runtime.java." + version + ".build");
            if (buildNumber != null) {
                System.out.println("buildNumber = " + buildNumber);
                return buildNumber;
            }
            buildNumber = System.getProperty("runtime.java.build");
            if (buildNumber != null) {
                System.out.println("buildNumber2 = " + buildNumber);
                return buildNumber;
            }

            switch (version) {
                case 24:
                    // latest explicitly found build number for 24
                    return "29";
                case 25:
                    return "3";
                default:
                    throw new IllegalArgumentException("Unsupported version " + version);
            }
        }
    }

    private static final Pattern VERSION_PATTERN = Pattern.compile(
        "(\\d+)(\\.\\d+\\.\\d+(?:\\.\\d+)?)?\\+(\\d+(?:\\.\\d+)?)(@([a-f0-9]{32}))?"
    );

    private static final List<OperatingSystem> supportedOperatingSystems = List.of(
        OperatingSystem.MAC_OS,
        OperatingSystem.LINUX,
        OperatingSystem.WINDOWS
    );

    // package private so it can be replaced by tests
    List<JdkBuild> builds = List.of(
        getBundledJdkBuild(),
        // release candidate of JDK 24
        new ReleaseJdkBuild(JavaLanguageVersion.of(24), "download.java.net", "24", "36", "1f9ff9062db4449d8ca828c504ffae90"),
        new EarlyAccessJdkBuild(JavaLanguageVersion.of(25))
    );

    private JdkBuild getBundledJdkBuild() {
        String bundledJdkVersion = VersionProperties.getBundledJdkVersion();
        JavaLanguageVersion bundledJdkMajorVersion = JavaLanguageVersion.of(VersionProperties.getBundledJdkMajorVersion());
        Matcher jdkVersionMatcher = VERSION_PATTERN.matcher(bundledJdkVersion);
        if (jdkVersionMatcher.matches() == false) {
            throw new IllegalStateException("Unable to parse bundled JDK version " + bundledJdkVersion);
        }
        String baseVersion = jdkVersionMatcher.group(1) + (jdkVersionMatcher.group(2) != null ? (jdkVersionMatcher.group(2)) : "");
        String build = jdkVersionMatcher.group(3);
        String hash = jdkVersionMatcher.group(5);
        return new ReleaseJdkBuild(bundledJdkMajorVersion, "download.oracle.com", baseVersion, build, hash);
    }

    /**
     * We need some place to map JavaLanguageVersion to buildNumber, minor version etc.
     * */
    @Override
    public Optional<JavaToolchainDownload> resolve(JavaToolchainRequest request) {
        JdkBuild build = findSupportedBuild(request);
        if (build == null) {
            return Optional.empty();
        }

        OperatingSystem operatingSystem = request.getBuildPlatform().getOperatingSystem();
        String extension = operatingSystem.equals(OperatingSystem.WINDOWS) ? "zip" : "tar.gz";
        String arch = toArchString(request.getBuildPlatform().getArchitecture());
        String os = toOsString(operatingSystem);
        return Optional.of(() -> URI.create(build.url(os, arch, extension)));
    }

    /**
     * Check if request can be full-filled by this resolver:
     * 1. BundledJdkVendor should match openjdk
     * 2. language version should match bundled jdk version
     * 3. vendor must be any or oracle
     * 4. Aarch64 windows images are not supported
     */
    private JdkBuild findSupportedBuild(JavaToolchainRequest request) {
        if (VersionProperties.getBundledJdkVendor().toLowerCase().equals("openjdk") == false) {
            return null;
        }
        JavaToolchainSpec javaToolchainSpec = request.getJavaToolchainSpec();
        if (anyVendorOr(javaToolchainSpec.getVendor().get(), JvmVendorSpec.ORACLE) == false) {
            return null;
        }
        BuildPlatform buildPlatform = request.getBuildPlatform();
        Architecture architecture = buildPlatform.getArchitecture();
        OperatingSystem operatingSystem = buildPlatform.getOperatingSystem();
        if (supportedOperatingSystems.contains(operatingSystem) == false
            || Architecture.AARCH64 == architecture && OperatingSystem.WINDOWS == operatingSystem) {
            return null;
        }

        JavaLanguageVersion languageVersion = javaToolchainSpec.getLanguageVersion().get();
        for (JdkBuild build : builds) {
            if (build.languageVersion().equals(languageVersion)) {
                return build;
            }
        }
        return null;
    }
}
