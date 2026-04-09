/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.toolchain;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.elasticsearch.gradle.VersionProperties;
import org.gradle.jvm.toolchain.JavaLanguageVersion;
import org.gradle.jvm.toolchain.JavaToolchainDownload;
import org.gradle.jvm.toolchain.JavaToolchainRequest;
import org.gradle.jvm.toolchain.JavaToolchainSpec;
import org.gradle.platform.Architecture;
import org.gradle.platform.BuildPlatform;
import org.gradle.platform.OperatingSystem;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A toolchain resolver that resolves early access JDKs from the Elasticsearch JDK archive.
 * <p>
 * This resolver can used to resolve JDKs that are not bundled with Elasticsearch but are available in the early access catalog.
 * It supports resolving JDKs based on their language version and build number.
 *
 * Currently the gradle toolchain support does not support querying specific versions (e.g. 26-ea+6) so. For now
 * this only supports resolving the latest early access build for a given language version.
 * <p>
 */
public abstract class EarlyAccessCatalogJdkToolchainResolver extends AbstractCustomJavaToolchainResolver {

    public static final String RECENT_JDK_RELEASES_CATALOG_URL = "https://builds.es-jdk-archive.com/jdks/openjdk/recent.json";

    interface JdkBuild {
        JavaLanguageVersion languageVersion();

        String url(String os, String arch, String extension);
    }

    @FunctionalInterface
    interface EarlyAccessJdkBuildResolver {
        Optional<PreReleaseJdkBuild> findLatestEABuild(JavaLanguageVersion languageVersion);
    }

    // allow overriding for testing
    EarlyAccessJdkBuildResolver earlyAccessJdkBuildResolver = (languageVersion) -> findLatestPreReleaseBuild(languageVersion);

    public record PreReleaseJdkBuild(JavaLanguageVersion languageVersion, int buildNumber, String type) implements JdkBuild {

        private static final Map<String, Integer> TYPE_RANK = Map.of("ea", 0, "rc", 1, "ga", 2);

        int typeRank() {
            return TYPE_RANK.getOrDefault(type, -1);
        }

        @Override
        public String url(String os, String arch, String extension) {
            // examples:
            // https://builds.es-jdk-archive.com/jdks/openjdk/26/openjdk-26-ea+6/openjdk-26-ea+6_linux-aarch64_bin.tar.gz
            // https://builds.es-jdk-archive.com/jdks/openjdk/26/openjdk-26-rc+30/openjdk-26_linux-x64_bin.tar.gz
            // https://builds.es-jdk-archive.com/jdks/openjdk/26/openjdk-26/openjdk-26_linux-x64_bin.tar.gz

            // EA builds include the type+build in both folder and artifact name
            // RC builds include type+build in folder only
            // GA builds use just the version for both folder and artifact
            String folderSuffix = type.equals("ga") ? "" : "-" + type + "+" + buildNumber;
            String artifactSuffix = type.equals("ea") ? "-" + type + "+" + buildNumber : "";
            return "https://builds.es-jdk-archive.com/jdks/openjdk/"
                + languageVersion.asInt()
                + "/"
                + "openjdk-"
                + languageVersion.asInt()
                + folderSuffix
                + "/"
                + "openjdk-"
                + languageVersion.asInt()
                + artifactSuffix
                + "_"
                + os
                + "-"
                + arch
                + "_bin."
                + extension;
        }
    }

    private static final List<OperatingSystem> supportedOperatingSystems = List.of(
        OperatingSystem.MAC_OS,
        OperatingSystem.LINUX,
        OperatingSystem.WINDOWS
    );

    /**
     * We need some place to map JavaLanguageVersion to buildNumber, minor version etc.
     * */
    @Override
    public Optional<JavaToolchainDownload> resolve(JavaToolchainRequest request) {
        if (Integer.parseInt(VersionProperties.getBundledJdkMajorVersion()) >= request.getJavaToolchainSpec()
            .getLanguageVersion()
            .get()
            .asInt()) {
        }
        return findSupportedBuild(request).map(build -> {
            OperatingSystem operatingSystem = request.getBuildPlatform().getOperatingSystem();
            String extension = operatingSystem.equals(OperatingSystem.WINDOWS) ? "zip" : "tar.gz";
            String arch = toArchString(request.getBuildPlatform().getArchitecture());
            String os = toOsString(operatingSystem);
            return (JavaToolchainDownload) () -> URI.create(build.url(os, arch, extension));
        });
    }

    /**
     * Check if request can be full-filled by this resolver:
     * 1. Aarch64 windows images are not supported
     */
    private Optional<PreReleaseJdkBuild> findSupportedBuild(JavaToolchainRequest request) {
        JavaToolchainSpec javaToolchainSpec = request.getJavaToolchainSpec();
        BuildPlatform buildPlatform = request.getBuildPlatform();
        Architecture architecture = buildPlatform.getArchitecture();
        OperatingSystem operatingSystem = buildPlatform.getOperatingSystem();

        if (supportedOperatingSystems.contains(operatingSystem) == false
            || Architecture.AARCH64 == architecture && OperatingSystem.WINDOWS == operatingSystem) {
            return Optional.empty();
        }

        JavaLanguageVersion languageVersion = javaToolchainSpec.getLanguageVersion().get();
        return earlyAccessJdkBuildResolver.findLatestEABuild(languageVersion);
    }

    static List<PreReleaseJdkBuild> findRecentPreReleaseBuild(JavaLanguageVersion languageVersion) {
        try {
            URL url = new URL(RECENT_JDK_RELEASES_CATALOG_URL);
            try (InputStream is = url.openStream()) {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode node = mapper.readTree(is);
                ObjectNode majors = (ObjectNode) node.get("majors");
                JsonNode perVersion = majors.get("" + languageVersion.asInt());
                if (perVersion == null) {
                    return List.of();
                }
                ArrayNode buildsNode = (ArrayNode) perVersion.get("builds");
                List<JsonNode> buildsList = new ArrayList<>();
                buildsNode.forEach(buildsList::add);
                List<PreReleaseJdkBuild> eaBuilds = buildsList.stream()
                    .map(
                        n -> new PreReleaseJdkBuild(
                            JavaLanguageVersion.of(n.get("major").asText()),
                            Integer.parseInt(n.get("build").asText()),
                            n.get("type").asText()
                        )
                    )
                    .toList();
                return eaBuilds.stream().filter(ea -> ea.languageVersion().equals(languageVersion)).toList();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    private static final Comparator<PreReleaseJdkBuild> BUILD_COMPARATOR = Comparator.comparingInt(PreReleaseJdkBuild::buildNumber)
        .thenComparingInt(PreReleaseJdkBuild::typeRank);

    public static Optional<PreReleaseJdkBuild> findPreReleaseBuild(JavaLanguageVersion languageVersion, int buildNumber) {
        return findRecentPreReleaseBuild(languageVersion).stream()
            .filter(preReleaseJdkBuild -> preReleaseJdkBuild.buildNumber == buildNumber)
            .max(BUILD_COMPARATOR);
    }

    public static Optional<PreReleaseJdkBuild> findLatestPreReleaseBuild(JavaLanguageVersion languageVersion) {
        return findRecentPreReleaseBuild(languageVersion).stream().max(BUILD_COMPARATOR);
    }

}
