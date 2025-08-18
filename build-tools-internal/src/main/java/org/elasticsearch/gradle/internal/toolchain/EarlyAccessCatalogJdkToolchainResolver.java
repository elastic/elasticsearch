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
import java.util.List;
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

    interface JdkBuild {
        JavaLanguageVersion languageVersion();

        String url(String os, String arch, String extension);
    }

    @FunctionalInterface
    interface EarlyAccessJdkBuildResolver {
        Optional<EarlyAccessJdkBuild> findLatestEABuild(JavaLanguageVersion languageVersion);
    }

    // allow overriding for testing
    EarlyAccessJdkBuildResolver earlyAccessJdkBuildResolver = (languageVersion) -> findLatestEABuild(languageVersion);

    record EarlyAccessJdkBuild(JavaLanguageVersion languageVersion, int buildNumber) implements JdkBuild {
        @Override
        public String url(String os, String arch, String extension) {
            // example:
            // https://builds.es-jdk-archive.com/jdks/openjdk/26/openjdk-26-ea+6/openjdk-26-ea+6_linux-aarch64_bin.tar.gz
            return "https://builds.es-jdk-archive.com/jdks/openjdk/"
                + languageVersion.asInt()
                + "/"
                + "openjdk-"
                + languageVersion.asInt()
                + "-ea+"
                + buildNumber
                + "/"
                + "openjdk-"
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
            // This resolver only handles early access builds, that are beyond the last bundled jdk version
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
    private Optional<EarlyAccessJdkBuild> findSupportedBuild(JavaToolchainRequest request) {
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

    private static Optional<EarlyAccessJdkBuild> findLatestEABuild(JavaLanguageVersion languageVersion) {
        try {
            URL url = new URL("https://storage.googleapis.com/elasticsearch-jdk-archive/jdks/openjdk/latest.json");
            try (InputStream is = url.openStream()) {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode node = mapper.readTree(is);
                ArrayNode buildsNode = (ArrayNode) node.get("builds");
                List<JsonNode> buildsList = new ArrayList<>();
                buildsNode.forEach(buildsList::add);
                List<EarlyAccessJdkBuild> eaBuilds = buildsList.stream()
                    .map(
                        n -> new EarlyAccessJdkBuild(
                            JavaLanguageVersion.of(n.get("major").asText()),
                            Integer.parseInt(n.get("build").asText())
                        )
                    )
                    .toList();
                return eaBuilds.stream().filter(ea -> ea.languageVersion().equals(languageVersion)).findFirst();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } catch (MalformedURLException e) {
            return Optional.empty();
        }
    }

    public static int findLatestEABuildNumber(int languageVersion) {
        return findLatestEABuild(JavaLanguageVersion.of(languageVersion)).map(ea -> ea.buildNumber()).get();
    }
}
