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
import org.gradle.jvm.toolchain.JvmVendorSpec;
import org.gradle.jvm.toolchain.internal.DefaultJvmVendorSpec;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class EarlyAccessCatalogJdkToolchainResolver extends AbstractCustomJavaToolchainResolver {

    interface JdkBuild {
        JavaLanguageVersion languageVersion();

        String url(String os, String arch, String extension);
    }

    private static final Pattern PATTERN = Pattern.compile("Oracle\\[(\\d+)/(\\d+)\\]");

    public static class EaCatalogVendorSpec extends JvmVendorSpec {

        private final JavaLanguageVersion languageVersion;
        private final int buildNumber;

        private EaCatalogVendorSpec(JavaLanguageVersion languageVersion, int buildNumber) {
            this.languageVersion = languageVersion;
            this.buildNumber = buildNumber;
        }

        public static EaCatalogVendorSpec of(JavaLanguageVersion languageVersion, int buildNumber) {
            return new EaCatalogVendorSpec(languageVersion, buildNumber);
        }

        @Override
        public boolean matches(String vendor) {
            return false;
        }

        public int getBuildNumber() {
            return buildNumber;
        }
    }

    @FunctionalInterface
    interface EarlyAccessJdkBuildResolver {
        Optional<? extends JdkBuild> findLatestEABuild(JavaLanguageVersion languageVersion);
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

    // static EarlyAccessJdkBuild getEarlyAccessBuild(JavaLanguageVersion languageVersion, String buildNumber) {
    // // first try the unversioned override, then the versioned override which has higher precedence
    // buildNumber = System.getProperty("runtime.java.build", buildNumber);
    // buildNumber = System.getProperty("runtime.java." + languageVersion.asInt() + ".build", buildNumber);
    //
    // return new EarlyAccessJdkBuild(languageVersion, buildNumber);
    // }

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
    private Optional<? extends JdkBuild> findSupportedBuild(JavaToolchainRequest request) {
        JavaToolchainSpec javaToolchainSpec = request.getJavaToolchainSpec();
        BuildPlatform buildPlatform = request.getBuildPlatform();
        Architecture architecture = buildPlatform.getArchitecture();
        OperatingSystem operatingSystem = buildPlatform.getOperatingSystem();

        if (supportedOperatingSystems.contains(operatingSystem) == false
            || Architecture.AARCH64 == architecture && OperatingSystem.WINDOWS == operatingSystem) {
            return Optional.empty();
        }

        JavaLanguageVersion languageVersion = javaToolchainSpec.getLanguageVersion().get();
        // resolve from vendor spec if available
        if (javaToolchainSpec.getVendor().isPresent() && javaToolchainSpec.getVendor().get() instanceof DefaultJvmVendorSpec) {
            DefaultJvmVendorSpec spec = (DefaultJvmVendorSpec) javaToolchainSpec.getVendor().get();
            String criteria = spec.toCriteria();
            Matcher matcher = PATTERN.matcher(criteria);
            if (matcher.matches()) {
                int level = Integer.parseInt(matcher.group(1));
                int build = Integer.parseInt(matcher.group(2));
                assert level == languageVersion.asInt() : "Language version does not match: " + level + " != " + languageVersion.asInt();
                return Optional.of(new EarlyAccessJdkBuild(languageVersion, build));
            }
        }
        return findLatestEABuild(languageVersion);
    }

    private static Optional<EarlyAccessJdkBuild> findLatestEABuild(JavaLanguageVersion languageVersion) {
        System.out.println("CatalogJdkToolchainResolver.findLatestEABuild");
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

    public static int findLatestEABuildNumber(JavaLanguageVersion languageVersion) {
        return findLatestEABuild(languageVersion).map(ea -> ea.buildNumber()).get();
    }
}
