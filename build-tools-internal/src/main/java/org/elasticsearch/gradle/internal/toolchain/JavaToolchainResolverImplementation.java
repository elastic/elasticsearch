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

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class JavaToolchainResolverImplementation implements JavaToolchainResolver {

    public static final Pattern VERSION_PATTERN = Pattern.compile(
        "(\\d+)(\\.\\d+\\.\\d+(?:\\.\\d+)?)?\\+(\\d+(?:\\.\\d+)?)(@([a-f0-9]{32}))?"
    );
    private final Map<String, String> javaVersionProperties;

    public JavaToolchainResolverImplementation() {
        this.javaVersionProperties = resolveSupportedLanguageVersion();
    }

    @NotNull
    private static Map<String, String> resolveSupportedLanguageVersion() {
        Properties props = new Properties();
        InputStream propsStream = VersionProperties.class.getResourceAsStream("/java.properties");
        if (propsStream == null) {
            throw new IllegalStateException("/java.properties resource missing");
        }
        try {
            props.load(propsStream);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load version properties", e);
        }
        Map map = props;
        Map<String, String> stringStringHashMap = map;

        return stringStringHashMap;
    }

    /**
     * We need some place to map JavaLanguageVersion to build, minor version etc.
     * */
    @Override
    public Optional<JavaToolchainDownload> resolve(JavaToolchainRequest request) {
        if (isRequestSupported(request) == false) {
            return Optional.empty();
        }
        String jdkPropertyName = "jdk_" + request.getJavaToolchainSpec().getLanguageVersion().get().asInt();
        String jdkVersion = javaVersionProperties.get(jdkPropertyName);
        Matcher jdkVersionMatcher = VERSION_PATTERN.matcher(jdkVersion);
        jdkVersionMatcher.matches();
        String major = jdkVersionMatcher.group(1);
        String baseVersion = major + (jdkVersionMatcher.group(2) != null ? (jdkVersionMatcher.group(2)) : "");
        String build = jdkVersionMatcher.group(3);
        String hash = jdkVersionMatcher.group(5);

        JavaToolchainSpec javaToolchainSpec = request.getJavaToolchainSpec();
        JvmVendorSpec jvmVendorSpec = javaToolchainSpec.getVendor().get();

        if (jvmVendorSpec.matches("any")
            || jvmVendorSpec.equals(JvmVendorSpec.ORACLE) && javaVersionProperties.get(jdkPropertyName + "_vendor").equals("openjdk")) {
            return calculateOracleOpenJdkURI(request, major, baseVersion, build, hash);
        } else if (jvmVendorSpec.equals(JvmVendorSpec.ADOPTIUM)
            && javaVersionProperties.get(jdkPropertyName + "_vendor").equals("adoptium")) {
                return calculateAdoptiumJdkURI(request, major, build);
            }
        System.out.println("jvmVendorSpec.equals(JvmVendorSpec.ADOPTIUM) = " + jvmVendorSpec.equals(JvmVendorSpec.ADOPTIUM));

        System.out.println("Optional.empty()");
        return Optional.empty();
    }

    private Optional<JavaToolchainDownload> calculateAdoptiumJdkURI(JavaToolchainRequest request, String baseVersion, String build) {
        String arch = toArchString(request.getBuildPlatform().getArchitecture());
        String os = toOsString(request.getBuildPlatform().getOperatingSystem());
        String versionSpecificUrl = baseVersion.equals("8") ? "-" : "+";
        String urlString = "https://api.adoptium.net/v3/binary/version/jdk"
            + baseVersion
            + versionSpecificUrl
            + build
            + "/"
            + os
            + "/"
            + arch
            + "/jdk/hotspot/normal/adoptium";
        return Optional.of(() -> URI.create(urlString));
    }

    @NotNull
    private Optional<JavaToolchainDownload> calculateOracleOpenJdkURI(
        JavaToolchainRequest request,
        String major,
        String baseVersion,
        String build,
        String hash
    ) {
        OperatingSystem operatingSystem = request.getBuildPlatform().getOperatingSystem();
        String extension = operatingSystem.equals(OperatingSystem.WINDOWS) ? "zip" : "tar.gz";
        String arch = toArchString(request.getBuildPlatform().getArchitecture());
        String os = toOsString(operatingSystem);
        return Optional.of(
            () -> URI.create(
                "https://download.oracle.com/java/GA/jdk"
                    + major
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
        JavaLanguageVersion javaLanguageVersion = request.getJavaToolchainSpec().getLanguageVersion().get();
        String resolvedSupportedJavaVersion = javaVersionProperties.get("jdk_" + javaLanguageVersion.asInt());
        String resolvedSupportedJavaVersionVendor = javaVersionProperties.get("jdk_" + javaLanguageVersion.asInt() + "_vendor");
        return resolvedSupportedJavaVersion != null
            && isRequestedVendorSupported(resolvedSupportedJavaVersionVendor, request.getJavaToolchainSpec().getVendor());
    }

    private static boolean isRequestedVendorSupported(Object resolvedSupportedVendor, Property<JvmVendorSpec> jvmVendorSpecProperty) {
        JvmVendorSpec jvmVendorSpec = jvmVendorSpecProperty.get();
        return jvmVendorSpec.matches("any")
            || (jvmVendorSpec.equals(JvmVendorSpec.ORACLE) && "openjdk".equals(resolvedSupportedVendor))
            || (jvmVendorSpec.equals(JvmVendorSpec.ADOPTIUM) && "adoptium".equals(resolvedSupportedVendor));
    }
}
