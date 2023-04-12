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
import org.gradle.internal.jvm.Jvm;
import org.gradle.jvm.toolchain.JavaLanguageVersion;
import org.gradle.jvm.toolchain.JavaToolchainDownload;
import org.gradle.jvm.toolchain.JavaToolchainRequest;
import org.gradle.jvm.toolchain.JavaToolchainResolver;
import org.gradle.jvm.toolchain.JavaToolchainSpec;
import org.gradle.jvm.toolchain.JvmVendorSpec;
import org.gradle.platform.Architecture;
import org.gradle.platform.OperatingSystem;

import java.net.URI;
import java.util.Optional;

public abstract class JavaToolchainResolverImplementation implements JavaToolchainResolver {

    /**
     * We need some place to map JavaLanguageVersion to build, minor version etc.
     * */
    @Override
    public Optional<JavaToolchainDownload> resolve(JavaToolchainRequest request) {
        String bundledJdkVendor = VersionProperties.getBundledJdkVendor();
        String bundledJdkVersion = VersionProperties.getBundledJdkVersion();
        System.out.println("bundledJdkVersion = " + bundledJdkVersion);
        mapBundledJdkVendorToVendorSpec(bundledJdkVendor);
        mapBundledJdkVersionToMajorVersion(bundledJdkVendor);

        JavaToolchainSpec javaToolchainSpec = request.getJavaToolchainSpec();
        Property<JavaLanguageVersion> languageVersion = javaToolchainSpec.getLanguageVersion();

        System.out.println("languageVersion.get() = " + languageVersion.get());
        Architecture architecture = request.getBuildPlatform().getArchitecture();
        OperatingSystem operatingSystem = request.getBuildPlatform().getOperatingSystem();
        System.out.println("operatingSystem = " + operatingSystem);
        System.out.println("architecture = " + architecture);

        JavaLanguageVersion majorVersion = javaToolchainSpec.getLanguageVersion().getOrNull();
        JvmVendorSpec jvmVendorSpec = javaToolchainSpec.getVendor().get();
        System.out.println("jvmVendorSpec = " + jvmVendorSpec);

        String repoUrl = null;
        String artifactPattern = null;
        String os = "linux";
        String arch = "x64";

        // if (jvmVendorSpec == null || jvmVendorSpec.equals(JvmVendorSpec.ADOPTIUM)) {
        // repoUrl = "https://api.adoptium.net/v3/binary/version/";
        //
        // if (majorVersion != null && majorVersion == JavaLanguageVersion.of(8)) {
        // // legacy pattern for JDK 8
        // artifactPattern = "jdk"
        // + "BASE_VERSION" // jdk.getBaseVersion()"
        // + "-"
        // + "BUILD" // jdk.getBuild()
        // + "/[module]/[classifier]/jdk/hotspot/normal/adoptium";
        // } else {
        // // current pattern since JDK 9
        // // jdk-12.0.2+10/linux/x64/jdk/hotspot/normal/adoptium
        //
        // artifactPattern = "jdk-"
        // + "BASE_VERSION" // jdk.getBaseVersion()
        // + "+"
        // + "BUILD" // jdk.getBuild()
        // + "/" + os + "/" + arch + "/[classifier]/jdk/hotspot/normal/adoptium";
        //// + "/[module]/[classifier]/jdk/hotspot/normal/adoptium";
        // }
        // }
        if (jvmVendorSpec == null || jvmVendorSpec.equals(JvmVendorSpec.ORACLE)) {
            repoUrl = "https://download.oracle.com";
            if ("jdk.getHash()" != null) {
                // current pattern since 12.0.1
                artifactPattern = "java/GA/jdk"
                    + "jdk.getBaseVersion()"
                    + "/"
                    + "jdk.getHash()"
                    + "/"
                    + "jdk.getBuild()"
                    + "/GPL/openjdk-[revision]_[module]-[classifier]_bin.[ext]";
            } else {
                // simpler legacy pattern from JDK 9 to JDK 12 that we are advocating to Oracle to bring back
                artifactPattern = "java/GA/jdk"
                    + "jdk.getMajor()"
                    + "/"
                    + "jdk.getBuild()"
                    + "/GPL/openjdk-[revision]_[module]-[classifier]_bin.[ext]";
            }
        }
        System.out.println("repoUrl = " + repoUrl + artifactPattern);
        String finalRepoUrl = repoUrl;
        // JavaToolchainDownload download = (JavaToolchainDownload) ;
        String finalArtifactPattern = artifactPattern;
        return Optional.of(() -> URI.create(finalRepoUrl + finalArtifactPattern));
    }

    private void mapBundledJdkVersionToMajorVersion(String bundledJdkVersion) {

    }

    private JvmVendorSpec mapBundledJdkVendorToVendorSpec(String bundledJdkVendor) {
        return bundledJdkVendor.equals("openjdk") ? JvmVendorSpec.ORACLE : JvmVendorSpec.matching(bundledJdkVendor);
    }
}
