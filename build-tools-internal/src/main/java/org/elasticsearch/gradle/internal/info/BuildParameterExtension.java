/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.info;

import org.elasticsearch.gradle.internal.BwcVersions;
import org.gradle.api.Action;
import org.gradle.api.JavaVersion;
import org.gradle.api.Task;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.jvm.toolchain.JavaToolchainSpec;

import java.io.File;
import java.io.Serializable;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

public class BuildParameterExtension implements Serializable {
    private final boolean inFipsJvm;
    private final File runtimeJavaHome;
    private final Boolean isRuntimeJavaHomeSet;
    private final List<JavaHome> javaVersions;
    private final JavaVersion minimumCompilerVersion;
    private final JavaVersion minimumRuntimeVersion;
    private final JavaVersion gradleJavaVersion;
    private final JavaVersion runtimeJavaVersion;
    private final Action<JavaToolchainSpec> javaToolChainSpec;
    private final String runtimeJavaDetails;
    private final String gitRevision;
    private final String gitOrigin;
    private transient AtomicReference<ZonedDateTime> buildDate = new AtomicReference<>();
    private final String testSeed;
    private final Boolean isCi;
    private final Integer defaultParallel;
    private final Boolean isSnapshotBuild;
    private final BwcVersions bwcVersions;

    public BuildParameterExtension(
        ProviderFactory providers,
        File runtimeJavaHome,
        Action<JavaToolchainSpec> javaToolChainSpec,
        JavaVersion runtimeJavaVersion,
        boolean isRuntimeJavaHomeSet,
        String runtimeJavaDetails,
        List<JavaHome> javaVersions,
        JavaVersion minimumCompilerVersion,
        JavaVersion minimumRuntimeVersion,
        JavaVersion gradleJavaVersion,
        String gitRevision,
        String gitOrigin,
        ZonedDateTime buildDate,
        String testSeed,
        boolean isCi,
        int defaultParallel,
        final boolean isSnapshotBuild,
        BwcVersions bwcVersions
    ) {
        this.inFipsJvm = providers.systemProperty("tests.fips.enabled").map(BuildParameterExtension::parseBoolean).getOrElse(false);
        this.runtimeJavaHome = runtimeJavaHome;
        this.javaToolChainSpec = javaToolChainSpec;
        this.runtimeJavaVersion = runtimeJavaVersion;
        this.isRuntimeJavaHomeSet = isRuntimeJavaHomeSet;
        this.runtimeJavaDetails = runtimeJavaDetails;
        this.javaVersions = javaVersions;
        this.minimumCompilerVersion = minimumCompilerVersion;
        this.minimumRuntimeVersion = minimumRuntimeVersion;
        this.gradleJavaVersion = gradleJavaVersion;
        this.gitRevision = gitRevision;
        this.gitOrigin = gitOrigin;
        this.testSeed = testSeed;
        this.isCi = isCi;
        this.defaultParallel = defaultParallel;
        this.isSnapshotBuild = isSnapshotBuild;
        this.bwcVersions = bwcVersions;
    }

    private static boolean parseBoolean(String s) {
        if (s == null) {
            return false;
        }
        return Boolean.parseBoolean(s);
    }

    public boolean getInFipsJvm() {
        return inFipsJvm;
    }

    public File getRuntimeJavaHome() {
        return runtimeJavaHome;
    }

    public void withFipsEnabledOnly(Task task) {
        task.onlyIf("FIPS mode disabled", task1 -> getInFipsJvm() == false);
    }

    public Boolean getIsRuntimeJavaHomeSet() {
        return isRuntimeJavaHomeSet;
    }

    public List<JavaHome> getJavaVersions() {
        return javaVersions;
    }

    public JavaVersion getMinimumCompilerVersion() {
        return minimumCompilerVersion;
    }

    public JavaVersion getMinimumRuntimeVersion() {
        return minimumRuntimeVersion;
    }

    public JavaVersion getGradleJavaVersion() {
        return gradleJavaVersion;
    }

    public JavaVersion getRuntimeJavaVersion() {
        return runtimeJavaVersion;
    }

    public Action<JavaToolchainSpec> getJavaToolChainSpec() {
        return javaToolChainSpec;
    }

    public String getRuntimeJavaDetails() {
        return runtimeJavaDetails;
    }

    public String getGitRevision() {
        return gitRevision;
    }

    public String getGitOrigin() {
        return gitOrigin;
    }

    public ZonedDateTime getBuildDate() {
        ZonedDateTime value = buildDate.get();
        if (value == null) {
            value = ZonedDateTime.now(ZoneOffset.UTC);
            if (buildDate.compareAndSet(null, value) == false) {
                // If another thread initialized it first, return the initialized value
                value = buildDate.get();
            }
        }
        return value;
    }

    public String getTestSeed() {
        return testSeed;
    }

    public Boolean isCi() {
        return isCi;
    }

    public Integer getDefaultParallel() {
        return defaultParallel;
    }

    public Boolean isSnapshotBuild() {
        return isSnapshotBuild;
    }

    public BwcVersions getBwcVersions() {
        return bwcVersions;
    }

    public Random getRandom() {
        return new Random(Long.parseUnsignedLong(testSeed.split(":")[0], 16));
    }

    public Boolean isGraalVmRuntime() {
        return runtimeJavaDetails.toLowerCase().contains("graalvm");
    }
}
