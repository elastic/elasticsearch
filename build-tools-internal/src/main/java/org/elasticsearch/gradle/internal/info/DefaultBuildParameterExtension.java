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
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.jvm.toolchain.JavaToolchainSpec;

import java.io.File;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public abstract class DefaultBuildParameterExtension implements BuildParameterExtension {
    private final Provider<Boolean> inFipsJvm;
    private final Provider<File> runtimeJavaHome;
    private final Boolean isRuntimeJavaHomeSet;
    private final List<JavaHome> javaVersions;
    private final JavaVersion minimumCompilerVersion;
    private final JavaVersion minimumRuntimeVersion;
    private final JavaVersion gradleJavaVersion;
    private final Provider<JavaVersion> runtimeJavaVersion;
    private final Provider<? extends Action<JavaToolchainSpec>> javaToolChainSpec;
    private final Provider<String> runtimeJavaDetails;
    private final Provider<String> gitRevision;

    private transient AtomicReference<ZonedDateTime> buildDate = new AtomicReference<>();
    private final String testSeed;
    private final Boolean isCi;
    private final Integer defaultParallel;
    private final Boolean snapshotBuild;

    // not final for testing
    private Provider<BwcVersions> bwcVersions;
    private Provider<String> gitOrigin;

    public DefaultBuildParameterExtension(
        ProviderFactory providers,
        Provider<File> runtimeJavaHome,
        Provider<? extends Action<JavaToolchainSpec>> javaToolChainSpec,
        Provider<JavaVersion> runtimeJavaVersion,
        boolean isRuntimeJavaHomeSet,
        Provider<String> runtimeJavaDetails,
        List<JavaHome> javaVersions,
        JavaVersion minimumCompilerVersion,
        JavaVersion minimumRuntimeVersion,
        JavaVersion gradleJavaVersion,
        Provider<String> gitRevision,
        Provider<String> gitOrigin,
        String testSeed,
        boolean isCi,
        int defaultParallel,
        final boolean isSnapshotBuild,
        Provider<BwcVersions> bwcVersions
    ) {
        this.inFipsJvm = providers.systemProperty("tests.fips.enabled").map(DefaultBuildParameterExtension::parseBoolean);
        this.runtimeJavaHome = cache(providers, runtimeJavaHome);
        this.javaToolChainSpec = cache(providers, javaToolChainSpec);
        this.runtimeJavaVersion = cache(providers, runtimeJavaVersion);
        this.isRuntimeJavaHomeSet = isRuntimeJavaHomeSet;
        this.runtimeJavaDetails = cache(providers, runtimeJavaDetails);
        this.javaVersions = javaVersions;
        this.minimumCompilerVersion = minimumCompilerVersion;
        this.minimumRuntimeVersion = minimumRuntimeVersion;
        this.gradleJavaVersion = gradleJavaVersion;
        this.gitRevision = gitRevision;
        this.testSeed = testSeed;
        this.isCi = isCi;
        this.defaultParallel = defaultParallel;
        this.snapshotBuild = isSnapshotBuild;
        this.bwcVersions = cache(providers, bwcVersions);
        this.gitOrigin = gitOrigin;
    }

    // This is a workaround for https://github.com/gradle/gradle/issues/25550
    private <T> Provider<T> cache(ProviderFactory providerFactory, Provider<T> incomingProvider) {
        SingleObjectCache<T> cache = new SingleObjectCache<>();
        return providerFactory.provider(() -> cache.computeIfAbsent(() -> incomingProvider.getOrNull()));
    }

    private static boolean parseBoolean(String s) {
        if (s == null) {
            return false;
        }
        return Boolean.parseBoolean(s);
    }

    @Override
    public boolean getInFipsJvm() {
        return inFipsJvm.getOrElse(false);
    }

    @Override
    public Provider<File> getRuntimeJavaHome() {
        return runtimeJavaHome;
    }

    @Override
    public void withFipsEnabledOnly(Task task) {
        task.onlyIf("FIPS mode disabled", task1 -> getInFipsJvm() == false);
    }

    @Override
    public Boolean getIsRuntimeJavaHomeSet() {
        return isRuntimeJavaHomeSet;
    }

    @Override
    public List<JavaHome> getJavaVersions() {
        return javaVersions;
    }

    @Override
    public JavaVersion getMinimumCompilerVersion() {
        return minimumCompilerVersion;
    }

    @Override
    public JavaVersion getMinimumRuntimeVersion() {
        return minimumRuntimeVersion;
    }

    @Override
    public JavaVersion getGradleJavaVersion() {
        return gradleJavaVersion;
    }

    @Override
    public Provider<JavaVersion> getRuntimeJavaVersion() {
        return runtimeJavaVersion;
    }

    @Override
    public Provider<? extends Action<JavaToolchainSpec>> getJavaToolChainSpec() {
        return javaToolChainSpec;
    }

    @Override
    public Provider<String> getRuntimeJavaDetails() {
        return runtimeJavaDetails;
    }

    @Override
    public Provider<String> getGitRevision() {
        return gitRevision;
    }

    @Override
    public Provider<String> getGitOrigin() {
        return gitOrigin;
    }

    @Override
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

    @Override
    public String getTestSeed() {
        return testSeed;
    }

    @Override
    public Boolean getCi() {
        return isCi;
    }

    @Override
    public Integer getDefaultParallel() {
        return defaultParallel;
    }

    @Override
    public Boolean getSnapshotBuild() {
        return snapshotBuild;
    }

    @Override
    public BwcVersions getBwcVersions() {
        return bwcVersions.get();
    }

    @Override
    public Random getRandom() {
        return new Random(Long.parseUnsignedLong(testSeed.split(":")[0], 16));
    }

    @Override
    public Boolean getGraalVmRuntime() {
        return runtimeJavaDetails.get().toLowerCase().contains("graalvm");
    }

    private static class SingleObjectCache<T> {
        private T instance;

        public T computeIfAbsent(Supplier<T> supplier) {
            synchronized (this) {
                if (instance == null) {
                    instance = supplier.get();
                }
                return instance;
            }
        }

        public T get() {
            return instance;
        }
    }

    public Provider<BwcVersions> getBwcVersionsProvider() {
        return bwcVersions;
    }

    // for testing; not part of public api
    public void setBwcVersions(Provider<BwcVersions> bwcVersions) {
        this.bwcVersions = bwcVersions;
    }

    // for testing; not part of public api
    public void setGitOrigin(Provider<String> gitOrigin) {
        this.gitOrigin = gitOrigin;
    }
}
