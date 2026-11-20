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
import org.gradle.jvm.toolchain.JavaToolchainSpec;

import java.io.File;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Random;

public interface BuildParameterExtension {
    String EXTENSION_NAME = "buildParams";

    boolean getInFipsJvm();

    Provider<File> getRuntimeJavaHome();

    void withFipsEnabledOnly(Task task);

    Boolean getIsRuntimeJavaHomeSet();

    RuntimeJava getRuntimeJava();

    List<JavaHome> getJavaVersions();

    JavaVersion getMinimumCompilerVersion();

    JavaVersion getMinimumRuntimeVersion();

    JavaVersion getGradleJavaVersion();

    Provider<JavaVersion> getRuntimeJavaVersion();

    Provider<? extends Action<JavaToolchainSpec>> getJavaToolChainSpec();

    Provider<String> getRuntimeJavaDetails();

    Provider<String> getGitRevision();

    Provider<String> getGitOrigin();

    ZonedDateTime getBuildDate();

    /**
     * The random seed for this build's tests. There is deliberately no eager {@code String} getter: the seed is new on
     * every build, so reading it while configuring (for example {@code buildParams.testSeed} in a build script) would make
     * the configuration cache discard its entry on every run. Map the provider instead and resolve it at execution time.
     */
    Provider<String> getTestSeedProvider();

    Boolean getCi();

    Integer getDefaultParallel();

    Boolean getSnapshotBuild();

    BwcVersions getBwcVersions();

    Provider<BwcVersions> getBwcVersionsProvider();

    Provider<Random> getRandom();

    Boolean getGraalVmRuntime();

    String getFipsMode();
}
