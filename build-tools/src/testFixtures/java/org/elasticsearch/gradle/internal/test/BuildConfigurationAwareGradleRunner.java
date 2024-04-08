/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test;

import org.gradle.testkit.runner.BuildResult;
import org.gradle.testkit.runner.GradleRunner;
import org.gradle.testkit.runner.InvalidPluginMetadataException;
import org.gradle.testkit.runner.InvalidRunnerConfigurationException;
import org.gradle.testkit.runner.UnexpectedBuildFailure;
import org.gradle.testkit.runner.UnexpectedBuildSuccess;

import java.io.File;
import java.io.Writer;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A Gradle runner that delegates to another runner, optionally configuring
 * - the configuring cache parameter
 * - the internal restricted build api service
 */
public class BuildConfigurationAwareGradleRunner extends GradleRunner {
    private final GradleRunner delegate;
    private final boolean ccCompatible;
    private final boolean buildApiRestrictionsDisabled;

    public BuildConfigurationAwareGradleRunner(GradleRunner delegate, boolean ccCompatible, boolean buildApiRestrictionsDisabled) {
        this.delegate = delegate;
        this.ccCompatible = ccCompatible;
        this.buildApiRestrictionsDisabled = buildApiRestrictionsDisabled;
    }

    @Override
    public GradleRunner withGradleVersion(String gradleVersion) {
        delegate.withGradleVersion(gradleVersion);
        return this;
    }

    @Override
    public GradleRunner withGradleInstallation(File file) {
        delegate.withGradleInstallation(file);
        return this;
    }

    @Override
    public GradleRunner withGradleDistribution(URI uri) {
        delegate.withGradleDistribution(uri);
        return this;
    }

    @Override
    public GradleRunner withTestKitDir(File file) {
        delegate.withTestKitDir(file);
        return this;
    }

    @Override
    public File getProjectDir() {
        return delegate.getProjectDir();
    }

    @Override
    public GradleRunner withProjectDir(File projectDir) {
        delegate.withProjectDir(projectDir);
        return this;
    }

    @Override
    public List<String> getArguments() {
        return delegate.getArguments();
    }

    @Override
    public GradleRunner withArguments(List<String> arguments) {
        List<String> effectiveArgs = new ArrayList<>(arguments);
        if (ccCompatible) {
            effectiveArgs.add("--configuration-cache");
        }
        if (buildApiRestrictionsDisabled) {
            effectiveArgs.add("-Dorg.elasticsearch.gradle.build-api-restriction.disabled=" + buildApiRestrictionsDisabled);
        }
        delegate.withArguments(effectiveArgs);
        return this;
    }

    @Override
    public GradleRunner withArguments(String... arguments) {
        withArguments(List.of(arguments));
        return this;
    }

    @Override
    public List<? extends File> getPluginClasspath() {
        return delegate.getPluginClasspath();
    }

    @Override
    public GradleRunner withPluginClasspath() throws InvalidPluginMetadataException {
        delegate.withPluginClasspath();
        return this;
    }

    @Override
    public GradleRunner withPluginClasspath(Iterable<? extends File> iterable) {
        delegate.withPluginClasspath(iterable);
        return this;
    }

    @Override
    public boolean isDebug() {
        return delegate.isDebug();
    }

    @Override
    public GradleRunner withDebug(boolean b) {
        delegate.withDebug(b);
        return this;
    }

    @Override
    public Map<String, String> getEnvironment() {
        return delegate.getEnvironment();
    }

    @Override
    public GradleRunner withEnvironment(Map<String, String> map) {
        delegate.withEnvironment(map);
        return this;
    }

    @Override
    public GradleRunner forwardStdOutput(Writer writer) {
        delegate.forwardStdOutput(writer);
        return this;
    }

    @Override
    public GradleRunner forwardStdError(Writer writer) {
        delegate.forwardStdOutput(writer);
        return this;
    }

    @Override
    public GradleRunner forwardOutput() {
        delegate.forwardOutput();
        return this;
    }

    @Override
    public BuildResult build() throws InvalidRunnerConfigurationException, UnexpectedBuildFailure {
        return delegate.build();
    }

    @Override
    public BuildResult buildAndFail() throws InvalidRunnerConfigurationException, UnexpectedBuildSuccess {
        return delegate.buildAndFail();
    }

    @Override
    public BuildResult run() throws InvalidRunnerConfigurationException {
        return delegate.run();
    }
}
