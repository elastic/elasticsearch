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
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.Writer;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InternalAwareGradleRunner extends GradleRunner {
    private GradleRunner delegate;

    public InternalAwareGradleRunner(GradleRunner delegate) {
        this.delegate = delegate;
    }

    @Override
    public GradleRunner withGradleVersion(String gradleVersion) {
        return delegate.withGradleVersion(gradleVersion);
    }

    @Override
    public GradleRunner withGradleInstallation(File file) {
        return delegate.withGradleInstallation(file);
    }

    @Override
    public GradleRunner withGradleDistribution(URI uri) {
        return delegate.withGradleDistribution(uri);
    }

    @Override
    public GradleRunner withTestKitDir(File file) {
        return delegate.withTestKitDir(file);
    }

    @Override
    public File getProjectDir() {
        return delegate.getProjectDir();
    }

    @Override
    public GradleRunner withProjectDir(File projectDir) {
        return delegate.withProjectDir(projectDir);
    }

    @Override
    public List<String> getArguments() {
        return delegate.getArguments();
    }

    @Override
    public GradleRunner withArguments(List<String> arguments) {
        List<String> collect = Stream.concat(arguments.stream(), Stream.of("-Dtest.external=true"))
                .collect(Collectors.toList());
        return delegate.withArguments(collect);
    }

    @Override
    public GradleRunner withArguments(String... arguments) {
        return withArguments(List.of(arguments));
    }

    @Override
    public List<? extends File> getPluginClasspath() {
        return delegate.getPluginClasspath();
    }

    @Override
    public GradleRunner withPluginClasspath() throws InvalidPluginMetadataException {
        return delegate.withPluginClasspath();
    }

    @Override
    public GradleRunner withPluginClasspath(Iterable<? extends File> iterable) {
        return delegate.withPluginClasspath(iterable);
    }

    @Override
    public boolean isDebug() {
        return delegate.isDebug();
    }

    @Override
    public GradleRunner withDebug(boolean b) {
        return delegate.withDebug(b);
    }

    @Nullable
    @Override
    public Map<String, String> getEnvironment() {
        return delegate.getEnvironment();
    }

    @Override
    public GradleRunner withEnvironment(@Nullable Map<String, String> map) {
        return delegate.withEnvironment(map);
    }

    @Override
    public GradleRunner forwardStdOutput(Writer writer) {
        return delegate.forwardStdOutput(writer);
    }

    @Override
    public GradleRunner forwardStdError(Writer writer) {
        return delegate.forwardStdOutput(writer);
    }

    @Override
    public GradleRunner forwardOutput() {
        return delegate.forwardOutput();
    }

    @Override
    public BuildResult build() throws InvalidRunnerConfigurationException, UnexpectedBuildFailure {
        return delegate.build();
    }

    @Override
    public BuildResult buildAndFail() throws InvalidRunnerConfigurationException, UnexpectedBuildSuccess {
        return delegate.buildAndFail();
    }
}
