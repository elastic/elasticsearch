/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.gradle.internal.test.TestUtils.normalizeString;

public class NormalizeOutputGradleRunner extends GradleRunner {

    private GradleRunner delegate;

    public NormalizeOutputGradleRunner(GradleRunner delegate) {
        this.delegate = delegate;
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
        delegate.withArguments(arguments);
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
        delegate.forwardStdError(writer);
        return this;
    }

    @Override
    public GradleRunner forwardOutput() {
        delegate.forwardOutput();
        return this;
    }

    @Override
    public BuildResult build() throws InvalidRunnerConfigurationException, UnexpectedBuildFailure {
        return normalizedBuildResult(delegate.build());
    }

    @Override
    public BuildResult buildAndFail() throws InvalidRunnerConfigurationException, UnexpectedBuildSuccess {
        return normalizedBuildResult(delegate.buildAndFail());
    }

    @Override
    public BuildResult run() throws InvalidRunnerConfigurationException {
        return normalizedBuildResult(delegate.run());
    }

    /**
     * Decorates a {@link BuildResult} so that {@link BuildResult#getOutput()} returns normalized output.
     * <p>
     * A {@link Proxy} is used instead of a class implementing {@link BuildResult} on purpose: Gradle
     * occasionally adds new abstract methods to the {@code BuildResult} interface (e.g. {@code getOutputReader()}
     * in Gradle 9.1 and {@code getConfigurationCacheOutcome()} in Gradle 9.8), which breaks compilation of any
     * static implementation on wrapper upgrades. The proxy implements whatever the interface looks like at
     * runtime and forwards everything except {@code getOutput()} untouched, so new interface methods keep
     * working without code changes here.
     */
    private BuildResult normalizedBuildResult(BuildResult result) {
        final String[] normalizedOutput = new String[1];
        return (BuildResult) Proxy.newProxyInstance(
            BuildResult.class.getClassLoader(),
            new Class<?>[] { BuildResult.class },
            (proxy, method, args) -> {
                try {
                    if (method.getName().equals("getOutput") && method.getParameterCount() == 0) {
                        if (normalizedOutput[0] == null) {
                            normalizedOutput[0] = normalizeString(result.getOutput(), getProjectDir());
                        }
                        return normalizedOutput[0];
                    }
                    return method.invoke(result, args);
                } catch (InvocationTargetException e) {
                    throw e.getCause();
                }
            }
        );
    }
}
