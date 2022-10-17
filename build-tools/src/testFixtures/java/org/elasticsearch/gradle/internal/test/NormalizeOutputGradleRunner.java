/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test;

import org.gradle.testkit.runner.BuildResult;
import org.gradle.testkit.runner.BuildTask;
import org.gradle.testkit.runner.GradleRunner;
import org.gradle.testkit.runner.InvalidPluginMetadataException;
import org.gradle.testkit.runner.InvalidRunnerConfigurationException;
import org.gradle.testkit.runner.TaskOutcome;
import org.gradle.testkit.runner.UnexpectedBuildFailure;
import org.gradle.testkit.runner.UnexpectedBuildSuccess;

import java.io.File;
import java.io.Writer;
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
        return new NormalizedBuildResult(delegate.build());
    }

    @Override
    public BuildResult buildAndFail() throws InvalidRunnerConfigurationException, UnexpectedBuildSuccess {
        return new NormalizedBuildResult(delegate.buildAndFail());
    }

    private class NormalizedBuildResult implements BuildResult {
        private BuildResult delegate;
        private String normalizedString;

        NormalizedBuildResult(BuildResult delegate) {
            this.delegate = delegate;
        }

        @Override
        public String getOutput() {
            if (normalizedString == null) {
                normalizedString = normalizeString(delegate.getOutput(), getProjectDir());
            }
            return normalizedString;
        }

        @Override
        public List<BuildTask> getTasks() {
            return delegate.getTasks();
        }

        @Override
        public List<BuildTask> tasks(TaskOutcome taskOutcome) {
            return delegate.tasks(taskOutcome);
        }

        @Override
        public List<String> taskPaths(TaskOutcome taskOutcome) {
            return delegate.taskPaths(taskOutcome);
        }

        @Override
        public BuildTask task(String s) {
            return delegate.task(s);
        }
    }
}
