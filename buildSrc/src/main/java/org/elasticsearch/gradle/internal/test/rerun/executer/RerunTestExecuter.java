/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rerun.executer;

import org.elasticsearch.gradle.internal.test.rerun.TestRerunTaskExtension;
import org.gradle.api.GradleException;
import org.gradle.api.internal.tasks.testing.JvmTestExecutionSpec;
import org.gradle.api.internal.tasks.testing.TestDescriptorInternal;
import org.gradle.api.internal.tasks.testing.TestExecuter;
import org.gradle.api.internal.tasks.testing.TestResultProcessor;
import org.gradle.internal.id.CompositeIdGenerator;
import org.gradle.process.internal.ExecException;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public final class RerunTestExecuter implements TestExecuter<JvmTestExecutionSpec> {

    private final TestRerunTaskExtension extension;
    private final TestExecuter<JvmTestExecutionSpec> delegate;

    public RerunTestExecuter(TestRerunTaskExtension extension, TestExecuter<JvmTestExecutionSpec> delegate) {
        this.extension = extension;
        this.delegate = delegate;
    }

    @Override
    public void execute(JvmTestExecutionSpec spec, TestResultProcessor testResultProcessor) {
        int maxRetries = extension.getMaxReruns().get();
        if (maxRetries <= 0) {
            delegate.execute(spec, testResultProcessor);
            return;
        }

        RerunTestResultProcessor retryTestResultProcessor = new RerunTestResultProcessor(testResultProcessor);

        int retryCount = 0;
        JvmTestExecutionSpec testExecutionSpec = spec;
        while (true) {
            try {
                delegate.execute(testExecutionSpec, retryTestResultProcessor);
                break;
            } catch (ExecException e) {
                extension.getDidRerun().set(true);
                report(retryCount + 1, retryTestResultProcessor.getActiveDescriptors());
                if (retryCount++ == maxRetries) {
                    throw new GradleException("Max retries(" + maxRetries + ") hit", e);
                } else {
                    retryTestResultProcessor.reset();
                }
            }
        }
    }

    @Override
    public void stopNow() {
        delegate.stopNow();
    }

    void report(int runCount, List<TestDescriptorInternal> activeDescriptors) {
        String report = "================\n"
            + "Test jvm exited unexpectedly.\n"
            + "Test jvm system exit trace (run: "
            + runCount
            + ")\n"
            + activeDescriptors.stream()
                .filter(d -> d.getId() instanceof CompositeIdGenerator.CompositeId)
                .sorted(Comparator.comparing(o -> o.getId().toString()))
                .map(TestDescriptorInternal::getName)
                .collect(Collectors.joining(" > "))
            + "\n"
            + "================\n";
        System.out.println(report);
    }
}
