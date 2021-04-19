/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rerun.executer;

import org.gradle.api.internal.tasks.testing.TestCompleteEvent;
import org.gradle.api.internal.tasks.testing.TestDescriptorInternal;
import org.gradle.api.internal.tasks.testing.TestResultProcessor;
import org.gradle.api.internal.tasks.testing.TestStartEvent;
import org.gradle.api.tasks.testing.TestOutputEvent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.gradle.api.tasks.testing.TestResult.ResultType.SKIPPED;

final class RerunTestResultProcessor implements TestResultProcessor {

    private final TestResultProcessor delegate;

    private final Map<Object, TestDescriptorInternal> activeDescriptorsById = new HashMap<>();

    private TestNames currentRoundFailedTests = new TestNames();
    private TestNames previousRoundFailedTests = new TestNames();

    private Object rootTestDescriptorId;

    private List<Map<Object,TestDescriptorInternal>> storedActiveDescriptorsWhenFailed = new ArrayList<>();

    RerunTestResultProcessor(TestResultProcessor delegate) {
        this.delegate = delegate;
    }

    @Override
    public void started(TestDescriptorInternal descriptor, TestStartEvent testStartEvent) {
        if (rootTestDescriptorId == null) {
            rootTestDescriptorId = descriptor.getId();
            activeDescriptorsById.put(descriptor.getId(), descriptor);
            delegate.started(descriptor, testStartEvent);
        } else if (descriptor.getId().equals(rootTestDescriptorId) == false) {
            activeDescriptorsById.put(descriptor.getId(), descriptor);
            delegate.started(descriptor, testStartEvent);
        }
    }

    @Override
    public void completed(Object testId, TestCompleteEvent testCompleteEvent) {
        if (testId.equals(rootTestDescriptorId)) {
            if (lastRun() == false) {
                return;
            }
        } else {
            TestDescriptorInternal descriptor = activeDescriptorsById.remove(testId);
            if (descriptor != null && descriptor.getClassName() != null) {
                String className = descriptor.getClassName();
                String name = descriptor.getName();
                boolean failedInPreviousRound = previousRoundFailedTests.remove(className, name);
                if (failedInPreviousRound && testCompleteEvent.getResultType() == SKIPPED) {
                    currentRoundFailedTests.add(className, name);
                }
            }
        }

        delegate.completed(testId, testCompleteEvent);
    }

    @Override
    public void output(Object testId, TestOutputEvent testOutputEvent) {
        delegate.output(testId, testOutputEvent);
    }

    @Override
    public void failure(Object testId, Throwable throwable) {
        final TestDescriptorInternal descriptor = activeDescriptorsById.get(testId);
        if (descriptor != null) {
            String className = descriptor.getClassName();
            if (className != null) {
                currentRoundFailedTests.add(className, descriptor.getName());
            }
        }

        delegate.failure(testId, throwable);
    }

    private boolean lastRun() {
        return activeDescriptorsById.size() == 1;
    }

    public RoundResult getResult() {
        return new RoundResult(currentRoundFailedTests, previousRoundFailedTests, lastRun());
    }

    public void reset(boolean lastRetry) {
        storeActiveDescriptors();
        this.previousRoundFailedTests = currentRoundFailedTests;
        this.currentRoundFailedTests = new TestNames();
        this.activeDescriptorsById.clear();
    }

    private void storeActiveDescriptors() {
        this.storedActiveDescriptorsWhenFailed.add(new HashMap<>(activeDescriptorsById));
    }


    public List<TestDescriptorInternal> getFailPaths() {
        return storedActiveDescriptorsWhenFailed.stream().flatMap(
            (Function<Map<Object, TestDescriptorInternal>, Stream<TestDescriptorInternal>>) objectTestDescriptorInternalMap ->
                objectTestDescriptorInternalMap.values().stream()).collect(Collectors.toList()
        );
    }
}
