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

final class RerunTestResultProcessor implements TestResultProcessor {

    private final TestResultProcessor delegate;

    private final Map<Object, TestDescriptorInternal> activeDescriptorsById = new HashMap<>();

    /**
     * gradle structures tests in a tree structure with the test task itself
     * being the root element. This is required to be tracked here to get the
     * structure right when rerunning a test task tests.
     * */
    private Object rootTestDescriptorId;

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
            if (activeDescriptorsById.size() != 1) {
                return;
            }
        } else {
            activeDescriptorsById.remove(testId);
        }

        delegate.completed(testId, testCompleteEvent);
    }

    @Override
    public void output(Object testId, TestOutputEvent testOutputEvent) {
        delegate.output(testId, testOutputEvent);
    }

    @Override
    public void failure(Object testId, Throwable throwable) {
        delegate.failure(testId, throwable);
    }

    public void reset() {
        this.activeDescriptorsById.clear();
    }

    public List<TestDescriptorInternal> getActiveDescriptors() {
        return new ArrayList<>(activeDescriptorsById.values());
    }
}
