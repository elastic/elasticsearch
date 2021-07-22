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
    private TestDescriptorInternal rootTestDescriptor;

    RerunTestResultProcessor(TestResultProcessor delegate) {
        this.delegate = delegate;
    }

    @Override
    public void started(TestDescriptorInternal descriptor, TestStartEvent testStartEvent) {
        activeDescriptorsById.put(descriptor.getId(), descriptor);
        if (rootTestDescriptor == null) {
            rootTestDescriptor = descriptor;
            try {
                delegate.started(descriptor, testStartEvent);
            } catch (IllegalArgumentException illegalArgumentException) {
                logTracing(descriptor.getId(), illegalArgumentException);
            }
        } else if (descriptor.getId().equals(rootTestDescriptor.getId()) == false) {
            boolean active = activeDescriptorsById.containsKey(testStartEvent.getParentId());
            if (active) {
                try {
                    delegate.started(descriptor, testStartEvent);
                } catch (IllegalArgumentException illegalArgumentException) {
                    logTracing(descriptor.getId(), illegalArgumentException);
                }
            }
        }
    }

    @Override
    public void completed(Object testId, TestCompleteEvent testCompleteEvent) {
        boolean active = activeDescriptorsById.containsKey(testId);
        if (testId.equals(rootTestDescriptor.getId())) {
            if (activeDescriptorsById.size() != 1) {
                return;
            }
        } else {
            activeDescriptorsById.remove(testId);
        }
        if (active) {
            try {
                delegate.completed(testId, testCompleteEvent);
            } catch (IllegalArgumentException illegalArgumentException) {
                logTracing(testId, illegalArgumentException);
            }
        }
    }

    @Override
    public void output(Object testId, TestOutputEvent testOutputEvent) {
        if (activeDescriptorsById.containsKey(testId)) {
            try {
                delegate.output(testId, testOutputEvent);
            } catch (IllegalArgumentException illegalArgumentException) {
                logTracing(testId, illegalArgumentException);
            }
        }
    }

    @Override
    public void failure(Object testId, Throwable throwable) {
        if (activeDescriptorsById.containsKey(testId)) {
            activeDescriptorsById.remove(testId);
            try {
                delegate.failure(testId, throwable);
            } catch (IllegalArgumentException illegalArgumentException) {
                logTracing(testId, illegalArgumentException);
            }
        }
    }

    private void logTracing(Object testId, IllegalArgumentException illegalArgumentException) {
        // Add tracing to diagnose why we see this on CI
        System.out.println("Rerun failure test id = " + testId);
        System.out.println("Active test descriptors:");
        for (Map.Entry<Object, TestDescriptorInternal> entry : activeDescriptorsById.entrySet()) {
            System.out.println("id= " + entry.getKey() + " -- " + entry.getValue().getDisplayName());
        }
        illegalArgumentException.printStackTrace();
    }

    public void reset() {
        this.activeDescriptorsById.clear();
        this.activeDescriptorsById.put(rootTestDescriptor.getId(), rootTestDescriptor);
    }

    public List<TestDescriptorInternal> getActiveDescriptors() {
        return new ArrayList<>(activeDescriptorsById.values());
    }
}
