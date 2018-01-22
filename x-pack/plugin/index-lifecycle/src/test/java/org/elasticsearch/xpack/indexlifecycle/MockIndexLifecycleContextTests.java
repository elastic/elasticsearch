/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.indexlifecycle.IndexLifecycleContext.Listener;

public class MockIndexLifecycleContextTests extends ESTestCase {

    public void testSetPhase() {
        String targetName = randomAlphaOfLengthBetween(1, 20);
        String newPhase = randomAlphaOfLengthBetween(1, 20);

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(targetName,
                randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20), randomIntBetween(0, 10)) {

            @Override
            public boolean canExecute(Phase phase) {
                throw new AssertionError("canExecute should not have been called.");
            }
        };

        // Use setOnce so it throws an error if we call the listener multiple
        // times
        SetOnce<Boolean> listenerCalled = new SetOnce<>();
        context.setPhase(newPhase, new Listener() {

            @Override
            public void onSuccess() {
                listenerCalled.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("Unexpected Error", e);
            }
        });

        assertEquals(true, listenerCalled.get());
        assertEquals(newPhase, context.getPhase());
        assertEquals("", context.getAction());
        assertEquals(targetName, context.getLifecycleTarget());
    }

    public void testGetPhase() {
        String phase = randomAlphaOfLengthBetween(1, 20);

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(randomAlphaOfLengthBetween(1, 20), phase,
                randomAlphaOfLengthBetween(1, 20), randomIntBetween(0, 10)) {

            @Override
            public boolean canExecute(Phase phase) {
                throw new AssertionError("canExecute should not have been called.");
            }
        };

        assertEquals(phase, context.getPhase());
    }

    public void testGetReplicas() {
        int replicas = randomIntBetween(1, 10);

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(randomAlphaOfLengthBetween(1, 20),
                randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20), replicas) {

            @Override
            public boolean canExecute(Phase phase) {
                throw new AssertionError("canExecute should not have been called.");
            }
        };

        assertEquals(replicas, context.getNumberOfReplicas());
    }

    public void testSetAction() {
        String targetName = randomAlphaOfLengthBetween(1, 20);
        String phase = randomAlphaOfLengthBetween(1, 20);
        String newAction = randomAlphaOfLengthBetween(1, 20);

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(targetName, phase,
                randomAlphaOfLengthBetween(1, 20), randomIntBetween(0, 10)) {

            @Override
            public boolean canExecute(Phase phase) {
                throw new AssertionError("canExecute should not have been called.");
            }
        };

        // Use setOnce so it throws an error if we call the listener multiple
        // times
        SetOnce<Boolean> listenerCalled = new SetOnce<>();
        context.setAction(newAction, new Listener() {

            @Override
            public void onSuccess() {
                listenerCalled.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("Unexpected Error", e);
            }
        });

        assertEquals(true, listenerCalled.get());
        assertEquals(newAction, context.getAction());
        assertEquals(phase, context.getPhase());
        assertEquals(targetName, context.getLifecycleTarget());
    }

    public void testGetAction() {
        String action = randomAlphaOfLengthBetween(1, 20);

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(randomAlphaOfLengthBetween(1, 20),
                randomAlphaOfLengthBetween(1, 20), action, randomIntBetween(0, 10)) {

            @Override
            public boolean canExecute(Phase phase) {
                throw new AssertionError("canExecute should not have been called.");
            }
        };

        assertEquals(action, context.getAction());
    }

    public void testGetLifecycleTarget() {
        String target = randomAlphaOfLengthBetween(1, 20);
        MockIndexLifecycleContext context = new MockIndexLifecycleContext(target, randomAlphaOfLengthBetween(1, 20),
                randomAlphaOfLengthBetween(1, 20), randomIntBetween(0, 10)) {

            @Override
            public boolean canExecute(Phase phase) {
                throw new AssertionError("canExecute should not have been called.");
            }
        };

        assertEquals(target, context.getLifecycleTarget());
    }

    public void testExecuteAction() {
        MockIndexLifecycleContext context = new MockIndexLifecycleContext(randomAlphaOfLengthBetween(1, 20),
                randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20), randomIntBetween(0, 10)) {

            @Override
            public boolean canExecute(Phase phase) {
                throw new AssertionError("canExecute should not have been called.");
            }
        };

        MockAction action = new MockAction();
        action.setCompleteOnExecute(true);

        assertFalse(action.wasCompleted());
        assertEquals(0L, action.getExecutedCount());

        SetOnce<Boolean> listenerCalled = new SetOnce<>();

        context.executeAction(action, new LifecycleAction.Listener() {

            @Override
            public void onSuccess(boolean completed) {
                listenerCalled.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("Unexpected method call", e);
            }
        });

        assertTrue(action.wasCompleted());
        assertEquals(1L, action.getExecutedCount());
        assertEquals(true, listenerCalled.get());
    }

    public void testFailOnPhaseSetter() {

        String phase = randomAlphaOfLengthBetween(1, 20);
        String action = randomAlphaOfLengthBetween(1, 20);
        MockIndexLifecycleContext context = new MockIndexLifecycleContext(randomAlphaOfLengthBetween(1, 20),
                phase, action, randomIntBetween(0, 10)) {

            @Override
            public boolean canExecute(Phase phase) {
                throw new AssertionError("canExecute should not have been called.");
            }
        };
        RuntimeException exception = new RuntimeException();
        context.failOnSetters(exception);

        SetOnce<Exception> listenerCalled = new SetOnce<>();

        context.setPhase(randomAlphaOfLengthBetween(1, 20), new Listener() {

            @Override
            public void onSuccess() {
                throw new AssertionError("Unexpected method call");
            }

            @Override
            public void onFailure(Exception e) {
                listenerCalled.set(e);
            }
        });

        assertSame(exception, listenerCalled.get());
        assertEquals(phase, context.getPhase());
        assertEquals(action, context.getAction());
    }

    public void testFailOnActionSetter() {

        String phase = randomAlphaOfLengthBetween(1, 20);
        String action = randomAlphaOfLengthBetween(1, 20);

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(randomAlphaOfLengthBetween(1, 20),
                phase, action, randomIntBetween(0, 10)) {

            @Override
            public boolean canExecute(Phase phase) {
                throw new AssertionError("canExecute should not have been called.");
            }
        };
        RuntimeException exception = new RuntimeException();
        context.failOnSetters(exception);

        SetOnce<Exception> listenerCalled = new SetOnce<>();

        context.setAction(randomAlphaOfLengthBetween(1, 20), new Listener() {

            @Override
            public void onSuccess() {
                throw new AssertionError("Unexpected method call");
            }

            @Override
            public void onFailure(Exception e) {
                listenerCalled.set(e);
            }
        });

        assertSame(exception, listenerCalled.get());
        assertEquals(phase, context.getPhase());
        assertEquals(action, context.getAction());
    }
}
