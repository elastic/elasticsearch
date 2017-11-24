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
                randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20)) {

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
                randomAlphaOfLengthBetween(1, 20)) {

            @Override
            public boolean canExecute(Phase phase) {
                throw new AssertionError("canExecute should not have been called.");
            }
        };

        assertEquals(phase, context.getPhase());
    }

    public void testSetAction() {
        String targetName = randomAlphaOfLengthBetween(1, 20);
        String phase = randomAlphaOfLengthBetween(1, 20);
        String newAction = randomAlphaOfLengthBetween(1, 20);

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(targetName, phase,
                randomAlphaOfLengthBetween(1, 20)) {

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
                randomAlphaOfLengthBetween(1, 20), action) {

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
                randomAlphaOfLengthBetween(1, 20)) {

            @Override
            public boolean canExecute(Phase phase) {
                throw new AssertionError("canExecute should not have been called.");
            }
        };

        assertEquals(target, context.getLifecycleTarget());
    }

    public void testExecuteAction() {
        MockIndexLifecycleContext context = new MockIndexLifecycleContext(randomAlphaOfLengthBetween(1, 20),
                randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20)) {

            @Override
            public boolean canExecute(Phase phase) {
                throw new AssertionError("canExecute should not have been called.");
            }
        };

        MockAction action = new MockAction();

        assertFalse(action.wasExecuted());

        context.executeAction(action);

        assertTrue(action.wasExecuted());
    }
}
