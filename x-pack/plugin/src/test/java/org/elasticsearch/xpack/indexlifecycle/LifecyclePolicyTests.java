/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

public class LifecyclePolicyTests extends ESTestCase {

    public void testExecuteNewIndexBeforeTrigger() throws Exception {
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String lifecycleName = randomAlphaOfLengthBetween(1, 20);
        List<Phase> phases = new ArrayList<>();
        List<LifecycleAction> actions = new ArrayList<>();
        MockAction firstAction = new MockAction();
        actions.add(firstAction);
        TimeValue after = TimeValue.timeValueSeconds(0);
        Phase firstPhase = new Phase("first_phase", after, actions);
        phases.add(firstPhase);
        actions = new ArrayList<>();
        MockAction secondAction = new MockAction();
        actions.add(secondAction);
        after = TimeValue.timeValueSeconds(10);
        Phase secondPhase = new Phase("second_phase", after, actions);
        phases.add(secondPhase);
        actions = new ArrayList<>();
        MockAction thirdAction = new MockAction();
        actions.add(thirdAction);
        after = TimeValue.timeValueSeconds(20);
        Phase thirdPhase = new Phase("third_phase", after, actions);
        phases.add(thirdPhase);
        LifecyclePolicy policy = new TestLifecyclePolicy(lifecycleName, phases);

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, "", "") {

            @Override
            public boolean canExecute(Phase phase) {
                if (phase == firstPhase) {
                    return false;
                } else {
                    throw new AssertionError("canExecute should not have been called on this phase: " + phase.getName());
                }
            }
        };

        policy.execute(context);

        assertEquals(indexName, context.getLifecycleTarget());
        assertEquals("", context.getPhase());
        assertEquals("", context.getAction());

        assertFalse(firstAction.wasCompleted());
        assertEquals(0L, firstAction.getExecutedCount());
        assertFalse(secondAction.wasCompleted());
        assertEquals(0L, secondAction.getExecutedCount());
        assertFalse(thirdAction.wasCompleted());
        assertEquals(0L, thirdAction.getExecutedCount());
    }

    public void testExecuteNewIndexAfterTrigger() throws Exception {
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String lifecycleName = randomAlphaOfLengthBetween(1, 20);
        List<Phase> phases = new ArrayList<>();
        List<LifecycleAction> actions = new ArrayList<>();
        MockAction firstAction = new MockAction();
        actions.add(firstAction);
        TimeValue after = TimeValue.timeValueSeconds(0);
        Phase firstPhase = new Phase("first_phase", after, actions);
        phases.add(firstPhase);
        actions = new ArrayList<>();
        MockAction secondAction = new MockAction();
        actions.add(secondAction);
        after = TimeValue.timeValueSeconds(10);
        Phase secondPhase = new Phase("second_phase", after, actions);
        phases.add(secondPhase);
        actions = new ArrayList<>();
        MockAction thirdAction = new MockAction();
        actions.add(thirdAction);
        after = TimeValue.timeValueSeconds(20);
        Phase thirdPhase = new Phase("third_phase", after, actions);
        phases.add(thirdPhase);
        LifecyclePolicy policy = new TestLifecyclePolicy(lifecycleName, phases);

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, "", "") {

            @Override
            public boolean canExecute(Phase phase) {
                if (phase == firstPhase) {
                    return true;
                } else {
                    throw new AssertionError("canExecute should not have been called on this phase: " + phase.getName());
                }
            }
        };

        policy.execute(context);

        assertEquals(indexName, context.getLifecycleTarget());
        assertEquals(firstPhase.getName(), context.getPhase());
        assertEquals(MockAction.NAME, context.getAction());

        assertFalse(firstAction.wasCompleted());
        assertEquals(1L, firstAction.getExecutedCount());
        assertFalse(secondAction.wasCompleted());
        assertEquals(0L, secondAction.getExecutedCount());
        assertFalse(thirdAction.wasCompleted());
        assertEquals(0L, thirdAction.getExecutedCount());
    }

    public void testExecuteNewIndexAfterTriggerFailure() throws Exception {
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String lifecycleName = randomAlphaOfLengthBetween(1, 20);
        List<Phase> phases = new ArrayList<>();
        List<LifecycleAction> actions = new ArrayList<>();
        MockAction firstAction = new MockAction();
        actions.add(firstAction);
        TimeValue after = TimeValue.timeValueSeconds(0);
        Phase firstPhase = new Phase("first_phase", after, actions);
        phases.add(firstPhase);
        actions = new ArrayList<>();
        MockAction secondAction = new MockAction();
        actions.add(secondAction);
        after = TimeValue.timeValueSeconds(10);
        Phase secondPhase = new Phase("second_phase", after, actions);
        phases.add(secondPhase);
        actions = new ArrayList<>();
        MockAction thirdAction = new MockAction();
        actions.add(thirdAction);
        after = TimeValue.timeValueSeconds(20);
        Phase thirdPhase = new Phase("third_phase", after, actions);
        phases.add(thirdPhase);
        LifecyclePolicy policy = new TestLifecyclePolicy(lifecycleName, phases);

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, "", "") {

            @Override
            public boolean canExecute(Phase phase) {
                if (phase == firstPhase) {
                    return true;
                } else {
                    throw new AssertionError("canExecute should not have been called on this phase: " + phase.getName());
                }
            }
        };

        RuntimeException exception = new RuntimeException();

        context.failOnSetters(exception);

        policy.execute(context);

        assertEquals(indexName, context.getLifecycleTarget());
        assertEquals("", context.getPhase());
        assertEquals("", context.getAction());

        assertFalse(firstAction.wasCompleted());
        assertEquals(0L, firstAction.getExecutedCount());
        assertFalse(secondAction.wasCompleted());
        assertEquals(0L, secondAction.getExecutedCount());
        assertFalse(thirdAction.wasCompleted());
        assertEquals(0L, thirdAction.getExecutedCount());
    }

    public void testExecuteFirstPhase() throws Exception {
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String lifecycleName = randomAlphaOfLengthBetween(1, 20);
        List<Phase> phases = new ArrayList<>();
        List<LifecycleAction> actions = new ArrayList<>();
        MockAction firstAction = new MockAction();
        actions.add(firstAction);
        TimeValue after = TimeValue.timeValueSeconds(0);
        Phase firstPhase = new Phase("first_phase", after, actions);
        phases.add(firstPhase);
        actions = new ArrayList<>();
        MockAction secondAction = new MockAction();
        actions.add(secondAction);
        after = TimeValue.timeValueSeconds(10);
        Phase secondPhase = new Phase("second_phase", after, actions);
        phases.add(secondPhase);
        actions = new ArrayList<>();
        MockAction thirdAction = new MockAction();
        actions.add(thirdAction);
        after = TimeValue.timeValueSeconds(20);
        Phase thirdPhase = new Phase("third_phase", after, actions);
        phases.add(thirdPhase);
        LifecyclePolicy policy = new TestLifecyclePolicy(lifecycleName, phases);

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, firstPhase.getName(), "") {

            @Override
            public boolean canExecute(Phase phase) {
                throw new AssertionError("canExecute should not have been called.");
            }
        };

        policy.execute(context);

        assertEquals(indexName, context.getLifecycleTarget());
        assertEquals(firstPhase.getName(), context.getPhase());
        assertEquals(MockAction.NAME, context.getAction());

        assertFalse(firstAction.wasCompleted());
        assertEquals(1L, firstAction.getExecutedCount());
        assertFalse(secondAction.wasCompleted());
        assertEquals(0L, secondAction.getExecutedCount());
        assertFalse(thirdAction.wasCompleted());
        assertEquals(0L, thirdAction.getExecutedCount());
    }

    public void testExecuteSecondPhase() throws Exception {
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String lifecycleName = randomAlphaOfLengthBetween(1, 20);
        List<Phase> phases = new ArrayList<>();
        List<LifecycleAction> actions = new ArrayList<>();
        MockAction firstAction = new MockAction();
        actions.add(firstAction);
        TimeValue after = TimeValue.timeValueSeconds(0);
        Phase firstPhase = new Phase("first_phase", after, actions);
        phases.add(firstPhase);
        actions = new ArrayList<>();
        MockAction secondAction = new MockAction();
        actions.add(secondAction);
        after = TimeValue.timeValueSeconds(10);
        Phase secondPhase = new Phase("second_phase", after, actions);
        phases.add(secondPhase);
        actions = new ArrayList<>();
        MockAction thirdAction = new MockAction();
        actions.add(thirdAction);
        after = TimeValue.timeValueSeconds(20);
        Phase thirdPhase = new Phase("third_phase", after, actions);
        phases.add(thirdPhase);
        LifecyclePolicy policy = new TestLifecyclePolicy(lifecycleName, phases);

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, secondPhase.getName(), "") {

            @Override
            public boolean canExecute(Phase phase) {
                throw new AssertionError("canExecute should not have been called.");
            }
        };

        policy.execute(context);

        assertEquals(indexName, context.getLifecycleTarget());
        assertEquals(secondPhase.getName(), context.getPhase());
        assertEquals(MockAction.NAME, context.getAction());

        assertFalse(firstAction.wasCompleted());
        assertEquals(0L, firstAction.getExecutedCount());
        assertFalse(secondAction.wasCompleted());
        assertEquals(1L, secondAction.getExecutedCount());
        assertFalse(thirdAction.wasCompleted());
        assertEquals(0L, thirdAction.getExecutedCount());
    }

    public void testExecuteThirdPhase() throws Exception {
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String lifecycleName = randomAlphaOfLengthBetween(1, 20);
        List<Phase> phases = new ArrayList<>();
        List<LifecycleAction> actions = new ArrayList<>();
        MockAction firstAction = new MockAction();
        actions.add(firstAction);
        TimeValue after = TimeValue.timeValueSeconds(0);
        Phase firstPhase = new Phase("first_phase", after, actions);
        phases.add(firstPhase);
        actions = new ArrayList<>();
        MockAction secondAction = new MockAction();
        actions.add(secondAction);
        after = TimeValue.timeValueSeconds(10);
        Phase secondPhase = new Phase("second_phase", after, actions);
        phases.add(secondPhase);
        actions = new ArrayList<>();
        MockAction thirdAction = new MockAction();
        actions.add(thirdAction);
        after = TimeValue.timeValueSeconds(20);
        Phase thirdPhase = new Phase("third_phase", after, actions);
        phases.add(thirdPhase);
        LifecyclePolicy policy = new TestLifecyclePolicy(lifecycleName, phases);

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, thirdPhase.getName(), "") {

            @Override
            public boolean canExecute(Phase phase) {
                throw new AssertionError("canExecute should not have been called.");
            }
        };

        policy.execute(context);

        assertEquals(indexName, context.getLifecycleTarget());
        assertEquals(thirdPhase.getName(), context.getPhase());
        assertEquals(MockAction.NAME, context.getAction());

        assertFalse(firstAction.wasCompleted());
        assertEquals(0L, firstAction.getExecutedCount());
        assertFalse(secondAction.wasCompleted());
        assertEquals(0L, secondAction.getExecutedCount());
        assertFalse(thirdAction.wasCompleted());
        assertEquals(1L, thirdAction.getExecutedCount());
    }

    public void testExecuteMissingPhase() throws Exception {
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String lifecycleName = randomAlphaOfLengthBetween(1, 20);
        List<Phase> phases = new ArrayList<>();
        List<LifecycleAction> actions = new ArrayList<>();
        MockAction firstAction = new MockAction();
        actions.add(firstAction);
        TimeValue after = TimeValue.timeValueSeconds(0);
        Phase firstPhase = new Phase("first_phase", after, actions);
        phases.add(firstPhase);
        actions = new ArrayList<>();
        MockAction secondAction = new MockAction();
        actions.add(secondAction);
        after = TimeValue.timeValueSeconds(10);
        Phase secondPhase = new Phase("second_phase", after, actions);
        phases.add(secondPhase);
        actions = new ArrayList<>();
        MockAction thirdAction = new MockAction();
        actions.add(thirdAction);
        after = TimeValue.timeValueSeconds(20);
        Phase thirdPhase = new Phase("third_phase", after, actions);
        phases.add(thirdPhase);
        LifecyclePolicy policy = new TestLifecyclePolicy(lifecycleName, phases);

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, "does_not_exist", "") {

            @Override
            public boolean canExecute(Phase phase) {
                throw new AssertionError("canExecute should not have been called.");
            }
        };

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> policy.execute(context));
        assertEquals(
                "Current phase [" + "does_not_exist" + "] not found in lifecycle [" + lifecycleName + "] for index [" + indexName + "]",
                exception.getMessage());

        assertEquals(indexName, context.getLifecycleTarget());
        assertEquals("does_not_exist", context.getPhase());
        assertEquals("", context.getAction());

        assertFalse(firstAction.wasCompleted());
        assertEquals(0L, firstAction.getExecutedCount());
        assertFalse(secondAction.wasCompleted());
        assertEquals(0L, secondAction.getExecutedCount());
        assertFalse(thirdAction.wasCompleted());
        assertEquals(0L, thirdAction.getExecutedCount());
    }

    public void testExecuteFirstPhaseCompletedBeforeTrigger() throws Exception {
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String lifecycleName = randomAlphaOfLengthBetween(1, 20);
        List<Phase> phases = new ArrayList<>();
        List<LifecycleAction> actions = new ArrayList<>();
        MockAction firstAction = new MockAction();
        actions.add(firstAction);
        TimeValue after = TimeValue.timeValueSeconds(0);
        Phase firstPhase = new Phase("first_phase", after, actions);
        phases.add(firstPhase);
        actions = new ArrayList<>();
        MockAction secondAction = new MockAction();
        actions.add(secondAction);
        after = TimeValue.timeValueSeconds(10);
        Phase secondPhase = new Phase("second_phase", after, actions);
        phases.add(secondPhase);
        actions = new ArrayList<>();
        MockAction thirdAction = new MockAction();
        actions.add(thirdAction);
        after = TimeValue.timeValueSeconds(20);
        Phase thirdPhase = new Phase("third_phase", after, actions);
        phases.add(thirdPhase);
        LifecyclePolicy policy = new TestLifecyclePolicy(lifecycleName, phases);
        
        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, firstPhase.getName(), Phase.PHASE_COMPLETED) {
            
            @Override
            public boolean canExecute(Phase phase) {
                if (phase == secondPhase) {
                    return false;
                } else {
                    throw new AssertionError("canExecute should not have been called on this phase: " + phase.getName());
                }
            }
        };

        policy.execute(context);

        assertEquals(indexName, context.getLifecycleTarget());
        assertEquals(firstPhase.getName(), context.getPhase());
        assertEquals(Phase.PHASE_COMPLETED, context.getAction());

        assertFalse(firstAction.wasCompleted());
        assertEquals(0L, thirdAction.getExecutedCount());
        assertFalse(thirdAction.wasCompleted());
        assertEquals(0L, secondAction.getExecutedCount());
        assertFalse(thirdAction.wasCompleted());
        assertEquals(0L, thirdAction.getExecutedCount());
    }

    public void testExecuteFirstPhaseCompletedAfterTrigger() throws Exception {
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String lifecycleName = randomAlphaOfLengthBetween(1, 20);
        List<Phase> phases = new ArrayList<>();
        List<LifecycleAction> actions = new ArrayList<>();
        MockAction firstAction = new MockAction();
        actions.add(firstAction);
        TimeValue after = TimeValue.timeValueSeconds(0);
        Phase firstPhase = new Phase("first_phase", after, actions);
        phases.add(firstPhase);
        actions = new ArrayList<>();
        MockAction secondAction = new MockAction();
        actions.add(secondAction);
        after = TimeValue.timeValueSeconds(10);
        Phase secondPhase = new Phase("second_phase", after, actions);
        phases.add(secondPhase);
        actions = new ArrayList<>();
        MockAction thirdAction = new MockAction();
        actions.add(thirdAction);
        after = TimeValue.timeValueSeconds(20);
        Phase thirdPhase = new Phase("third_phase", after, actions);
        phases.add(thirdPhase);
        LifecyclePolicy policy = new TestLifecyclePolicy(lifecycleName, phases);

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, firstPhase.getName(), Phase.PHASE_COMPLETED) {

            @Override
            public boolean canExecute(Phase phase) {
                if (phase == secondPhase) {
                    return true;
                } else {
                    throw new AssertionError("canExecute should not have been called on this phase: " + phase.getName());
                }
            }
        };

        policy.execute(context);

        assertEquals(indexName, context.getLifecycleTarget());
        assertEquals(secondPhase.getName(), context.getPhase());
        assertEquals(MockAction.NAME, context.getAction());

        assertFalse(firstAction.wasCompleted());
        assertEquals(0L, firstAction.getExecutedCount());
        assertFalse(secondAction.wasCompleted());
        assertEquals(1L, secondAction.getExecutedCount());
        assertFalse(thirdAction.wasCompleted());
        assertEquals(0L, thirdAction.getExecutedCount());
    }

    public void testExecuteSecondPhaseCompletedBeforeTrigger() throws Exception {
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String lifecycleName = randomAlphaOfLengthBetween(1, 20);
        List<Phase> phases = new ArrayList<>();
        List<LifecycleAction> actions = new ArrayList<>();
        MockAction firstAction = new MockAction();
        actions.add(firstAction);
        TimeValue after = TimeValue.timeValueSeconds(0);
        Phase firstPhase = new Phase("first_phase", after, actions);
        phases.add(firstPhase);
        actions = new ArrayList<>();
        MockAction secondAction = new MockAction();
        actions.add(secondAction);
        after = TimeValue.timeValueSeconds(10);
        Phase secondPhase = new Phase("second_phase", after, actions);
        phases.add(secondPhase);
        actions = new ArrayList<>();
        MockAction thirdAction = new MockAction();
        actions.add(thirdAction);
        after = TimeValue.timeValueSeconds(20);
        Phase thirdPhase = new Phase("third_phase", after, actions);
        phases.add(thirdPhase);
        LifecyclePolicy policy = new TestLifecyclePolicy(lifecycleName, phases);

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, secondPhase.getName(), Phase.PHASE_COMPLETED) {

            @Override
            public boolean canExecute(Phase phase) {
                if (phase == thirdPhase) {
                    return false;
                } else {
                    throw new AssertionError("canExecute should not have been called on this phase: " + phase.getName());
                }
            }
        };

        policy.execute(context);

        assertEquals(indexName, context.getLifecycleTarget());
        assertEquals(secondPhase.getName(), context.getPhase());
        assertEquals(Phase.PHASE_COMPLETED, context.getAction());

        assertFalse(firstAction.wasCompleted());
        assertEquals(0L, firstAction.getExecutedCount());
        assertFalse(secondAction.wasCompleted());
        assertEquals(0L, secondAction.getExecutedCount());
        assertFalse(thirdAction.wasCompleted());
        assertEquals(0L, thirdAction.getExecutedCount());
    }

    public void testExecuteSecondPhaseCompletedAfterTrigger() throws Exception {
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String lifecycleName = randomAlphaOfLengthBetween(1, 20);
        List<Phase> phases = new ArrayList<>();
        List<LifecycleAction> actions = new ArrayList<>();
        MockAction firstAction = new MockAction();
        actions.add(firstAction);
        TimeValue after = TimeValue.timeValueSeconds(0);
        Phase firstPhase = new Phase("first_phase", after, actions);
        phases.add(firstPhase);
        actions = new ArrayList<>();
        MockAction secondAction = new MockAction();
        actions.add(secondAction);
        after = TimeValue.timeValueSeconds(10);
        Phase secondPhase = new Phase("second_phase", after, actions);
        phases.add(secondPhase);
        actions = new ArrayList<>();
        MockAction thirdAction = new MockAction();
        actions.add(thirdAction);
        after = TimeValue.timeValueSeconds(20);
        Phase thirdPhase = new Phase("third_phase", after, actions);
        phases.add(thirdPhase);
        LifecyclePolicy policy = new TestLifecyclePolicy(lifecycleName, phases);

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, secondPhase.getName(), Phase.PHASE_COMPLETED) {

            @Override
            public boolean canExecute(Phase phase) {
                if (phase == thirdPhase) {
                    return true;
                } else {
                    throw new AssertionError("canExecute should not have been called on this phase: " + phase.getName());
                }
            }
        };

        policy.execute(context);

        assertEquals(indexName, context.getLifecycleTarget());
        assertEquals(thirdPhase.getName(), context.getPhase());
        assertEquals(MockAction.NAME, context.getAction());

        assertFalse(firstAction.wasCompleted());
        assertEquals(0L, firstAction.getExecutedCount());
        assertFalse(secondAction.wasCompleted());
        assertEquals(0L, secondAction.getExecutedCount());
        assertFalse(thirdAction.wasCompleted());
        assertEquals(1L, thirdAction.getExecutedCount());
    }

    public void testExecuteThirdPhaseCompleted() throws Exception {
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String lifecycleName = randomAlphaOfLengthBetween(1, 20);
        List<Phase> phases = new ArrayList<>();
        List<LifecycleAction> actions = new ArrayList<>();
        MockAction firstAction = new MockAction();
        actions.add(firstAction);
        TimeValue after = TimeValue.timeValueSeconds(0);
        Phase firstPhase = new Phase("first_phase", after, actions);
        phases.add(firstPhase);
        actions = new ArrayList<>();
        MockAction secondAction = new MockAction();
        actions.add(secondAction);
        after = TimeValue.timeValueSeconds(10);
        Phase secondPhase = new Phase("second_phase", after, actions);
        phases.add(secondPhase);
        actions = new ArrayList<>();
        MockAction thirdAction = new MockAction();
        actions.add(thirdAction);
        after = TimeValue.timeValueSeconds(20);
        Phase thirdPhase = new Phase("third_phase", after, actions);
        phases.add(thirdPhase);
        LifecyclePolicy policy = new TestLifecyclePolicy(lifecycleName, phases);

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, thirdPhase.getName(), Phase.PHASE_COMPLETED) {

            @Override
            public boolean canExecute(Phase phase) {
                throw new AssertionError("canExecute should not have been called");
            }
        };

        policy.execute(context);

        assertEquals(indexName, context.getLifecycleTarget());
        assertEquals(thirdPhase.getName(), context.getPhase());
        assertEquals(Phase.PHASE_COMPLETED, context.getAction());

        assertFalse(firstAction.wasCompleted());
        assertEquals(0L, firstAction.getExecutedCount());
        assertFalse(secondAction.wasCompleted());
        assertEquals(0L, secondAction.getExecutedCount());
        assertFalse(thirdAction.wasCompleted());
        assertEquals(0L, thirdAction.getExecutedCount());
    }

}
