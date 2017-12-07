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
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class LifecyclePolicyTests extends ESTestCase {

    private String indexName;
    private String lifecycleName;
    private MockAction firstAction;
    private MockAction secondAction;
    private MockAction thirdAction;
    private Phase firstPhase;
    private Phase secondPhase;
    private Phase thirdPhase;
    private LifecyclePolicy policy;

    @Before
    public void setupPolicy() {
        indexName = randomAlphaOfLengthBetween(1, 20);
        lifecycleName = randomAlphaOfLengthBetween(1, 20);
        List<Phase> phases = new ArrayList<>();
        firstAction = new MockAction();
        Map<String, LifecycleAction> actions = Collections.singletonMap(MockAction.NAME, firstAction);
        TimeValue after = TimeValue.timeValueSeconds(0);
        firstPhase = new Phase("first_phase", after, actions);
        phases.add(firstPhase);
        secondAction = new MockAction();
        actions = Collections.singletonMap(MockAction.NAME, secondAction);
        after = TimeValue.timeValueSeconds(10);
        secondPhase = new Phase("second_phase", after, actions);
        phases.add(secondPhase);
        thirdAction = new MockAction();
        actions = Collections.singletonMap(MockAction.NAME, thirdAction);
        after = TimeValue.timeValueSeconds(20);
        thirdPhase = new Phase("third_phase", after, actions);
        phases.add(thirdPhase);
        policy = new TestLifecyclePolicy(lifecycleName, phases);
    }

    public void testExecuteNewIndexBeforeTrigger() throws Exception {
        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, "", "", 0) {

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
        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, "", "", 0) {

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
        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, "", "", 0) {

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
        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, firstPhase.getName(), "", 0) {

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
        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, secondPhase.getName(), "", 0) {

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
        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, thirdPhase.getName(), "", 0) {

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
        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, "does_not_exist", "", 0) {

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
        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, firstPhase.getName(), Phase.PHASE_COMPLETED, 0) {

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
        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, firstPhase.getName(), Phase.PHASE_COMPLETED, 0) {

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
        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, secondPhase.getName(), Phase.PHASE_COMPLETED, 0) {

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
        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, secondPhase.getName(), Phase.PHASE_COMPLETED, 0) {

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
        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, thirdPhase.getName(), Phase.PHASE_COMPLETED, 0) {
            @Override
            public boolean canExecute(Phase phase) {
                throw new AssertionError("canExecute should not have been called");
            }

            @Override
            public int getNumberOfReplicas() {
                return 0;
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
