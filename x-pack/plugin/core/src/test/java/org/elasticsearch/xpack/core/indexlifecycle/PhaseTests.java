/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PhaseTests extends AbstractSerializingTestCase<Phase> {
    private String phaseName;

    @Before
    public void setup() {
        phaseName = randomAlphaOfLength(20); // NORELEASE we need to randomise the phase name rather 
                                             // than use the same name for all instances
    }

    @Override
    protected Phase createTestInstance() {
        TimeValue after = null;
        if (randomBoolean()) {
            after = TimeValue.parseTimeValue(randomTimeValue(0, 1000000000, "s", "m", "h", "d"), "test_after");
        }
        Map<String, LifecycleAction> actions = Collections.emptyMap();
        if (randomBoolean()) {
            actions = Collections.singletonMap(DeleteAction.NAME, new DeleteAction());
        }
        return new Phase(phaseName, after, actions);
    }

    @Override
    protected Phase doParseInstance(XContentParser parser) throws IOException {
        return Phase.parse(parser, phaseName);
    }

    @Override
    protected Reader<Phase> instanceReader() {
        return Phase::new;
    }

    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Arrays
                .asList(new NamedWriteableRegistry.Entry(LifecycleAction.class, DeleteAction.NAME, DeleteAction::new)));
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>(ClusterModule.getNamedXWriteables());
        entries.add(new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DeleteAction.NAME), DeleteAction::parse));
        return new NamedXContentRegistry(entries);
    }

    @Override
    protected Phase mutateInstance(Phase instance) throws IOException {
        String name = instance.getName();
        TimeValue after = instance.getAfter();
        Map<String, LifecycleAction> actions = instance.getActions();
        switch (between(0, 2)) {
        case 0:
            name = name + randomAlphaOfLengthBetween(1, 5);
            break;
        case 1:
            after = TimeValue.timeValueSeconds(after.getSeconds() + randomIntBetween(1, 1000));
            break;
        case 2:
            actions = new HashMap<>(actions);
            actions.put(MockAction.NAME, new MockAction());
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new Phase(name, after, actions);
    }

    public void testDefaultAfter() {
        Phase phase = new Phase(randomAlphaOfLength(20), null, Collections.emptyMap());
        assertEquals(TimeValue.ZERO, phase.getAfter());
    }

    public void testExecuteNewIndexCompleteActions() throws Exception {
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String phaseName = randomAlphaOfLengthBetween(1, 20);
        TimeValue after = TimeValue.timeValueSeconds(randomIntBetween(10, 100));
        Map<String, LifecycleAction> actions = new HashMap<>();
        MockAction firstAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "first_action";
            }
        };
        firstAction.setCompleteOnExecute(true);
        actions.put(firstAction.getWriteableName(), firstAction);
        MockAction secondAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "second_action";
            }
        };
        secondAction.setCompleteOnExecute(true);
        actions.put(secondAction.getWriteableName(), secondAction);
        MockAction thirdAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "third_action";
            }
        };
        thirdAction.setCompleteOnExecute(true);
        actions.put(thirdAction.getWriteableName(), thirdAction);
        Phase phase = new Phase(phaseName, after, actions);

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, phaseName, "", 0) {

            @Override
            public boolean canExecute(Phase phase) {
                throw new AssertionError("canExecute should not have been called");
            }
        };

        phase.execute(context, current -> {
            if (current == null) {
                return firstAction;
            } else if ("first_action".equals(current.getWriteableName())) {
                return secondAction;
            } else if ("second_action".equals(current.getWriteableName())) {
                return thirdAction;
            }
            return null;
        });

        assertEquals(indexName, context.getLifecycleTarget());
        assertEquals(phaseName, context.getPhase());
        assertEquals(Phase.PHASE_COMPLETED, context.getAction());

        assertTrue(firstAction.wasCompleted());
        assertEquals(1L, firstAction.getExecutedCount());
        assertTrue(secondAction.wasCompleted());
        assertEquals(1L, secondAction.getExecutedCount());
        assertTrue(thirdAction.wasCompleted());
        assertEquals(1L, thirdAction.getExecutedCount());
    }

    public void testExecuteNewIndexIncompleteFirstAction() throws Exception {
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String phaseName = randomAlphaOfLengthBetween(1, 20);
        TimeValue after = TimeValue.timeValueSeconds(randomIntBetween(10, 100));
        Map<String, LifecycleAction> actions = new HashMap<>();
        MockAction firstAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "first_action";
            }
        };
        firstAction.setCompleteOnExecute(false);
        actions.put(firstAction.getWriteableName(), firstAction);
        MockAction secondAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "second_action";
            }
        };
        actions.put(secondAction.getWriteableName(), secondAction);
        MockAction thirdAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "third_action";
            }
        };
        actions.put(thirdAction.getWriteableName(), thirdAction);
        Phase phase = new Phase(phaseName, after, actions);

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, phaseName, "", 0) {

            @Override
            public boolean canExecute(Phase phase) {
                throw new AssertionError("canExecute should not have been called");
            }
        };

        phase.execute(context, current -> {
            if (current == null) {
                return firstAction;
            } else if ("first_action".equals(current.getWriteableName())) {
                return secondAction;
            } else if ("second_action".equals(current.getWriteableName())) {
                return thirdAction;
            }
            return null;
        });

        assertEquals(indexName, context.getLifecycleTarget());
        assertEquals(phaseName, context.getPhase());
        assertEquals(firstAction.getWriteableName(), context.getAction());

        assertFalse(firstAction.wasCompleted());
        assertEquals(1L, firstAction.getExecutedCount());
        assertFalse(secondAction.wasCompleted());
        assertEquals(0L, secondAction.getExecutedCount());
        assertFalse(thirdAction.wasCompleted());
        assertEquals(0L, thirdAction.getExecutedCount());
    }

    public void testExecuteNewIndexFailure() throws Exception {
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String phaseName = randomAlphaOfLengthBetween(1, 20);
        TimeValue after = TimeValue.timeValueSeconds(randomIntBetween(10, 100));
        Map<String, LifecycleAction> actions = new HashMap<>();
        MockAction firstAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "first_action";
            }
        };
        actions.put(firstAction.getWriteableName(), firstAction);
        MockAction secondAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "second_action";
            }
        };
        actions.put(secondAction.getWriteableName(), secondAction);
        MockAction thirdAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "third_action";
            }
        };
        actions.put(thirdAction.getWriteableName(), thirdAction);
        Phase phase = new Phase(phaseName, after, actions);

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, phaseName, "", 0) {

            @Override
            public boolean canExecute(Phase phase) {
                throw new AssertionError("canExecute should not have been called");
            }
        };

        RuntimeException exception = new RuntimeException();

        context.failOnSetters(exception);
        
        phase.execute(context, current -> {
            if (current == null) {
                return firstAction;
            } else if ("first_action".equals(current.getWriteableName())) {
                return secondAction;
            } else if ("second_action".equals(current.getWriteableName())) {
                return thirdAction;
            }
            return null;
        });

        assertEquals(indexName, context.getLifecycleTarget());
        assertEquals(phaseName, context.getPhase());
        assertEquals("", context.getAction());

        assertFalse(firstAction.wasCompleted());
        assertEquals(0L, firstAction.getExecutedCount());
        assertFalse(secondAction.wasCompleted());
        assertEquals(0L, secondAction.getExecutedCount());
        assertFalse(thirdAction.wasCompleted());
        assertEquals(0L, thirdAction.getExecutedCount());
    }

    public void testExecuteNewIndexNoActions() throws Exception {
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String phaseName = randomAlphaOfLengthBetween(1, 20);
        TimeValue after = TimeValue.timeValueSeconds(randomIntBetween(10, 100));
        Phase phase = new Phase(phaseName, after, Collections.emptyMap());

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, phaseName, "", 0) {

            @Override
            public boolean canExecute(Phase phase) {
                throw new AssertionError("canExecute should not have been called");
            }
        };

        phase.execute(context, a -> null);

        assertEquals(indexName, context.getLifecycleTarget());
        assertEquals(phaseName, context.getPhase());
        assertEquals(Phase.PHASE_COMPLETED, context.getAction());
    }

    public void testExecutePhaseAlreadyComplete() throws Exception {
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String phaseName = randomAlphaOfLengthBetween(1, 20);
        TimeValue after = TimeValue.timeValueSeconds(randomIntBetween(10, 100));
        List<LifecycleAction> actions = new ArrayList<>();
        MockAction firstAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "first_action";
            }
        };
        actions.add(firstAction);
        MockAction secondAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "second_action";
            }
        };
        actions.add(secondAction);
        MockAction thirdAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "third_action";
            }
        };
        Phase phase = new Phase(phaseName, after, Collections.emptyMap());

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, phaseName, Phase.PHASE_COMPLETED, 0) {

            @Override
            public boolean canExecute(Phase phase) {
                throw new AssertionError("canExecute should not have been called");
            }
        };

        phase.execute(context, current -> {
            if (current == null) {
                return firstAction;
            } else if ("first_action".equals(current.getWriteableName())) {
                return secondAction;
            } else if ("second_action".equals(current.getWriteableName())) {
                return thirdAction;
            }
            return null;
        });

        assertEquals(indexName, context.getLifecycleTarget());
        assertEquals(phaseName, context.getPhase());
        assertEquals(Phase.PHASE_COMPLETED, context.getAction());

        assertFalse(firstAction.wasCompleted());
        assertEquals(0L, firstAction.getExecutedCount());
        assertFalse(secondAction.wasCompleted());
        assertEquals(0L, secondAction.getExecutedCount());
        assertFalse(thirdAction.wasCompleted());
        assertEquals(0L, thirdAction.getExecutedCount());
    }

    public void testExecuteFirstAction() throws Exception {
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String phaseName = randomAlphaOfLengthBetween(1, 20);
        TimeValue after = TimeValue.timeValueSeconds(randomIntBetween(10, 100));
        Map<String, LifecycleAction> actions = new HashMap<>();
        MockAction firstAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "first_action";
            }
        };
        firstAction.setCompleteOnExecute(false);
        actions.put(firstAction.getWriteableName(), firstAction);
        MockAction secondAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "second_action";
            }
        };
        secondAction.setCompleteOnExecute(false);
        actions.put(secondAction.getWriteableName(), secondAction);
        MockAction thirdAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "third_action";
            }
        };
        thirdAction.setCompleteOnExecute(false);
        actions.put(thirdAction.getWriteableName(), thirdAction);
        Phase phase = new Phase(phaseName, after, actions);

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, phaseName, firstAction.getWriteableName(), 0) {

            @Override
            public boolean canExecute(Phase phase) {
                throw new AssertionError("canExecute should not have been called");
            }
        };

        phase.execute(context, current -> {
            if (current == null) {
                return firstAction;
            } else if ("first_action".equals(current.getWriteableName())) {
                return secondAction;
            } else if ("second_action".equals(current.getWriteableName())) {
                return thirdAction;
            }
            return null;
        });

        assertEquals(indexName, context.getLifecycleTarget());
        assertEquals(phaseName, context.getPhase());
        assertEquals(firstAction.getWriteableName(), context.getAction());

        assertFalse(firstAction.wasCompleted());
        assertEquals(1L, firstAction.getExecutedCount());
        assertFalse(secondAction.wasCompleted());
        assertEquals(0L, secondAction.getExecutedCount());
        assertFalse(thirdAction.wasCompleted());
        assertEquals(0L, thirdAction.getExecutedCount());

        firstAction.setCompleteOnExecute(true);

        phase.execute(context, current -> {
            if (current == null) {
                return firstAction;
            } else if ("first_action".equals(current.getWriteableName())) {
                return secondAction;
            } else if ("second_action".equals(current.getWriteableName())) {
                return thirdAction;
            }
            return null;
        });

        assertEquals(indexName, context.getLifecycleTarget());
        assertEquals(phaseName, context.getPhase());
        assertEquals(secondAction.getWriteableName(), context.getAction());

        assertTrue(firstAction.wasCompleted());
        assertEquals(2L, firstAction.getExecutedCount());
        assertFalse(secondAction.wasCompleted());
        assertEquals(1L, secondAction.getExecutedCount());
        assertFalse(thirdAction.wasCompleted());
        assertEquals(0L, thirdAction.getExecutedCount());
    }

    public void testExecuteFirstActionIndexDoesNotSurvive() throws Exception {
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String phaseName = randomAlphaOfLengthBetween(1, 20);
        TimeValue after = TimeValue.timeValueSeconds(randomIntBetween(10, 100));
        Map<String, LifecycleAction> actions = new HashMap<>();
        MockAction firstAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "first_action";
            }

            @Override
            public boolean indexSurvives() {
                return false;
            }
        };
        firstAction.setCompleteOnExecute(false);
        actions.put(firstAction.getWriteableName(), firstAction);
        MockAction secondAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "second_action";
            }
        };
        secondAction.setCompleteOnExecute(false);
        actions.put(secondAction.getWriteableName(), secondAction);
        MockAction thirdAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "third_action";
            }
        };
        thirdAction.setCompleteOnExecute(false);
        actions.put(thirdAction.getWriteableName(), thirdAction);
        Phase phase = new Phase(phaseName, after, actions);

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, phaseName, firstAction.getWriteableName(), 0) {

            @Override
            public boolean canExecute(Phase phase) {
                throw new AssertionError("canExecute should not have been called");
            }
        };

        phase.execute(context, current -> {
            if (current == null) {
                return firstAction;
            } else if ("first_action".equals(current.getWriteableName())) {
                return secondAction;
            } else if ("second_action".equals(current.getWriteableName())) {
                return thirdAction;
            }
            return null;
        });

        assertEquals(indexName, context.getLifecycleTarget());
        assertEquals(phaseName, context.getPhase());
        assertEquals(firstAction.getWriteableName(), context.getAction());

        assertFalse(firstAction.wasCompleted());
        assertEquals(1L, firstAction.getExecutedCount());
        assertFalse(secondAction.wasCompleted());
        assertEquals(0L, secondAction.getExecutedCount());
        assertFalse(thirdAction.wasCompleted());
        assertEquals(0L, thirdAction.getExecutedCount());

        firstAction.setCompleteOnExecute(true);

        phase.execute(context, current -> {
            if (current == null) {
                return firstAction;
            } else if ("first_action".equals(current.getWriteableName())) {
                return secondAction;
            } else if ("second_action".equals(current.getWriteableName())) {
                return thirdAction;
            }
            return null;
        });

        assertEquals(indexName, context.getLifecycleTarget());
        assertEquals(phaseName, context.getPhase());
        assertEquals(firstAction.getWriteableName(), context.getAction());

        assertTrue(firstAction.wasCompleted());
        assertEquals(2L, firstAction.getExecutedCount());
        assertFalse(secondAction.wasCompleted());
        assertEquals(0L, secondAction.getExecutedCount());
        assertFalse(thirdAction.wasCompleted());
        assertEquals(0L, thirdAction.getExecutedCount());
    }

    public void testExecuteSecondAction() throws Exception {
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String phaseName = randomAlphaOfLengthBetween(1, 20);
        TimeValue after = TimeValue.timeValueSeconds(randomIntBetween(10, 100));
        Map<String, LifecycleAction> actions = new HashMap<>();
        MockAction firstAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "first_action";
            }
        };
        firstAction.setCompleteOnExecute(false);
        actions.put(firstAction.getWriteableName(), firstAction);
        MockAction secondAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "second_action";
            }
        };
        secondAction.setCompleteOnExecute(false);
        actions.put(secondAction.getWriteableName(), secondAction);
        MockAction thirdAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "third_action";
            }
        };
        thirdAction.setCompleteOnExecute(false);
        actions.put(thirdAction.getWriteableName(), thirdAction);
        Phase phase = new Phase(phaseName, after, actions);

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, phaseName, secondAction.getWriteableName(), 0) {

            @Override
            public boolean canExecute(Phase phase) {
                throw new AssertionError("canExecute should not have been called");
            }
        };

        phase.execute(context, current -> {
            if (current == null) {
                return firstAction;
            } else if ("first_action".equals(current.getWriteableName())) {
                return secondAction;
            } else if ("second_action".equals(current.getWriteableName())) {
                return thirdAction;
            }
            return null;
        });

        assertEquals(indexName, context.getLifecycleTarget());
        assertEquals(phaseName, context.getPhase());
        assertEquals(secondAction.getWriteableName(), context.getAction());

        assertFalse(firstAction.wasCompleted());
        assertEquals(0L, firstAction.getExecutedCount());
        assertFalse(secondAction.wasCompleted());
        assertEquals(1L, secondAction.getExecutedCount());
        assertFalse(thirdAction.wasCompleted());
        assertEquals(0L, thirdAction.getExecutedCount());

        secondAction.setCompleteOnExecute(true);

        phase.execute(context, current -> {
            if (current == null) {
                return firstAction;
            } else if ("first_action".equals(current.getWriteableName())) {
                return secondAction;
            } else if ("second_action".equals(current.getWriteableName())) {
                return thirdAction;
            }
            return null;
        });

        assertEquals(indexName, context.getLifecycleTarget());
        assertEquals(phaseName, context.getPhase());
        assertEquals(thirdAction.getWriteableName(), context.getAction());

        assertFalse(firstAction.wasCompleted());
        assertEquals(0L, firstAction.getExecutedCount());
        assertTrue(secondAction.wasCompleted());
        assertEquals(2L, secondAction.getExecutedCount());
        assertFalse(thirdAction.wasCompleted());
        assertEquals(1L, thirdAction.getExecutedCount());
    }

    public void testExecuteThirdAction() throws Exception {
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String phaseName = randomAlphaOfLengthBetween(1, 20);
        TimeValue after = TimeValue.timeValueSeconds(randomIntBetween(10, 100));
        Map<String, LifecycleAction> actions = new HashMap<>();
        MockAction firstAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "first_action";
            }
        };
        firstAction.setCompleteOnExecute(false);
        actions.put(firstAction.getWriteableName(), firstAction);
        MockAction secondAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "second_action";
            }
        };
        secondAction.setCompleteOnExecute(false);
        actions.put(secondAction.getWriteableName(), secondAction);
        MockAction thirdAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "third_action";
            }
        };
        thirdAction.setCompleteOnExecute(false);
        actions.put(thirdAction.getWriteableName(), thirdAction);
        Phase phase = new Phase(phaseName, after, actions);

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, phaseName, thirdAction.getWriteableName(), 0) {

            @Override
            public boolean canExecute(Phase phase) {
                throw new AssertionError("canExecute should not have been called");
            }
        };

        phase.execute(context, current -> {
            if (current == null) {
                return firstAction;
            } else if ("first_action".equals(current.getWriteableName())) {
                return secondAction;
            } else if ("second_action".equals(current.getWriteableName())) {
                return thirdAction;
            }
            return null;
        });

        assertEquals(indexName, context.getLifecycleTarget());
        assertEquals(phaseName, context.getPhase());
        assertEquals(thirdAction.getWriteableName(), context.getAction());

        assertFalse(firstAction.wasCompleted());
        assertEquals(0L, firstAction.getExecutedCount());
        assertFalse(secondAction.wasCompleted());
        assertEquals(0L, secondAction.getExecutedCount());
        assertFalse(thirdAction.wasCompleted());
        assertEquals(1L, thirdAction.getExecutedCount());

        thirdAction.setCompleteOnExecute(true);

        phase.execute(context, current -> {
            if (current == null) {
                return firstAction;
            } else if ("first_action".equals(current.getWriteableName())) {
                return secondAction;
            } else if ("second_action".equals(current.getWriteableName())) {
                return thirdAction;
            }
            return null;
        });

        assertEquals(indexName, context.getLifecycleTarget());
        assertEquals(phaseName, context.getPhase());
        assertEquals(Phase.PHASE_COMPLETED, context.getAction());

        assertFalse(firstAction.wasCompleted());
        assertEquals(0L, firstAction.getExecutedCount());
        assertFalse(secondAction.wasCompleted());
        assertEquals(0L, secondAction.getExecutedCount());
        assertTrue(thirdAction.wasCompleted());
        assertEquals(2L, thirdAction.getExecutedCount());
    }

    public void testExecuteMissingAction() throws Exception {
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String phaseName = randomAlphaOfLengthBetween(1, 20);
        TimeValue after = TimeValue.timeValueSeconds(randomIntBetween(10, 100));
        Map<String, LifecycleAction> actions = new HashMap<>();
        MockAction firstAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "first_action";
            }
        };
        actions.put(firstAction.getWriteableName(), firstAction);
        MockAction secondAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "second_action";
            }
        };
        actions.put(secondAction.getWriteableName(), secondAction);
        MockAction thirdAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "third_action";
            }
        };
        actions.put(thirdAction.getWriteableName(), thirdAction);
        Phase phase = new Phase(phaseName, after, actions);

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, phaseName, "does_not_exist", 0) {

            @Override
            public boolean canExecute(Phase phase) {
                throw new AssertionError("canExecute should not have been called");
            }
        };

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> phase.execute(context, a -> firstAction));
        assertEquals("Current action [" + "does_not_exist" + "] not found in phase [" + phaseName + "] for index [" + indexName + "]",
                exception.getMessage());

        assertEquals(indexName, context.getLifecycleTarget());
        assertEquals(phaseName, context.getPhase());
        assertEquals("does_not_exist", context.getAction());

        assertFalse(firstAction.wasCompleted());
        assertEquals(0L, firstAction.getExecutedCount());
        assertFalse(secondAction.wasCompleted());
        assertEquals(0L, secondAction.getExecutedCount());
        assertFalse(thirdAction.wasCompleted());
        assertEquals(0L, thirdAction.getExecutedCount());
    }

    public void testExecuteActionFailure() throws Exception {
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String phaseName = randomAlphaOfLengthBetween(1, 20);
        TimeValue after = TimeValue.timeValueSeconds(randomIntBetween(10, 100));
        Map<String, LifecycleAction> actions = new HashMap<>();
        MockAction firstAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "first_action";
            }
        };
        firstAction.setCompleteOnExecute(false);
        actions.put(firstAction.getWriteableName(), firstAction);
        MockAction secondAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "second_action";
            }
        };
        actions.put(secondAction.getWriteableName(), secondAction);
        MockAction thirdAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "third_action";
            }
        };
        thirdAction.setCompleteOnExecute(false);
        actions.put(thirdAction.getWriteableName(), thirdAction);
        Phase phase = new Phase(phaseName, after, actions);

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, phaseName, secondAction.getWriteableName(), 0) {

            @Override
            public boolean canExecute(Phase phase) {
                throw new AssertionError("canExecute should not have been called");
            }
        };

        // First check that if an exception is thrown when we execute the second
        // action, action is not completed and the phase and action are not
        // updated in the context
        Exception exception = new RuntimeException();
        secondAction.setCompleteOnExecute(false);
        secondAction.setExceptionToThrow(exception);

        phase.execute(context, current -> {
            if (current == null) {
                return firstAction;
            } else if ("first_action".equals(current.getWriteableName())) {
                return secondAction;
            } else if ("second_action".equals(current.getWriteableName())) {
                return thirdAction;
            }
            return null;
        });

        assertEquals(indexName, context.getLifecycleTarget());
        assertEquals(phaseName, context.getPhase());
        assertEquals(secondAction.getWriteableName(), context.getAction());

        assertFalse(firstAction.wasCompleted());
        assertEquals(0L, firstAction.getExecutedCount());
        assertFalse(secondAction.wasCompleted());
        assertEquals(1L, secondAction.getExecutedCount());
        assertFalse(thirdAction.wasCompleted());
        assertEquals(0L, thirdAction.getExecutedCount());

        // Now check that if an exception is thrown when we execute the second
        // action again, action is not completed even though it would normally
        // complete, also check the third action is not executed in this case
        // and the phase and action are not updated in the context
        secondAction.setCompleteOnExecute(true);

        phase.execute(context, current -> {
            if (current == null) {
                return firstAction;
            } else if ("first_action".equals(current.getWriteableName())) {
                return secondAction;
            } else if ("second_action".equals(current.getWriteableName())) {
                return thirdAction;
            }
            return null;
        });

        assertEquals(indexName, context.getLifecycleTarget());
        assertEquals(phaseName, context.getPhase());
        assertEquals(secondAction.getWriteableName(), context.getAction());

        assertFalse(firstAction.wasCompleted());
        assertEquals(0L, firstAction.getExecutedCount());
        assertFalse(secondAction.wasCompleted());
        assertEquals(2L, secondAction.getExecutedCount());
        assertFalse(thirdAction.wasCompleted());
        assertEquals(0L, thirdAction.getExecutedCount());

        // Now check that if the action is run again without an exception thrown
        // then it completes successfully, the action and phase in the context
        // are updated and the third action is executed
        secondAction.setCompleteOnExecute(true);
        secondAction.setExceptionToThrow(null);

        phase.execute(context, current -> {
            if (current == null) {
                return firstAction;
            } else if ("first_action".equals(current.getWriteableName())) {
                return secondAction;
            } else if ("second_action".equals(current.getWriteableName())) {
                return thirdAction;
            }
            return null;
        });

        assertEquals(indexName, context.getLifecycleTarget());
        assertEquals(phaseName, context.getPhase());
        assertEquals(thirdAction.getWriteableName(), context.getAction());

        assertFalse(firstAction.wasCompleted());
        assertEquals(0L, firstAction.getExecutedCount());
        assertTrue(secondAction.wasCompleted());
        assertEquals(3L, secondAction.getExecutedCount());
        assertFalse(thirdAction.wasCompleted());
        assertEquals(1L, thirdAction.getExecutedCount());
    }

    public void testExecuteActionFailedSetAction() throws Exception {
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String phaseName = randomAlphaOfLengthBetween(1, 20);
        TimeValue after = TimeValue.timeValueSeconds(randomIntBetween(10, 100));
        Map<String, LifecycleAction> actions = new HashMap<>();
        MockAction firstAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "first_action";
            }
        };
        firstAction.setCompleteOnExecute(false);
        actions.put(firstAction.getWriteableName(), firstAction);
        MockAction secondAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "second_action";
            }
        };
        secondAction.setCompleteOnExecute(true);
        actions.put(secondAction.getWriteableName(), secondAction);
        MockAction thirdAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "third_action";
            }
        };
        thirdAction.setCompleteOnExecute(false);
        actions.put(thirdAction.getWriteableName(), thirdAction);
        Phase phase = new Phase(phaseName, after, actions);

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, phaseName, secondAction.getWriteableName(), 0) {

            @Override
            public boolean canExecute(Phase phase) {
                throw new AssertionError("canExecute should not have been called");
            }
        };

        // First check that if setting the new action fails we don't execute the
        // next phase
        Exception exception = new RuntimeException();
        context.failOnSetters(exception);

        phase.execute(context, current -> {
            if (current == null) {
                return firstAction;
            } else if ("first_action".equals(current.getWriteableName())) {
                return secondAction;
            } else if ("second_action".equals(current.getWriteableName())) {
                return thirdAction;
            }
            return null;
        });

        assertEquals(indexName, context.getLifecycleTarget());
        assertEquals(phaseName, context.getPhase());
        assertEquals(secondAction.getWriteableName(), context.getAction());

        assertFalse(firstAction.wasCompleted());
        assertEquals(0L, firstAction.getExecutedCount());
        assertTrue(secondAction.wasCompleted());
        assertEquals(1L, secondAction.getExecutedCount());
        assertFalse(thirdAction.wasCompleted());
        assertEquals(0L, thirdAction.getExecutedCount());

        // Now check that if we execute the phase again the current action is
        // re-executed and if setting the new action fails again we still don't
        // execute the next action
        secondAction.setCompleteOnExecute(true);
        secondAction.resetCompleted();

        phase.execute(context, current -> {
            if (current == null) {
                return firstAction;
            } else if ("first_action".equals(current.getWriteableName())) {
                return secondAction;
            } else if ("second_action".equals(current.getWriteableName())) {
                return thirdAction;
            }
            return null;
        });

        assertEquals(indexName, context.getLifecycleTarget());
        assertEquals(phaseName, context.getPhase());
        assertEquals(secondAction.getWriteableName(), context.getAction());

        assertFalse(firstAction.wasCompleted());
        assertEquals(0L, firstAction.getExecutedCount());
        assertTrue(secondAction.wasCompleted());
        assertEquals(2L, secondAction.getExecutedCount());
        assertFalse(thirdAction.wasCompleted());
        assertEquals(0L, thirdAction.getExecutedCount());

        // Now check that if we execute the phase again the current action is
        // re-executed and if setting the new action suceeds now the next action
        // is executed
        secondAction.setCompleteOnExecute(true);
        context.failOnSetters(null);
        secondAction.resetCompleted();

        phase.execute(context, current -> {
            if (current == null) {
                return firstAction;
            } else if ("first_action".equals(current.getWriteableName())) {
                return secondAction;
            } else if ("second_action".equals(current.getWriteableName())) {
                return thirdAction;
            }
            return null;
        });

        assertEquals(indexName, context.getLifecycleTarget());
        assertEquals(phaseName, context.getPhase());
        assertEquals(thirdAction.getWriteableName(), context.getAction());

        assertFalse(firstAction.wasCompleted());
        assertEquals(0L, firstAction.getExecutedCount());
        assertTrue(secondAction.wasCompleted());
        assertEquals(3L, secondAction.getExecutedCount());
        assertFalse(thirdAction.wasCompleted());
        assertEquals(1L, thirdAction.getExecutedCount());
    }

    public void testExecuteLastActionFailedSetAction() throws Exception {
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String phaseName = randomAlphaOfLengthBetween(1, 20);
        TimeValue after = TimeValue.timeValueSeconds(randomIntBetween(10, 100));
        Map<String, LifecycleAction> actions = new HashMap<>();
        MockAction firstAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "first_action";
            }
        };
        firstAction.setCompleteOnExecute(false);
        actions.put(firstAction.getWriteableName(), firstAction);
        MockAction secondAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "second_action";
            }
        };
        secondAction.setCompleteOnExecute(false);
        actions.put(secondAction.getWriteableName(), secondAction);
        MockAction thirdAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "third_action";
            }
        };
        thirdAction.setCompleteOnExecute(true);
        actions.put(thirdAction.getWriteableName(), thirdAction);
        Phase phase = new Phase(phaseName, after, actions);

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, phaseName, thirdAction.getWriteableName(), 0) {

            @Override
            public boolean canExecute(Phase phase) {
                throw new AssertionError("canExecute should not have been called");
            }
        };

        // First check setting the ACTION_COMPLETED fails
        Exception exception = new RuntimeException();
        context.failOnSetters(exception);

        phase.execute(context, current -> {
            if (current == null) {
                return firstAction;
            } else if ("first_action".equals(current.getWriteableName())) {
                return secondAction;
            } else if ("second_action".equals(current.getWriteableName())) {
                return thirdAction;
            }
            return null;
        });

        assertEquals(indexName, context.getLifecycleTarget());
        assertEquals(phaseName, context.getPhase());
        assertEquals(thirdAction.getWriteableName(), context.getAction());

        assertFalse(firstAction.wasCompleted());
        assertEquals(0L, firstAction.getExecutedCount());
        assertFalse(secondAction.wasCompleted());
        assertEquals(0L, secondAction.getExecutedCount());
        assertTrue(thirdAction.wasCompleted());
        assertEquals(1L, thirdAction.getExecutedCount());

        // Now check the same happens if it fails again
        thirdAction.setCompleteOnExecute(true);
        thirdAction.resetCompleted();

        phase.execute(context, current -> {
            if (current == null) {
                return firstAction;
            } else if ("first_action".equals(current.getWriteableName())) {
                return secondAction;
            } else if ("second_action".equals(current.getWriteableName())) {
                return thirdAction;
            }
            return null;
        });

        assertEquals(indexName, context.getLifecycleTarget());
        assertEquals(phaseName, context.getPhase());
        assertEquals(thirdAction.getWriteableName(), context.getAction());

        assertFalse(firstAction.wasCompleted());
        assertEquals(0L, firstAction.getExecutedCount());
        assertFalse(secondAction.wasCompleted());
        assertEquals(0L, secondAction.getExecutedCount());
        assertTrue(thirdAction.wasCompleted());
        assertEquals(2L, thirdAction.getExecutedCount());

        // Now check the action updates if we try again and setting the
        // PHASE_COMPLETED succeeds
        context.failOnSetters(null);
        thirdAction.resetCompleted();

        phase.execute(context, current -> {
            if (current == null) {
                return firstAction;
            } else if ("first_action".equals(current.getWriteableName())) {
                return secondAction;
            } else if ("second_action".equals(current.getWriteableName())) {
                return thirdAction;
            }
            return null;
        });

        assertEquals(indexName, context.getLifecycleTarget());
        assertEquals(phaseName, context.getPhase());
        assertEquals(Phase.PHASE_COMPLETED, context.getAction());

        assertFalse(firstAction.wasCompleted());
        assertEquals(0L, firstAction.getExecutedCount());
        assertFalse(secondAction.wasCompleted());
        assertEquals(0L, secondAction.getExecutedCount());
        assertTrue(thirdAction.wasCompleted());
        assertEquals(3L, thirdAction.getExecutedCount());
    }

}
