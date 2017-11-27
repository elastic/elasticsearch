/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.Tuple;
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
import java.util.List;

public class PhaseTests extends AbstractSerializingTestCase<Phase> {
    
    private NamedXContentRegistry registry;
    private String phaseName;

    @Before
    public void setup() {
        List<NamedXContentRegistry.Entry> entries = Arrays
                .asList(new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DeleteAction.NAME), DeleteAction::parse));
        registry = new NamedXContentRegistry(entries);
        phaseName = randomAlphaOfLength(20); // NOCOMMIT we need to randomise the phase name rather 
                                             // than use the same name for all instances
    }

    @Override
    protected Phase createTestInstance() {
        TimeValue after = TimeValue.parseTimeValue(randomTimeValue(0, 1000000000, "s", "m", "h", "d"), "test_after");
        List<LifecycleAction> actions = new ArrayList<>();
        if (randomBoolean()) {
            actions.add(new DeleteAction());
        }
        return new Phase(phaseName, after, actions);
    }

    @Override
    protected Phase doParseInstance(XContentParser parser) throws IOException {
        
        return Phase.parse(parser, new Tuple<>(phaseName, registry));
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
    protected Phase mutateInstance(Phase instance) throws IOException {
        String name = instance.getName();
        TimeValue after = instance.getAfter();
        List<LifecycleAction> actions = instance.getActions();
        switch (between(0, 2)) {
        case 0:
            name = name + randomAlphaOfLengthBetween(1, 5);
            break;
        case 1:
            after = TimeValue.timeValueSeconds(after.getSeconds() + randomIntBetween(1, 1000));
            break;
        case 2:
            actions = new ArrayList<>(actions);
            actions.add(new DeleteAction());
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new Phase(name, after, actions);
    }

    public void testExecuteNewIndexCompleteActions() throws Exception {
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
        firstAction.setCompleteOnExecute(true);
        actions.add(firstAction);
        MockAction secondAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "second_action";
            }
        };
        secondAction.setCompleteOnExecute(true);
        actions.add(secondAction);
        MockAction thirdAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "third_action";
            }
        };
        thirdAction.setCompleteOnExecute(true);
        actions.add(thirdAction);
        Phase phase = new Phase(phaseName, after, actions);

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, phaseName, "") {

            @Override
            public boolean canExecute(Phase phase) {
                throw new AssertionError("canExecute should not have been called");
            }
        };

        phase.execute(context);

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
        List<LifecycleAction> actions = new ArrayList<>();
        MockAction firstAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "first_action";
            }
        };
        firstAction.setCompleteOnExecute(false);
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
        actions.add(thirdAction);
        Phase phase = new Phase(phaseName, after, actions);

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, phaseName, "") {

            @Override
            public boolean canExecute(Phase phase) {
                throw new AssertionError("canExecute should not have been called");
            }
        };

        phase.execute(context);

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
        actions.add(thirdAction);
        Phase phase = new Phase(phaseName, after, actions);

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, phaseName, "") {

            @Override
            public boolean canExecute(Phase phase) {
                throw new AssertionError("canExecute should not have been called");
            }
        };

        RuntimeException exception = new RuntimeException();

        context.failOnSetters(exception);
        
        phase.execute(context);

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
        Phase phase = new Phase(phaseName, after, Collections.emptyList());

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, phaseName, "") {

            @Override
            public boolean canExecute(Phase phase) {
                throw new AssertionError("canExecute should not have been called");
            }
        };

        phase.execute(context);

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
        Phase phase = new Phase(phaseName, after, Collections.emptyList());

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, phaseName, Phase.PHASE_COMPLETED) {

            @Override
            public boolean canExecute(Phase phase) {
                throw new AssertionError("canExecute should not have been called");
            }
        };

        phase.execute(context);

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
        List<LifecycleAction> actions = new ArrayList<>();
        MockAction firstAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "first_action";
            }
        };
        firstAction.setCompleteOnExecute(false);
        actions.add(firstAction);
        MockAction secondAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "second_action";
            }
        };
        secondAction.setCompleteOnExecute(false);
        actions.add(secondAction);
        MockAction thirdAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "third_action";
            }
        };
        thirdAction.setCompleteOnExecute(false);
        actions.add(thirdAction);
        Phase phase = new Phase(phaseName, after, actions);

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, phaseName, firstAction.getWriteableName()) {

            @Override
            public boolean canExecute(Phase phase) {
                throw new AssertionError("canExecute should not have been called");
            }
        };

        phase.execute(context);

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

        phase.execute(context);

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

    public void testExecuteSecondAction() throws Exception {
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
        firstAction.setCompleteOnExecute(false);
        actions.add(firstAction);
        MockAction secondAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "second_action";
            }
        };
        secondAction.setCompleteOnExecute(false);
        actions.add(secondAction);
        MockAction thirdAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "third_action";
            }
        };
        thirdAction.setCompleteOnExecute(false);
        actions.add(thirdAction);
        Phase phase = new Phase(phaseName, after, actions);

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, phaseName, secondAction.getWriteableName()) {

            @Override
            public boolean canExecute(Phase phase) {
                throw new AssertionError("canExecute should not have been called");
            }
        };

        phase.execute(context);

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

        phase.execute(context);

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
        List<LifecycleAction> actions = new ArrayList<>();
        MockAction firstAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "first_action";
            }
        };
        firstAction.setCompleteOnExecute(false);
        actions.add(firstAction);
        MockAction secondAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "second_action";
            }
        };
        secondAction.setCompleteOnExecute(false);
        actions.add(secondAction);
        MockAction thirdAction = new MockAction() {
            @Override
            public String getWriteableName() {
                return "third_action";
            }
        };
        thirdAction.setCompleteOnExecute(false);
        actions.add(thirdAction);
        Phase phase = new Phase(phaseName, after, actions);

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, phaseName, thirdAction.getWriteableName()) {

            @Override
            public boolean canExecute(Phase phase) {
                throw new AssertionError("canExecute should not have been called");
            }
        };

        phase.execute(context);

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

        phase.execute(context);

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
        actions.add(thirdAction);
        Phase phase = new Phase(phaseName, after, actions);

        MockIndexLifecycleContext context = new MockIndexLifecycleContext(indexName, phaseName, "does_not_exist") {

            @Override
            public boolean canExecute(Phase phase) {
                throw new AssertionError("canExecute should not have been called");
            }
        };

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> phase.execute(context));
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

}
