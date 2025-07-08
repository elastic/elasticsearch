/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.rule;

import org.elasticsearch.xpack.esql.core.tree.NodeTests.Dummy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class RuleExecutorTests extends AbstractRuleTestCase {

    // Test RuleExecutor implementation
    static class TestRuleExecutor extends RuleExecutor<Dummy> {
        public List<RuleExecutor.Batch<Dummy>> batches = new ArrayList<>();

        @Override
        public List<RuleExecutor.Batch<Dummy>> batches() {
            return batches;
        }
    }

    public void testBasicSyncRuleExecution() {
        TestRuleExecutor executor = new TestRuleExecutor();
        Dummy input = new TestNode("test");

        RuleExecutor.Batch<Dummy> batch = new RuleExecutor.Batch<>("TestBatch", new ConditionalRule("test", "success"));
        executor.batches.add(batch);

        AsyncResult<TestRuleExecutor.ExecutionInfo> result = new AsyncResult<>();
        executor.executeWithInfo(input, result.listener());

        result.assertSuccess();
        assertEquals("success", ((TestNode) result.get().after()).value());
    }

    public void testMultipleRulesInBatch() {
        TestRuleExecutor executor = new TestRuleExecutor();
        Dummy input = new TestNode("start");

        RuleExecutor.Batch<Dummy> batch = new RuleExecutor.Batch<>(
            "TestBatch",
            new ConditionalRule("start", "middle"),
            new ConditionalRule("middle", "end")
        );
        executor.batches.add(batch);

        AsyncResult<TestRuleExecutor.ExecutionInfo> result = new AsyncResult<>();
        executor.executeWithInfo(input, result.listener());

        result.assertSuccess();
        assertEquals("end", ((TestNode) result.get().after()).value());

        // Check transformations - batch runs twice:
        // 1st iteration: both rules apply, 2nd iteration: no changes
        var transformations = result.get().transformations();
        assertThat(transformations.keySet().size(), equalTo(1));

        var batchTransformations = transformations.values().iterator().next();
        assertThat(batchTransformations.size(), equalTo(4)); // 2 rules × 2 iterations
    }

    public void testAsyncRuleExecution() {
        TestRuleExecutor executor = new TestRuleExecutor();
        Dummy input = new TestNode("test");
        CountingAsyncRule asyncRule = new CountingAsyncRule("_async");

        RuleExecutor.Batch<Dummy> batch = new RuleExecutor.Batch<>("AsyncBatch", asyncRule);
        executor.batches.add(batch);

        AsyncResult<TestRuleExecutor.ExecutionInfo> result = new AsyncResult<>();
        executor.executeWithInfo(input, result.listener());

        result.assertSuccess();
        assertEquals("test_async", ((TestNode) result.get().after()).value());
        // Rule is called twice: once in first iteration (applies), once in second iteration (no change)
        assertEquals(2, asyncRule.getCallCount());
    }

    public void testRuleFailure() {
        TestRuleExecutor executor = new TestRuleExecutor();
        Dummy input = new TestNode("test");

        RuleExecutor.Batch<Dummy> batch = new RuleExecutor.Batch<>("FailingBatch", new FailingRule("Test error"));
        executor.batches.add(batch);

        AsyncResult<TestRuleExecutor.ExecutionInfo> result = new AsyncResult<>();
        executor.executeWithInfo(input, result.listener());

        result.assertFailure("Test error");
    }

    public void testRuleExecutionOrder() {
        TestRuleExecutor executor = new TestRuleExecutor();
        Dummy input = new TestNode("A");

        List<String> executionOrder = new ArrayList<>();

        Rule<Dummy, Dummy> rule1 = new Rule.Sync<Dummy, Dummy>() {
            @Override
            public Dummy apply(Dummy node) {
                executionOrder.add("rule1");
                // Only transform A to B, avoid infinite loops
                if ("A".equals(((TestNode) node).value())) {
                    return new TestNode("B", node.children());
                }
                return node;
            }

            @Override
            public String name() {
                return "Rule1";
            }
        };

        Rule<Dummy, Dummy> rule2 = new Rule.Sync<Dummy, Dummy>() {
            @Override
            public Dummy apply(Dummy node) {
                executionOrder.add("rule2");
                // Only transform B to C, avoid infinite loops
                if ("B".equals(((TestNode) node).value())) {
                    return new TestNode("C", node.children());
                }
                return node;
            }

            @Override
            public String name() {
                return "Rule2";
            }
        };

        RuleExecutor.Batch<Dummy> batch = new RuleExecutor.Batch<>("OrderBatch", rule1, rule2);
        executor.batches.add(batch);

        AsyncResult<TestRuleExecutor.ExecutionInfo> result = new AsyncResult<>();
        executor.executeWithInfo(input, result.listener());

        result.assertSuccess();
        assertEquals("C", ((TestNode) result.get().after()).value());
        // Execution order: rule1, rule2 (first iteration), rule1, rule2 (second iteration - no changes)
        assertEquals(Arrays.asList("rule1", "rule2", "rule1", "rule2"), executionOrder);
    }

    public void testNoChangeRule() {
        TestRuleExecutor executor = new TestRuleExecutor();
        Dummy input = new TestNode("test");

        Rule<Dummy, Dummy> noChangeRule = new Rule.Sync<Dummy, Dummy>() {
            @Override
            public Dummy apply(Dummy node) {
                return node; // No change
            }

            @Override
            public String name() {
                return "NoChange";
            }
        };

        RuleExecutor.Batch<Dummy> batch = new RuleExecutor.Batch<>("NoChangeBatch", noChangeRule);
        executor.batches.add(batch);

        AsyncResult<TestRuleExecutor.ExecutionInfo> result = new AsyncResult<>();
        executor.executeWithInfo(input, result.listener());

        result.assertSuccess();
        assertEquals("test", ((TestNode) result.get().after()).value());
        assertEquals(input, result.get().after()); // Same instance since no change

        // Check that transformation was recorded but marked as no change
        var transformations = result.get().transformations();
        assertThat(transformations.keySet().size(), equalTo(1));
        var batchTransformations = transformations.values().iterator().next();
        assertThat(batchTransformations.size(), equalTo(1));
        assertFalse("Should not have changed", batchTransformations.get(0).hasChanged());
    }

    public void testExecuteShortcut() {
        TestRuleExecutor executor = new TestRuleExecutor();
        Dummy input = new TestNode("test");

        RuleExecutor.Batch<Dummy> batch = new RuleExecutor.Batch<>("TestBatch", new ConditionalRule("test", "done"));
        executor.batches.add(batch);

        AsyncResult<Dummy> result = new AsyncResult<>();
        executor.execute(input, result.listener());

        result.assertSuccess();
        assertEquals("done", ((TestNode) result.get()).value());
    }

    public void testMultipleBatches() {
        TestRuleExecutor executor = new TestRuleExecutor();
        Dummy input = new TestNode("start");

        RuleExecutor.Batch<Dummy> batch1 = new RuleExecutor.Batch<>("Batch1", new ConditionalRule("start", "middle"));
        RuleExecutor.Batch<Dummy> batch2 = new RuleExecutor.Batch<>("Batch2", new ConditionalRule("middle", "end"));

        executor.batches.add(batch1);
        executor.batches.add(batch2);

        AsyncResult<TestRuleExecutor.ExecutionInfo> result = new AsyncResult<>();
        executor.executeWithInfo(input, result.listener());

        result.assertSuccess();
        assertEquals("end", ((TestNode) result.get().after()).value());

        // Should have transformations from both batches
        var transformations = result.get().transformations();
        assertThat(transformations.keySet().size(), equalTo(2));
    }

    public void testTransformationTracking() {
        TestRuleExecutor executor = new TestRuleExecutor();
        Dummy input = new TestNode("original");

        RuleExecutor.Batch<Dummy> batch = new RuleExecutor.Batch<>(
            "TrackingBatch",
            new ConditionalRule("original", "modified"),
            new ConditionalRule("modified", "final")
        );
        executor.batches.add(batch);

        AsyncResult<TestRuleExecutor.ExecutionInfo> result = new AsyncResult<>();
        executor.executeWithInfo(input, result.listener());

        result.assertSuccess();

        TestRuleExecutor.ExecutionInfo info = result.get();
        assertEquals("original", ((TestNode) info.before()).value());
        assertEquals("final", ((TestNode) info.after()).value());

        var transformations = info.transformations();
        assertThat(transformations.keySet().size(), equalTo(1));
        var batchTransformations = transformations.values().iterator().next();
        assertThat(batchTransformations.size(), equalTo(4)); // 2 rules × 2 iterations

        // First iteration transformations
        TestRuleExecutor.Transformation first = batchTransformations.get(0);
        assertEquals("ConditionaloriginalTomodified", first.name());
        assertEquals("original", ((TestNode) first.before()).value());
        assertEquals("modified", ((TestNode) first.after()).value());
        assertTrue("Should have changed", first.hasChanged());

        TestRuleExecutor.Transformation second = batchTransformations.get(1);
        assertEquals("ConditionalmodifiedTofinal", second.name());
        assertEquals("modified", ((TestNode) second.before()).value());
        assertEquals("final", ((TestNode) second.after()).value());
        assertTrue("Should have changed", second.hasChanged());

        // Second iteration transformations (no changes)
        TestRuleExecutor.Transformation third = batchTransformations.get(2);
        assertEquals("ConditionaloriginalTomodified", third.name());
        assertFalse("Should not have changed in second iteration", third.hasChanged());

        TestRuleExecutor.Transformation fourth = batchTransformations.get(3);
        assertEquals("ConditionalmodifiedTofinal", fourth.name());
        assertFalse("Should not have changed in second iteration", fourth.hasChanged());
    }
}
