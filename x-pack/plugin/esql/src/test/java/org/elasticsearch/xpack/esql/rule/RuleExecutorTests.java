/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.rule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class RuleExecutorTests extends AbstractRuleTestCase {

    // Test RuleExecutor implementation
    static class TestRuleExecutor extends RuleExecutor<TestNode> {
        public List<RuleExecutor.Batch<TestNode>> batches = new ArrayList<>();

        @Override
        public List<RuleExecutor.Batch<TestNode>> batches() {
            return batches;
        }
    }

    public void testBasicSyncRuleExecution() {
        TestRuleExecutor executor = new TestRuleExecutor();
        TestNode input = new TestNode("test");

        RuleExecutor.Batch<TestNode> batch = new RuleExecutor.Batch<>("TestBatch", new ConditionalRule("test", "success"));
        executor.batches.add(batch);

        AsyncResult<TestRuleExecutor.ExecutionInfo> result = new AsyncResult<>();
        executor.executeWithInfo(input, result.listener());

        result.assertSuccess();
        assertEquals("success", result.get().after().value());
    }

    public void testMultipleRulesInBatch() {
        TestRuleExecutor executor = new TestRuleExecutor();
        TestNode input = new TestNode("start");

        RuleExecutor.Batch<TestNode> batch = new RuleExecutor.Batch<>(
            "TestBatch",
            new ConditionalRule("start", "middle"),
            new ConditionalRule("middle", "end")
        );
        executor.batches.add(batch);

        AsyncResult<TestRuleExecutor.ExecutionInfo> result = new AsyncResult<>();
        executor.executeWithInfo(input, result.listener());

        result.assertSuccess();
        assertEquals("end", result.get().after().value());

        // Check transformations
        var transformations = result.get().transformations();
        assertThat(transformations.keySet().size(), equalTo(1));
        var batchTransformations = transformations.values().iterator().next();
        assertThat(batchTransformations.size(), equalTo(2));
        assertEquals("ConditionalstartTomiddle", batchTransformations.get(0).name());
        assertEquals("ConditionalmiddleToend", batchTransformations.get(1).name());
    }

    public void testAsyncRuleExecution() {
        TestRuleExecutor executor = new TestRuleExecutor();
        TestNode input = new TestNode("test");
        CountingAsyncRule asyncRule = new CountingAsyncRule("_async");

        RuleExecutor.Batch<TestNode> batch = new RuleExecutor.Batch<>("AsyncBatch", asyncRule);
        executor.batches.add(batch);

        AsyncResult<TestRuleExecutor.ExecutionInfo> result = new AsyncResult<>();
        executor.executeWithInfo(input, result.listener());

        result.assertSuccess();
        assertEquals("test_async", result.get().after().value());
        assertEquals(1, asyncRule.getCallCount());
    }

    public void testRuleFailure() {
        TestRuleExecutor executor = new TestRuleExecutor();
        TestNode input = new TestNode("test");

        RuleExecutor.Batch<TestNode> batch = new RuleExecutor.Batch<>("FailingBatch", new FailingRule("Test error"));
        executor.batches.add(batch);

        AsyncResult<TestRuleExecutor.ExecutionInfo> result = new AsyncResult<>();
        executor.executeWithInfo(input, result.listener());

        result.assertFailure("Test error");
    }

    public void testRuleExecutionOrder() {
        TestRuleExecutor executor = new TestRuleExecutor();
        TestNode input = new TestNode("A");

        List<String> executionOrder = new ArrayList<>();

        Rule<TestNode, TestNode> rule1 = new Rule.Sync<TestNode, TestNode>() {
            @Override
            public TestNode apply(TestNode node) {
                executionOrder.add("rule1");
                // Only transform A to B, avoid infinite loops
                if ("A".equals(node.value())) {
                    return new TestNode("B", node.children());
                }
                return node;
            }

            @Override
            public String name() {
                return "Rule1";
            }
        };

        Rule<TestNode, TestNode> rule2 = new Rule.Sync<TestNode, TestNode>() {
            @Override
            public TestNode apply(TestNode node) {
                executionOrder.add("rule2");
                // Only transform B to C, avoid infinite loops
                if ("B".equals(node.value())) {
                    return new TestNode("C", node.children());
                }
                return node;
            }

            @Override
            public String name() {
                return "Rule2";
            }
        };

        RuleExecutor.Batch<TestNode> batch = new RuleExecutor.Batch<>("OrderBatch", rule1, rule2);
        executor.batches.add(batch);

        AsyncResult<TestRuleExecutor.ExecutionInfo> result = new AsyncResult<>();
        executor.executeWithInfo(input, result.listener());

        result.assertSuccess();
        assertEquals("C", result.get().after().value());
        assertEquals(Arrays.asList("rule1", "rule2"), executionOrder);
    }

    public void testNoChangeRule() {
        TestRuleExecutor executor = new TestRuleExecutor();
        TestNode input = new TestNode("test");

        Rule<TestNode, TestNode> noChangeRule = new Rule.Sync<TestNode, TestNode>() {
            @Override
            public TestNode apply(TestNode node) {
                return node; // No change
            }

            @Override
            public String name() {
                return "NoChange";
            }
        };

        RuleExecutor.Batch<TestNode> batch = new RuleExecutor.Batch<>("NoChangeBatch", noChangeRule);
        executor.batches.add(batch);

        AsyncResult<TestRuleExecutor.ExecutionInfo> result = new AsyncResult<>();
        executor.executeWithInfo(input, result.listener());

        result.assertSuccess();
        assertEquals("test", result.get().after().value());
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
        TestNode input = new TestNode("test");

        RuleExecutor.Batch<TestNode> batch = new RuleExecutor.Batch<>("TestBatch", new ConditionalRule("test", "done"));
        executor.batches.add(batch);

        AsyncResult<TestNode> result = new AsyncResult<>();
        executor.execute(input, result.listener());

        result.assertSuccess();
        assertEquals("done", result.get().value());
    }

    public void testMultipleBatches() {
        TestRuleExecutor executor = new TestRuleExecutor();
        TestNode input = new TestNode("start");

        RuleExecutor.Batch<TestNode> batch1 = new RuleExecutor.Batch<>("Batch1", new ConditionalRule("start", "middle"));
        RuleExecutor.Batch<TestNode> batch2 = new RuleExecutor.Batch<>("Batch2", new ConditionalRule("middle", "end"));

        executor.batches.add(batch1);
        executor.batches.add(batch2);

        AsyncResult<TestRuleExecutor.ExecutionInfo> result = new AsyncResult<>();
        executor.executeWithInfo(input, result.listener());

        result.assertSuccess();
        assertEquals("end", result.get().after().value());

        // Should have transformations from both batches
        var transformations = result.get().transformations();
        assertThat(transformations.keySet().size(), equalTo(2));
    }

    public void testTransformationTracking() {
        TestRuleExecutor executor = new TestRuleExecutor();
        TestNode input = new TestNode("original");

        RuleExecutor.Batch<TestNode> batch = new RuleExecutor.Batch<>(
            "TrackingBatch",
            new ConditionalRule("original", "modified"),
            new ConditionalRule("modified", "final")
        );
        executor.batches.add(batch);

        AsyncResult<TestRuleExecutor.ExecutionInfo> result = new AsyncResult<>();
        executor.executeWithInfo(input, result.listener());

        result.assertSuccess();

        TestRuleExecutor.ExecutionInfo info = result.get();
        assertEquals("original", info.before().value());
        assertEquals("final", info.after().value());

        var transformations = info.transformations();
        assertThat(transformations.keySet().size(), equalTo(1));
        var batchTransformations = transformations.values().iterator().next();
        assertThat(batchTransformations.size(), equalTo(2));

        TestRuleExecutor.Transformation first = batchTransformations.get(0);
        assertEquals("ConditionaloriginalTomodified", first.name());
        assertEquals("original", first.before().value());
        assertEquals("modified", first.after().value());
        assertTrue("Should have changed", first.hasChanged());

        TestRuleExecutor.Transformation second = batchTransformations.get(1);
        assertEquals("ConditionalmodifiedTofinal", second.name());
        assertEquals("modified", second.before().value());
        assertEquals("final", second.after().value());
        assertTrue("Should have changed", second.hasChanged());
    }
}
