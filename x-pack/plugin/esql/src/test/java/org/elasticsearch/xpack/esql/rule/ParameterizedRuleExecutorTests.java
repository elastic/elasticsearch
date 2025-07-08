/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.rule;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.esql.core.tree.NodeTests.Dummy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class ParameterizedRuleExecutorTests extends AbstractRuleTestCase {

    // Test parameterized executor implementation
    protected static class TestParameterizedRuleExecutor extends ParameterizedRuleExecutor<Dummy, String> {
        public List<RuleExecutor.Batch<Dummy>> batches = new ArrayList<>();

        public TestParameterizedRuleExecutor(String context) {
            super(context);
        }

        @Override
        protected Iterable<RuleExecutor.Batch<Dummy>> batches() {
            return batches;
        }
    }

    protected static class TestContextParameterizedRuleExecutor extends ParameterizedRuleExecutor<Dummy, TestContext> {
        public List<RuleExecutor.Batch<Dummy>> batches = new ArrayList<>();

        public TestContextParameterizedRuleExecutor(TestContext context) {
            super(context);
        }

        @Override
        protected Iterable<RuleExecutor.Batch<Dummy>> batches() {
            return batches;
        }
    }

    public void testBasicParameterizedRuleExecution() {
        TestParameterizedRuleExecutor executor = new TestParameterizedRuleExecutor("param_value");
        Dummy input = new TestNode("test");

        TestParameterizedRule rule = new TestParameterizedRule();
        RuleExecutor.Batch<Dummy> batch = new RuleExecutor.Batch<>("ParamBatch", rule);
        executor.batches.add(batch);

        AsyncResult<TestParameterizedRuleExecutor.ExecutionInfo> result = new AsyncResult<>();
        executor.executeWithInfo(input, result.listener());

        result.assertSuccess();
        assertEquals("test_param_value", ((TestNode) result.get().after()).value());
    }

    public void testParameterizedRuleWithComplexContext() {
        TestContext context = new TestContext("start_", "_end");
        TestContextParameterizedRuleExecutor executor = new TestContextParameterizedRuleExecutor(context);
        Dummy input = new TestNode("middle");

        TestContextParameterizedRule rule = new TestContextParameterizedRule();
        RuleExecutor.Batch<Dummy> batch = new RuleExecutor.Batch<>("ContextBatch", rule);
        executor.batches.add(batch);

        AsyncResult<TestContextParameterizedRuleExecutor.ExecutionInfo> result = new AsyncResult<>();
        executor.executeWithInfo(input, result.listener());

        result.assertSuccess();
        assertEquals("start_middle_end", ((TestNode) result.get().after()).value());
    }

    public void testMultipleParameterizedRulesInBatch() {
        TestParameterizedRuleExecutor executor = new TestParameterizedRuleExecutor("X");
        Dummy input = new TestNode("test");  // Use "test" as trigger for TestParameterizedRule

        ConditionalParameterizedRule rule1 = new ConditionalParameterizedRule("test");
        ConditionalParameterizedRule rule2 = new ConditionalParameterizedRule("test_X");

        RuleExecutor.Batch<Dummy> batch = new RuleExecutor.Batch<>("MultiBatch", rule1, rule2);
        executor.batches.add(batch);

        AsyncResult<TestParameterizedRuleExecutor.ExecutionInfo> result = new AsyncResult<>();
        executor.executeWithInfo(input, result.listener());

        result.assertSuccess();
        assertEquals("test_X_X", ((TestNode) result.get().after()).value());

        // Check transformations - the batch runs twice:
        // 1st iteration: rule1 applies (test -> test_X), then rule2 applies (test_X -> test_X_X)
        // 2nd iteration: no rules apply (no changes), so execution stops
        var transformations = result.get().transformations();
        assertThat(transformations.keySet().size(), equalTo(1));
        var batchTransformations = transformations.values().iterator().next();

        // We should have 4 transformations total: 2 from first iteration, 2 from second (no-change)
        assertThat(batchTransformations.size(), equalTo(4));

        // First iteration transformations
        assertEquals("ConditionalParameterized_test", batchTransformations.get(0).name());
        assertTrue("First rule should have changed", batchTransformations.get(0).hasChanged());
        assertEquals("ConditionalParameterized_test_X", batchTransformations.get(1).name());
        assertTrue("Second rule should have changed", batchTransformations.get(1).hasChanged());

        // Second iteration transformations (no changes)
        assertEquals("ConditionalParameterized_test", batchTransformations.get(2).name());
        assertFalse("First rule should not change in second iteration", batchTransformations.get(2).hasChanged());
        assertEquals("ConditionalParameterized_test_X", batchTransformations.get(3).name());
        assertFalse("Second rule should not change in second iteration", batchTransformations.get(3).hasChanged());
    }

    public void testMixedRulesInParameterizedExecutor() {
        TestParameterizedRuleExecutor executor = new TestParameterizedRuleExecutor("param");
        Dummy input = new TestNode("test");

        // Mix parameterized and non-parameterized rules
        ConditionalRule nonParamRule = new ConditionalRule("test", "test_suffix");

        // Use ConditionalParameterizedRule that triggers on the result of the first rule
        ConditionalParameterizedRule paramRule = new ConditionalParameterizedRule("test_suffix");

        RuleExecutor.Batch<Dummy> batch = new RuleExecutor.Batch<>("MixedBatch", nonParamRule, paramRule);
        executor.batches.add(batch);

        AsyncResult<TestParameterizedRuleExecutor.ExecutionInfo> result = new AsyncResult<>();
        executor.executeWithInfo(input, result.listener());

        result.assertSuccess();
        // Should apply non-parameterized rule first, then parameterized rule
        assertEquals("test_suffix_param", ((TestNode) result.get().after()).value());
    }

    public void testParameterizedRuleFailure() {
        TestParameterizedRuleExecutor executor = new TestParameterizedRuleExecutor("error_param");
        Dummy input = new TestNode("test");

        // Create a failing parameterized rule
        ParameterizedRule<Dummy, Dummy, String> failingRule = new ParameterizedRule.Async<Dummy, Dummy, String>() {
            @Override
            public void apply(Dummy node, String param, ActionListener<Dummy> listener) {
                listener.onFailure(new RuntimeException("Parameterized rule failed with: " + param));
            }

            @Override
            public String name() {
                return "FailingParameterizedRule";
            }
        };

        RuleExecutor.Batch<Dummy> batch = new RuleExecutor.Batch<>("FailingBatch", failingRule);
        executor.batches.add(batch);

        AsyncResult<TestParameterizedRuleExecutor.ExecutionInfo> result = new AsyncResult<>();
        executor.executeWithInfo(input, result.listener());

        result.assertFailure("Parameterized rule failed with: error_param");
    }

    public void testParameterizedRuleExecutionOrder() {
        TestParameterizedRuleExecutor executor = new TestParameterizedRuleExecutor("ORDER");
        Dummy input = new TestNode("start");

        List<String> executionOrder = new ArrayList<>();

        ParameterizedRule<Dummy, Dummy, String> rule1 = new ParameterizedRule.Sync<Dummy, Dummy, String>() {
            @Override
            public Dummy apply(Dummy node, String param) {
                executionOrder.add("rule1_" + param);
                // Only apply to "start" to prevent infinite loops
                if (((TestNode) node).value().equals("start")) {
                    return new TestNode("1", ((TestNode) node).children());
                }
                return node;
            }

            @Override
            public String name() {
                return "ParamRule1";
            }
        };

        ParameterizedRule<Dummy, Dummy, String> rule2 = new ParameterizedRule.Sync<Dummy, Dummy, String>() {
            @Override
            public Dummy apply(Dummy node, String param) {
                executionOrder.add("rule2_" + param);
                // Only apply to "1" to prevent infinite loops
                if (((TestNode) node).value().equals("1")) {
                    return new TestNode("12", ((TestNode) node).children());
                }
                return node;
            }

            @Override
            public String name() {
                return "ParamRule2";
            }
        };

        RuleExecutor.Batch<Dummy> batch = new RuleExecutor.Batch<>("OrderBatch", rule1, rule2);
        executor.batches.add(batch);

        AsyncResult<TestParameterizedRuleExecutor.ExecutionInfo> result = new AsyncResult<>();
        executor.executeWithInfo(input, result.listener());

        result.assertSuccess();
        assertEquals("12", ((TestNode) result.get().after()).value());
        // Execution order: rule1, rule2 (first iteration), rule1, rule2 (second iteration - no changes)
        assertEquals(Arrays.asList("rule1_ORDER", "rule2_ORDER", "rule1_ORDER", "rule2_ORDER"), executionOrder);
    }

    public void testParameterizedRuleNoChange() {
        TestParameterizedRuleExecutor executor = new TestParameterizedRuleExecutor("ignored");
        Dummy input = new TestNode("test");

        ParameterizedRule<Dummy, Dummy, String> noChangeRule = new ParameterizedRule.Sync<Dummy, Dummy, String>() {
            @Override
            public Dummy apply(Dummy node, String param) {
                return node; // No change regardless of parameter
            }

            @Override
            public String name() {
                return "NoChangeParamRule";
            }
        };

        RuleExecutor.Batch<Dummy> batch = new RuleExecutor.Batch<>("NoChangeBatch", noChangeRule);
        executor.batches.add(batch);

        AsyncResult<TestParameterizedRuleExecutor.ExecutionInfo> result = new AsyncResult<>();
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

    public void testParameterizedRuleWithMultipleBatches() {
        TestParameterizedRuleExecutor executor = new TestParameterizedRuleExecutor("middle");
        Dummy input = new TestNode("test");

        // First batch with parameterized rule
        TestParameterizedRule paramRule = new TestParameterizedRule();
        RuleExecutor.Batch<Dummy> batch1 = new RuleExecutor.Batch<>("PrependBatch", paramRule);

        // Second batch with non-parameterized rule that triggers on the result of first batch
        ConditionalRule appendRule = new ConditionalRule("test_middle", "test_middle_final");
        RuleExecutor.Batch<Dummy> batch2 = new RuleExecutor.Batch<>("AppendBatch", appendRule);

        executor.batches.add(batch1);
        executor.batches.add(batch2);

        AsyncResult<TestParameterizedRuleExecutor.ExecutionInfo> result = new AsyncResult<>();
        executor.executeWithInfo(input, result.listener());

        result.assertSuccess();
        assertEquals("test_middle_final", ((TestNode) result.get().after()).value());

        // Should have transformations from both batches
        var transformations = result.get().transformations();
        assertThat(transformations.keySet().size(), equalTo(2));
    }

    public void testParameterizedExecuteShortcut() {
        TestParameterizedRuleExecutor executor = new TestParameterizedRuleExecutor("shortcut");
        Dummy input = new TestNode("test");

        TestParameterizedRule rule = new TestParameterizedRule();
        RuleExecutor.Batch<Dummy> batch = new RuleExecutor.Batch<>("TestBatch", rule);
        executor.batches.add(batch);

        AsyncResult<Dummy> result = new AsyncResult<>();
        executor.execute(input, result.listener());

        result.assertSuccess();
        assertEquals("test_shortcut", ((TestNode) result.get()).value());
    }

    public void testParameterizedAsyncRule() {
        TestParameterizedRuleExecutor executor = new TestParameterizedRuleExecutor("async_param");
        Dummy input = new TestNode("test");

        ParameterizedRule<Dummy, Dummy, String> asyncRule = new ParameterizedRule.Async<Dummy, Dummy, String>() {
            @Override
            public void apply(Dummy node, String param, ActionListener<Dummy> listener) {
                // Only apply to "test" nodes to prevent infinite loops
                if (((TestNode) node).value().equals("test")) {
                    listener.onResponse(new TestNode("async_" + ((TestNode) node).value() + "_" + param, ((TestNode) node).children()));
                } else {
                    listener.onResponse(node);
                }
            }

            @Override
            public String name() {
                return "AsyncParameterizedRule";
            }
        };

        RuleExecutor.Batch<Dummy> batch = new RuleExecutor.Batch<>("AsyncBatch", asyncRule);
        executor.batches.add(batch);

        AsyncResult<TestParameterizedRuleExecutor.ExecutionInfo> result = new AsyncResult<>();
        executor.executeWithInfo(input, result.listener());

        result.assertSuccess();
        assertEquals("async_test_async_param", ((TestNode) result.get().after()).value());
    }

    public void testParameterizedRuleContextAccess() {
        TestParameterizedRuleExecutor executor = new TestParameterizedRuleExecutor("context_value");
        Dummy input = new TestNode("test");

        // Verify that the executor's context is correctly passed to rules
        assertEquals("context_value", executor.context());

        TestParameterizedRule rule = new TestParameterizedRule();
        RuleExecutor.Batch<Dummy> batch = new RuleExecutor.Batch<>("ContextBatch", rule);
        executor.batches.add(batch);

        AsyncResult<TestParameterizedRuleExecutor.ExecutionInfo> result = new AsyncResult<>();
        executor.executeWithInfo(input, result.listener());

        result.assertSuccess();
        assertEquals("test_context_value", ((TestNode) result.get().after()).value());
    }
}
