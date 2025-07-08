/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.rule;

import org.elasticsearch.action.ActionListener;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class ParameterizedRuleExecutorTests extends AbstractRuleTestCase {

    // Test ParameterizedRuleExecutor implementation
    static class TestParameterizedRuleExecutor extends ParameterizedRuleExecutor<TestNode, String> {
        public List<RuleExecutor.Batch<TestNode>> batches = new ArrayList<>();

        TestParameterizedRuleExecutor(String context) {
            super(context);
        }

        @Override
        public List<RuleExecutor.Batch<TestNode>> batches() {
            return batches;
        }
    }

    static class TestContextParameterizedRuleExecutor extends ParameterizedRuleExecutor<TestNode, TestContext> {
        public List<RuleExecutor.Batch<TestNode>> batches = new ArrayList<>();

        TestContextParameterizedRuleExecutor(TestContext context) {
            super(context);
        }

        @Override
        public List<RuleExecutor.Batch<TestNode>> batches() {
            return batches;
        }
    }

    public void testBasicParameterizedRuleExecution() {
        TestParameterizedRuleExecutor executor = new TestParameterizedRuleExecutor("param_value");
        TestNode input = new TestNode("test");

        TestParameterizedRule rule = new TestParameterizedRule();
        RuleExecutor.Batch<TestNode> batch = new RuleExecutor.Batch<>("ParamBatch", rule);
        executor.batches.add(batch);

        AsyncResult<TestParameterizedRuleExecutor.ExecutionInfo> result = new AsyncResult<>();
        executor.executeWithInfo(input, result.listener());

        result.assertSuccess();
        assertEquals("test_param_value", result.get().after().value());
    }

    public void testParameterizedRuleWithComplexContext() {
        TestContext context = new TestContext("start_", "_end");
        TestContextParameterizedRuleExecutor executor = new TestContextParameterizedRuleExecutor(context);
        TestNode input = new TestNode("middle");

        TestContextParameterizedRule rule = new TestContextParameterizedRule();
        RuleExecutor.Batch<TestNode> batch = new RuleExecutor.Batch<>("ContextBatch", rule);
        executor.batches.add(batch);

        AsyncResult<TestContextParameterizedRuleExecutor.ExecutionInfo> result = new AsyncResult<>();
        executor.executeWithInfo(input, result.listener());

        result.assertSuccess();
        assertEquals("start_middle_end", result.get().after().value());
    }

    public void testMultipleParameterizedRulesInBatch() {
        TestParameterizedRuleExecutor executor = new TestParameterizedRuleExecutor("X");
        TestNode input = new TestNode("test");  // Use "test" as trigger for TestParameterizedRule

        ConditionalParameterizedRule rule1 = new ConditionalParameterizedRule("test");
        ConditionalParameterizedRule rule2 = new ConditionalParameterizedRule("test_X");

        RuleExecutor.Batch<TestNode> batch = new RuleExecutor.Batch<>("MultiBatch", rule1, rule2);
        executor.batches.add(batch);

        AsyncResult<TestParameterizedRuleExecutor.ExecutionInfo> result = new AsyncResult<>();
        executor.executeWithInfo(input, result.listener());

        result.assertSuccess();
        assertEquals("test_X_X", result.get().after().value());

        // Check transformations
        var transformations = result.get().transformations();
        assertThat(transformations.keySet().size(), equalTo(1));
        var batchTransformations = transformations.values().iterator().next();
        assertThat(batchTransformations.size(), equalTo(2));
        assertEquals("ConditionalParameterized_test", batchTransformations.get(0).name());
        assertEquals("ConditionalParameterized_test_X", batchTransformations.get(1).name());
    }

    public void testMixedRulesInParameterizedExecutor() {
        TestParameterizedRuleExecutor executor = new TestParameterizedRuleExecutor("param");
        TestNode input = new TestNode("test");

        // Mix parameterized and non-parameterized rules
        ConditionalRule nonParamRule = new ConditionalRule("test", "test_suffix");
        TestParameterizedRule paramRule = new TestParameterizedRule();

        RuleExecutor.Batch<TestNode> batch = new RuleExecutor.Batch<>("MixedBatch", nonParamRule, paramRule);
        executor.batches.add(batch);

        AsyncResult<TestParameterizedRuleExecutor.ExecutionInfo> result = new AsyncResult<>();
        executor.executeWithInfo(input, result.listener());

        result.assertSuccess();
        // Should apply non-parameterized rule first, then parameterized rule
        assertEquals("test_suffix_param", result.get().after().value());
    }

    public void testParameterizedRuleFailure() {
        TestParameterizedRuleExecutor executor = new TestParameterizedRuleExecutor("error_param");
        TestNode input = new TestNode("test");

        // Create a failing parameterized rule
        ParameterizedRule<TestNode, TestNode, String> failingRule = new ParameterizedRule.Async<TestNode, TestNode, String>() {
            @Override
            public void apply(TestNode node, String param, ActionListener<TestNode> listener) {
                listener.onFailure(new RuntimeException("Parameterized rule failed with: " + param));
            }

            @Override
            public String name() {
                return "FailingParameterizedRule";
            }
        };

        RuleExecutor.Batch<TestNode> batch = new RuleExecutor.Batch<>("FailingBatch", failingRule);
        executor.batches.add(batch);

        AsyncResult<TestParameterizedRuleExecutor.ExecutionInfo> result = new AsyncResult<>();
        executor.executeWithInfo(input, result.listener());

        result.assertFailure("Parameterized rule failed with: error_param");
    }

    public void testParameterizedRuleExecutionOrder() {
        TestParameterizedRuleExecutor executor = new TestParameterizedRuleExecutor("ORDER");
        TestNode input = new TestNode("");

        List<String> executionOrder = new ArrayList<>();

        ParameterizedRule<TestNode, TestNode, String> rule1 = new ParameterizedRule.Sync<TestNode, TestNode, String>() {
            @Override
            public TestNode apply(TestNode node, String param) {
                executionOrder.add("rule1_" + param);
                return new TestNode(node.value() + "1", node.children());
            }

            @Override
            public String name() {
                return "ParamRule1";
            }
        };

        ParameterizedRule<TestNode, TestNode, String> rule2 = new ParameterizedRule.Sync<TestNode, TestNode, String>() {
            @Override
            public TestNode apply(TestNode node, String param) {
                executionOrder.add("rule2_" + param);
                return new TestNode(node.value() + "2", node.children());
            }

            @Override
            public String name() {
                return "ParamRule2";
            }
        };

        RuleExecutor.Batch<TestNode> batch = new RuleExecutor.Batch<>("OrderBatch", rule1, rule2);
        executor.batches.add(batch);

        AsyncResult<TestParameterizedRuleExecutor.ExecutionInfo> result = new AsyncResult<>();
        executor.executeWithInfo(input, result.listener());

        result.assertSuccess();
        assertEquals("12", result.get().after().value());
        assertEquals(Arrays.asList("rule1_ORDER", "rule2_ORDER"), executionOrder);
    }

    public void testParameterizedRuleNoChange() {
        TestParameterizedRuleExecutor executor = new TestParameterizedRuleExecutor("ignored");
        TestNode input = new TestNode("test");

        ParameterizedRule<TestNode, TestNode, String> noChangeRule = new ParameterizedRule.Sync<TestNode, TestNode, String>() {
            @Override
            public TestNode apply(TestNode node, String param) {
                return node; // No change regardless of parameter
            }

            @Override
            public String name() {
                return "NoChangeParamRule";
            }
        };

        RuleExecutor.Batch<TestNode> batch = new RuleExecutor.Batch<>("NoChangeBatch", noChangeRule);
        executor.batches.add(batch);

        AsyncResult<TestParameterizedRuleExecutor.ExecutionInfo> result = new AsyncResult<>();
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

    public void testParameterizedRuleWithMultipleBatches() {
        TestParameterizedRuleExecutor executor = new TestParameterizedRuleExecutor("middle");
        TestNode input = new TestNode("test");

        // First batch with parameterized rule
        TestParameterizedRule paramRule = new TestParameterizedRule();
        RuleExecutor.Batch<TestNode> batch1 = new RuleExecutor.Batch<>("PrependBatch", paramRule);

        // Second batch with non-parameterized rule that triggers on the result of first batch
        ConditionalRule appendRule = new ConditionalRule("test_middle", "test_middle_final");
        RuleExecutor.Batch<TestNode> batch2 = new RuleExecutor.Batch<>("AppendBatch", appendRule);

        executor.batches.add(batch1);
        executor.batches.add(batch2);

        AsyncResult<TestParameterizedRuleExecutor.ExecutionInfo> result = new AsyncResult<>();
        executor.executeWithInfo(input, result.listener());

        result.assertSuccess();
        assertEquals("test_middle_final", result.get().after().value());

        // Should have transformations from both batches
        var transformations = result.get().transformations();
        assertThat(transformations.keySet().size(), equalTo(2));
    }

    public void testParameterizedExecuteShortcut() {
        TestParameterizedRuleExecutor executor = new TestParameterizedRuleExecutor("shortcut");
        TestNode input = new TestNode("test");

        TestParameterizedRule rule = new TestParameterizedRule();
        RuleExecutor.Batch<TestNode> batch = new RuleExecutor.Batch<>("TestBatch", rule);
        executor.batches.add(batch);

        AsyncResult<TestNode> result = new AsyncResult<>();
        executor.execute(input, result.listener());

        result.assertSuccess();
        assertEquals("test_shortcut", result.get().value());
    }

    public void testParameterizedAsyncRule() {
        TestParameterizedRuleExecutor executor = new TestParameterizedRuleExecutor("async_param");
        TestNode input = new TestNode("test");

        ParameterizedRule<TestNode, TestNode, String> asyncRule = new ParameterizedRule.Async<TestNode, TestNode, String>() {
            @Override
            public void apply(TestNode node, String param, ActionListener<TestNode> listener) {
                // Only apply to "test" nodes to prevent infinite loops
                if (node.value().equals("test")) {
                    listener.onResponse(new TestNode("async_" + node.value() + "_" + param, node.children()));
                } else {
                    listener.onResponse(node);
                }
            }

            @Override
            public String name() {
                return "AsyncParameterizedRule";
            }
        };

        RuleExecutor.Batch<TestNode> batch = new RuleExecutor.Batch<>("AsyncBatch", asyncRule);
        executor.batches.add(batch);

        AsyncResult<TestParameterizedRuleExecutor.ExecutionInfo> result = new AsyncResult<>();
        executor.executeWithInfo(input, result.listener());

        result.assertSuccess();
        assertEquals("async_test_async_param", result.get().after().value());
    }

    public void testParameterizedRuleContextAccess() {
        TestParameterizedRuleExecutor executor = new TestParameterizedRuleExecutor("context_value");
        TestNode input = new TestNode("test");

        // Verify that the executor's context is correctly passed to rules
        assertEquals("context_value", executor.context());

        TestParameterizedRule rule = new TestParameterizedRule();
        RuleExecutor.Batch<TestNode> batch = new RuleExecutor.Batch<>("ContextBatch", rule);
        executor.batches.add(batch);

        AsyncResult<TestParameterizedRuleExecutor.ExecutionInfo> result = new AsyncResult<>();
        executor.executeWithInfo(input, result.listener());

        result.assertSuccess();
        assertEquals("test_context_value", result.get().after().value());
    }
}
