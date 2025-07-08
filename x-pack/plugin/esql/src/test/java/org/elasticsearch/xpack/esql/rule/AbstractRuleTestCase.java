/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.rule;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeTests.Dummy;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;

/**
 * Abstract base class for rule execution tests providing common test infrastructure.
 */
public abstract class AbstractRuleTestCase extends ESTestCase {

    // Test node implementation extending Dummy to avoid EsqlNodeSubclassTests scanning
    protected static class TestNode extends Dummy {
        protected TestNode(String value) {
            this(Source.EMPTY, Collections.emptyList(), value);
        }

        protected TestNode(String value, List<Dummy> children) {
            this(Source.EMPTY, children, value);
        }

        protected TestNode(Source source, List<Dummy> children, String value) {
            super(source, children, value);
        }

        public String value() {
            return thing(); // Delegate to Dummy's thing() method
        }

        @Override
        protected NodeInfo<TestNode> info() {
            return NodeInfo.create(this, TestNode::new, children(), value());
        }

        @Override
        public TestNode replaceChildren(List<Dummy> newChildren) {
            return new TestNode(source(), newChildren, value());
        }

        @Override
        public String toString() {
            return value() + (children().isEmpty() ? "" : "(" + children() + ")");
        }
    }

    // Test rule implementations
    protected static class AppendRule extends Rule.Sync<Dummy, Dummy> {
        private final String suffix;

        public AppendRule(String suffix) {
            this.suffix = suffix;
        }

        @Override
        public Dummy apply(Dummy node) {
            return new TestNode(((TestNode) node).value() + suffix, node.children());
        }

        @Override
        public String name() {
            return "Append" + suffix;
        }
    }

    protected static class ConditionalRule extends Rule.Sync<Dummy, Dummy> {
        private final String trigger;
        private final String replacement;

        public ConditionalRule(String trigger, String replacement) {
            this.trigger = trigger;
            this.replacement = replacement;
        }

        @Override
        public Dummy apply(Dummy node) {
            if (((TestNode) node).value().equals(trigger)) {
                return new TestNode(replacement, node.children());
            }
            return node; // No change if condition not met
        }

        @Override
        public String name() {
            return "Conditional" + trigger + "To" + replacement;
        }
    }

    protected static class CountingAsyncRule extends Rule.Async<Dummy, Dummy> {
        private final AtomicInteger callCount = new AtomicInteger(0);
        private final String suffix;

        public CountingAsyncRule(String suffix) {
            this.suffix = suffix;
        }

        @Override
        public void apply(Dummy node, ActionListener<Dummy> listener) {
            callCount.incrementAndGet();
            // Only apply to "test" nodes to prevent infinite loops
            if (((TestNode) node).value().equals("test")) {
                listener.onResponse(new TestNode(((TestNode) node).value() + suffix, node.children()));
            } else {
                listener.onResponse(node);
            }
        }

        @Override
        public String name() {
            return "CountingAsync" + suffix;
        }

        public int getCallCount() {
            return callCount.get();
        }
    }

    protected static class FailingRule extends Rule.Async<Dummy, Dummy> {
        private final String errorMessage;

        public FailingRule(String errorMessage) {
            this.errorMessage = errorMessage;
        }

        @Override
        public void apply(Dummy node, ActionListener<Dummy> listener) {
            listener.onFailure(new RuntimeException(errorMessage));
        }

        @Override
        public String name() {
            return "FailingRule";
        }
    }

    protected static class TestParameterizedRule extends ParameterizedRule.Sync<Dummy, Dummy, String> {
        @Override
        public Dummy apply(Dummy node, String param) {
            // Only apply to specific values to prevent infinite loops
            if (((TestNode) node).value().equals("test")) {
                return new TestNode(((TestNode) node).value() + "_" + param, node.children());
            }
            return node;
        }

        @Override
        public String name() {
            return "TestParameterizedRule";
        }
    }

    protected static class TestContextParameterizedRule extends ParameterizedRule.Sync<Dummy, Dummy, TestContext> {
        @Override
        public Dummy apply(Dummy node, TestContext context) {
            // Only apply to specific values to prevent infinite loops
            if (((TestNode) node).value().equals("middle")) {
                return new TestNode(context.prefix + ((TestNode) node).value() + context.suffix, node.children());
            }
            return node;
        }

        @Override
        public String name() {
            return "TestContextParameterizedRule";
        }
    }

    protected static class ConditionalParameterizedRule extends ParameterizedRule.Sync<Dummy, Dummy, String> {
        private final String trigger;

        public ConditionalParameterizedRule(String trigger) {
            this.trigger = trigger;
        }

        @Override
        public Dummy apply(Dummy node, String param) {
            if (((TestNode) node).value().equals(trigger)) {
                return new TestNode(((TestNode) node).value() + "_" + param, node.children());
            }
            return node;
        }

        @Override
        public String name() {
            return "ConditionalParameterized_" + trigger;
        }
    }

    // Test context class
    protected static class TestContext {
        public final String prefix;
        public final String suffix;

        public TestContext(String prefix, String suffix) {
            this.prefix = prefix;
            this.suffix = suffix;
        }

        @Override
        public String toString() {
            return prefix + "..." + suffix;
        }
    }

    // Helper methods for async testing
    protected static class AsyncResult<T> {
        private final CountDownLatch latch = new CountDownLatch(1);
        private final AtomicReference<T> result = new AtomicReference<>();
        private final AtomicReference<Throwable> error = new AtomicReference<>();

        public ActionListener<T> listener() {
            return ActionListener.wrap(res -> {
                result.set(res);
                latch.countDown();
            }, err -> {
                error.set(err);
                latch.countDown();
            });
        }

        public boolean await() throws InterruptedException {
            return latch.await(5, TimeUnit.SECONDS);
        }

        public T get() {
            return result.get();
        }

        public Throwable getError() {
            return error.get();
        }

        public void assertSuccess() {
            try {
                assertTrue("Execution should complete within timeout", await());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            assertNull("Should not have error: " + getError(), getError());
            assertNotNull("Should have result", get());
        }

        public void assertFailure() {
            try {
                assertTrue("Execution should complete within timeout", await());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            assertNotNull("Should have error", getError());
            assertNull("Should not have result", get());
        }

        public void assertFailure(String expectedMessage) {
            assertFailure();
            assertThat(getError().getMessage(), containsString(expectedMessage));
        }
    }

    // Utility method to await async results
    protected static boolean await(CountDownLatch latch) {
        try {
            return latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
