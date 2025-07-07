/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.rule;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
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

    // Test node implementation
    protected static class TestNode extends Node<TestNode> {
        private final String value;
        private final List<TestNode> children;

        public TestNode(String value) {
            this(Source.EMPTY, value, Collections.emptyList());
        }

        public TestNode(String value, List<TestNode> children) {
            this(Source.EMPTY, value, children);
        }

        public TestNode(Source source, String value, List<TestNode> children) {
            super(source, children);
            this.value = value;
            this.children = children;
        }

        public String value() {
            return value;
        }

        @Override
        public TestNode replaceChildren(List<TestNode> newChildren) {
            return new TestNode(source(), value, newChildren);
        }

        @Override
        protected NodeInfo<TestNode> info() {
            return NodeInfo.create(this, TestNode::new, value, children);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            // Not needed for tests
        }

        @Override
        public String getWriteableName() {
            return "test-node";
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if ((obj instanceof TestNode) == false) return false;
            TestNode other = (TestNode) obj;
            return value.equals(other.value) && children.equals(other.children);
        }

        @Override
        public int hashCode() {
            return value.hashCode() * 31 + children.hashCode();
        }

        @Override
        public String toString() {
            return value + (children.isEmpty() ? "" : "(" + children + ")");
        }
    }

    // Test rule implementations
    protected static class AppendRule extends Rule.Sync<TestNode, TestNode> {
        private final String suffix;

        public AppendRule(String suffix) {
            this.suffix = suffix;
        }

        @Override
        public TestNode apply(TestNode node) {
            return new TestNode(node.value() + suffix, node.children());
        }

        @Override
        public String name() {
            return "Append" + suffix;
        }
    }

    protected static class ConditionalRule extends Rule.Sync<TestNode, TestNode> {
        private final String trigger;
        private final String replacement;

        public ConditionalRule(String trigger, String replacement) {
            this.trigger = trigger;
            this.replacement = replacement;
        }

        @Override
        public TestNode apply(TestNode node) {
            if (node.value().equals(trigger)) {
                return new TestNode(replacement, node.children());
            }
            return node; // No change if condition not met
        }

        @Override
        public String name() {
            return "Conditional" + trigger + "To" + replacement;
        }
    }

    protected static class CountingAsyncRule extends Rule.Async<TestNode, TestNode> {
        private final AtomicInteger callCount = new AtomicInteger(0);
        private final String suffix;

        public CountingAsyncRule(String suffix) {
            this.suffix = suffix;
        }

        @Override
        public void apply(TestNode node, ActionListener<TestNode> listener) {
            callCount.incrementAndGet();
            // Simulate async processing
            listener.onResponse(new TestNode(node.value() + suffix, node.children()));
        }

        @Override
        public String name() {
            return "CountingAsync" + suffix;
        }

        public int getCallCount() {
            return callCount.get();
        }
    }

    protected static class FailingRule extends Rule.Async<TestNode, TestNode> {
        private final String errorMessage;

        public FailingRule(String errorMessage) {
            this.errorMessage = errorMessage;
        }

        @Override
        public void apply(TestNode node, ActionListener<TestNode> listener) {
            listener.onFailure(new RuntimeException(errorMessage));
        }

        @Override
        public String name() {
            return "FailingRule";
        }
    }

    protected static class TestParameterizedRule extends ParameterizedRule.Sync<TestNode, TestNode, String> {
        @Override
        public TestNode apply(TestNode node, String param) {
            return new TestNode(node.value() + "_" + param, node.children());
        }

        @Override
        public String name() {
            return "TestParameterizedRule";
        }
    }

    protected static class TestContextParameterizedRule extends ParameterizedRule.Sync<TestNode, TestNode, TestContext> {
        @Override
        public TestNode apply(TestNode node, TestContext context) {
            return new TestNode(context.prefix + node.value() + context.suffix, node.children());
        }

        @Override
        public String name() {
            return "TestContextParameterizedRule";
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
