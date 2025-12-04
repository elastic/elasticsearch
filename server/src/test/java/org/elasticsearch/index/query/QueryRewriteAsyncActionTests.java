/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class QueryRewriteAsyncActionTests extends ESTestCase {
    public void testRewrite() throws IOException, InterruptedException {
        QueryRewriteContext ctx = new QueryRewriteContext(null, null, null);

        TestRewritable testRewritable = new TestRewritable(false, false);

        assertEquals(0, testRewritable.execCounter().intValue());

        TestRewritable rewritten = Rewriteable.rewrite(testRewritable, ctx);

        assertEquals(testRewritable, rewritten);

        CountDownLatch latch = new CountDownLatch(1);
        ctx.executeAsyncActions(new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        });

        latch.await();

        // we check the number of actions that were executed
        assertEquals(rewritten.numberOfAsyncActions(), testRewritable.execCounter().intValue());
        // we check that for each action, all consumers were executed
        assertEquals(rewritten.actualAsyncResults(), rewritten.expectedAsyncResults());
    }

    public void testRewriteWithConsumerFailure() throws IOException, InterruptedException {
        TestRewritable testRewritable = new TestRewritable(true, false);
        checkRewriteFailure(testRewritable, "it's a trap!");
    }

    public void testRewriteWithActionFailure() throws IOException, InterruptedException {
        TestRewritable testRewritable = new TestRewritable(false, true);
        checkRewriteFailure(testRewritable, "Mesa no tink so");
    }

    private void checkRewriteFailure(TestRewritable testRewritable, String expectedError) throws IOException, InterruptedException {
        QueryRewriteContext ctx = new QueryRewriteContext(null, null, null);

        TestRewritable rewritten = Rewriteable.rewrite(testRewritable, ctx);

        assertEquals(testRewritable, rewritten);
        CountDownLatch latch = new CountDownLatch(1);

        SetOnce<Exception> listenerException = new SetOnce<>();
        ctx.executeAsyncActions(new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                throw new AssertionError("Should not be called");
            }

            @Override
            public void onFailure(Exception e) {
                latch.countDown();
                listenerException.set(e);
            }
        });

        latch.await();

        assertTrue(listenerException.get() instanceof IllegalStateException);
        assertEquals(expectedError, listenerException.get().getMessage());
    }

    public static final class TestQueryRewriteAsyncAction extends QueryRewriteAsyncAction<Integer> {

        private final Integer label;
        private final SetOnce<Boolean> hasRun = new SetOnce<>();
        private final AtomicInteger execCounter;
        private final boolean shouldFail;

        public TestQueryRewriteAsyncAction(QueryRewriteContext context, Integer label, AtomicInteger execCounter, boolean shouldFail) {
            super(context);
            this.label = label;
            this.execCounter = execCounter;
            this.shouldFail = shouldFail;
        }

        @Override
        protected void execute(Client client, ActionListener<Integer> listener) {
            // we expect that an action is executed only once
            hasRun.set(true);
            // we increment the global counter of how many actions were executed
            execCounter.incrementAndGet();
            if (shouldFail) {
                listener.onFailure(new IllegalStateException("Mesa no tink so"));
            } else {
                listener.onResponse(label);
            }
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(label);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof TestQueryRewriteAsyncAction == false) {
                return false;
            }

            TestQueryRewriteAsyncAction other = (TestQueryRewriteAsyncAction) obj;
            return Objects.equals(label, other.label);
        }
    }

    public static final class TestRewritable implements Rewriteable<TestRewritable> {

        private final Map<Integer, Set<Integer>> actualAsyncResults = new HashMap<>();
        private final Map<Integer, Set<Integer>> expectedAsyncResults = new HashMap<>();
        private final int numberOfAsyncActions = randomIntBetween(1, 10);
        private final AtomicInteger execCounter = new AtomicInteger();
        private final boolean failConsumer;
        private final boolean failAction;

        TestRewritable(boolean failConsumer, boolean failAction) {
            this.failConsumer = failConsumer;
            this.failAction = failAction;
        }

        @Override
        public TestRewritable rewrite(QueryRewriteContext ctx) throws IOException {
            if (actualAsyncResults.isEmpty() == false) {
                return this;
            }

            // pick a random step where either the consumer or the action will fail
            int failedActionStep = randomIntBetween(0, numberOfAsyncActions - 1);

            // we generate a random number of async actions
            // for each action, we generate a random number of consumers
            // when a consumer is executed, it adds its label to actualAsyncResults
            IntStream.range(0, numberOfAsyncActions).forEach(actionLabel -> {
                // generate labels for consumers
                Set<Integer> consumerLabels = new HashSet<>();
                IntStream.range(0, randomIntBetween(1, 10)).forEach(j -> consumerLabels.add(randomInt()));
                expectedAsyncResults.put(actionLabel, consumerLabels);

                // pick a random consumer that we should fail if needed
                int failedConsumerLabel = randomFrom(consumerLabels);

                consumerLabels.forEach(consumerLabel -> {
                    SetOnce<Boolean> hasRun = new SetOnce<>();
                    ctx.registerUniqueRewriteAction(
                        // we register the same action multiple times
                        new TestQueryRewriteAsyncAction(ctx, actionLabel, execCounter, failAction && failedActionStep == actionLabel),
                        (result) -> {
                            // a consumer is executed only once
                            hasRun.set(true);

                            if (failConsumer && failedActionStep == actionLabel && failedConsumerLabel == consumerLabel) {
                                throw new IllegalStateException("it's a trap!");
                            }

                            // when a consumer is executed, we add its label to the results map
                            actualAsyncResults.computeIfAbsent(result, k -> new HashSet<>()).add(consumerLabel);
                        }
                    );
                });
            });
            return this;
        }

        public Map<Integer, Set<Integer>> actualAsyncResults() {
            return actualAsyncResults;
        }

        public Map<Integer, Set<Integer>> expectedAsyncResults() {
            return expectedAsyncResults;
        }

        public int numberOfAsyncActions() {
            return numberOfAsyncActions;
        }

        public AtomicInteger execCounter() {
            return execCounter;
        }
    }
}
