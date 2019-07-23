/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.enrich;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.hamcrest.CoreMatchers.containsString;

public class EnrichPolicyExecutorTests extends ESTestCase {

    private static ThreadPool testThreadPool;
    private static final ActionListener<PolicyExecutionResult> noOpListener = new ActionListener<PolicyExecutionResult>() {
        @Override
        public void onResponse(PolicyExecutionResult policyExecutionResult) { }

        @Override
        public void onFailure(Exception e) { }
    };

    @BeforeClass
    public static void beforeCLass() {
        testThreadPool = new TestThreadPool("EnrichPolicyExecutorTests");
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(testThreadPool, 30, TimeUnit.SECONDS);
    }

    /**
     * A policy runner drop-in replacement that just waits on a given countdown latch, and reports success after the latch is counted down.
     */
    private static class BlockingTestPolicyRunner implements Runnable {
        private final CountDownLatch latch;
        private final ActionListener<PolicyExecutionResult> listener;

        BlockingTestPolicyRunner(CountDownLatch latch, ActionListener<PolicyExecutionResult> listener) {
            this.latch = latch;
            this.listener = listener;
        }

        @Override
        public void run() {
            try {
                latch.await();
                listener.onResponse(new PolicyExecutionResult(true));
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted waiting for test framework to continue the test", e);
            }
        }
    }

    /**
     * A mocked policy executor that accepts policy execution requests which block until the returned latch is decremented. Allows for
     * controlling the timing for "in flight" policy executions to test for correct locking logic.
     */
    private static class EnrichPolicyTestExecutor extends EnrichPolicyExecutor {

        EnrichPolicyTestExecutor(Settings settings, ClusterService clusterService, Client client, ThreadPool threadPool,
                                 IndexNameExpressionResolver indexNameExpressionResolver, LongSupplier nowSupplier) {
            super(settings, clusterService, client, threadPool, indexNameExpressionResolver, new EnrichPolicyLocks(), nowSupplier);
        }

        private CountDownLatch currentLatch;
        CountDownLatch testRunPolicy(String policyName, EnrichPolicy policy, ActionListener<PolicyExecutionResult> listener) {
            currentLatch = new CountDownLatch(1);
            runPolicy(policyName, policy, listener);
            return currentLatch;
        }

        @Override
        protected Runnable createPolicyRunner(String policyName, EnrichPolicy policy, ActionListener<PolicyExecutionResult> listener) {
            if (currentLatch == null) {
                throw new IllegalStateException("Use the testRunPolicy method on this test instance");
            }
            return new BlockingTestPolicyRunner(currentLatch, listener);
        }
    }

    public void testNonConcurrentPolicyExecution() throws InterruptedException {
        String testPolicyName = "test_policy";
        EnrichPolicy testPolicy = new EnrichPolicy(EnrichPolicy.EXACT_MATCH_TYPE, null, Arrays.asList("some_index"), "keyfield",
            Collections.singletonList("valuefield"));
        final EnrichPolicyTestExecutor testExecutor = new EnrichPolicyTestExecutor(Settings.EMPTY, null, null, testThreadPool,
            new IndexNameExpressionResolver(), ESTestCase::randomNonNegativeLong);

        // Launch a fake policy run that will block until firstTaskBlock is counted down.
        final CountDownLatch firstTaskComplete = new CountDownLatch(1);
        final CountDownLatch firstTaskBlock = testExecutor.testRunPolicy(testPolicyName, testPolicy,
            new LatchedActionListener<>(noOpListener, firstTaskComplete));

        // Launch a second fake run that should fail immediately because the lock is obtained.
        EsRejectedExecutionException expected = expectThrows(EsRejectedExecutionException.class,
            "Expected exception but nothing was thrown", () -> {
            CountDownLatch countDownLatch = testExecutor.testRunPolicy(testPolicyName, testPolicy, noOpListener);
            // Should throw exception on the previous statement, but if it doesn't, be a
            // good citizen and conclude the fake runs to keep the logs clean from interrupted exceptions
            countDownLatch.countDown();
            firstTaskBlock.countDown();
            firstTaskComplete.await();
        });

        // Conclude the first mock run
        firstTaskBlock.countDown();
        firstTaskComplete.await();

        // Validate exception from second run
        assertThat(expected.getMessage(), containsString("Policy execution failed. Policy execution for [" + testPolicyName +
            "] is already in progress."));

        // Ensure that the lock from the previous run has been cleared
        CountDownLatch secondTaskComplete = new CountDownLatch(1);
        CountDownLatch secondTaskBlock = testExecutor.testRunPolicy(testPolicyName, testPolicy,
            new LatchedActionListener<>(noOpListener, secondTaskComplete));
        secondTaskBlock.countDown();
        secondTaskComplete.await();
    }

    public void testMaximumPolicyExecutionLimit() throws InterruptedException {
        String testPolicyBaseName = "test_policy_";
        Settings testSettings = Settings.builder().put(EnrichPlugin.ENRICH_MAX_CONCURRENT_POLICY_EXECUTIONS.getKey(), 2).build();
        EnrichPolicy testPolicy = new EnrichPolicy(EnrichPolicy.EXACT_MATCH_TYPE, null, Collections.singletonList("some_index"), "keyfield",
            Collections.singletonList("valuefield"));
        final EnrichPolicyTestExecutor testExecutor = new EnrichPolicyTestExecutor(testSettings, null, null, testThreadPool,
            new IndexNameExpressionResolver(), ESTestCase::randomNonNegativeLong);

        // Launch a two fake policy runs that will block until counted down to use up the maximum concurrent
        final CountDownLatch firstTaskComplete = new CountDownLatch(1);
        final CountDownLatch firstTaskBlock = testExecutor.testRunPolicy(testPolicyBaseName + "1", testPolicy,
            new LatchedActionListener<>(noOpListener, firstTaskComplete));

        final CountDownLatch secondTaskComplete = new CountDownLatch(1);
        final CountDownLatch secondTaskBlock = testExecutor.testRunPolicy(testPolicyBaseName + "2", testPolicy,
            new LatchedActionListener<>(noOpListener, secondTaskComplete));

        // Launch a third fake run that should fail immediately because the lock is obtained.
        EsRejectedExecutionException expected = expectThrows(EsRejectedExecutionException.class,
            "Expected exception but nothing was thrown", () -> {
                CountDownLatch countDownLatch = testExecutor.testRunPolicy(testPolicyBaseName + "3", testPolicy, noOpListener);
                // Should throw exception on the previous statement, but if it doesn't, be a
                // good citizen and conclude the fake runs to keep the logs clean from interrupted exceptions
                countDownLatch.countDown();
                firstTaskBlock.countDown();
                secondTaskBlock.countDown();
                firstTaskComplete.await();
                secondTaskComplete.await();
            });

        // Conclude the first mock run
        firstTaskBlock.countDown();
        secondTaskBlock.countDown();
        firstTaskComplete.await();
        secondTaskComplete.await();

        // Validate exception from second run
        assertThat(expected.getMessage(), containsString("Policy execution failed. Policy execution for [test_policy_3] would exceed " +
            "maximum concurrent policy executions [2]"));

        // Ensure that the lock from the previous run has been cleared
        CountDownLatch finalTaskComplete = new CountDownLatch(1);
        CountDownLatch finalTaskBlock = testExecutor.testRunPolicy(testPolicyBaseName + "1", testPolicy,
            new LatchedActionListener<>(noOpListener, finalTaskComplete));
        finalTaskBlock.countDown();
        finalTaskComplete.await();
    }
}
