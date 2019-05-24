/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.enrich;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;

public class EnrichPolicyExecutorTests extends ESTestCase {

    private static ThreadPool testThreadPool;
    private static ActionListener<PolicyExecutionResult> noOpListener = new ActionListener<>() {
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
        testThreadPool = null;
    }

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

    private static class EnrichPolicyTestExecutor extends EnrichPolicyExecutor {

        EnrichPolicyTestExecutor(Settings settings, ClusterService clusterService, Client client, ThreadPool threadPool,
                                 IndexNameExpressionResolver indexNameExpressionResolver, LongSupplier nowSupplier) {
            super(settings, clusterService, client, threadPool, indexNameExpressionResolver, nowSupplier);
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
        EnrichPolicy testPolicy = new EnrichPolicy(EnrichPolicy.EXACT_MATCH_TYPE, null, List.of("some_index"), "keyfield",
            List.of("valuefield"));
        EnrichPolicyTestExecutor testExecutor = new EnrichPolicyTestExecutor(Settings.EMPTY, null, null, testThreadPool,
            new IndexNameExpressionResolver(), ESTestCase::randomNonNegativeLong);

        // Launch a fake policy run that will block until firstTaskBlock is counted down.
        CountDownLatch firstTaskComplete = new CountDownLatch(1);
        CountDownLatch firstTaskBlock = testExecutor.testRunPolicy(testPolicyName, testPolicy,
            new LatchedActionListener<>(noOpListener, firstTaskComplete));

        // Launch a second fake run that should fail immediately because the lock is obtained.
        Exception expected = null;
        try {
            CountDownLatch countDownLatch = testExecutor.testRunPolicy(testPolicyName, testPolicy, noOpListener);
            // Should throw exception on the previous statement, but if it doesn't, be a
            // good citizen and conclude the fake run.
            countDownLatch.countDown();
        } catch (Exception e) {
            expected = e;
        }

        // Conclude the first mock run
        firstTaskBlock.countDown();
        firstTaskComplete.await();

        // Validate exception from second run
        if (expected != null) {
            assertThat(expected, instanceOf(ElasticsearchException.class));
            assertThat(expected.getMessage(), containsString("Policy execution failed. Policy execution for [" + testPolicyName +
                "] is already in progress."));
        } else {
            fail("Expected exception but nothing was thrown");
        }

        // Ensure that the lock from the previous run has been cleared
        CountDownLatch secondTaskComplete = new CountDownLatch(1);
        CountDownLatch secondTaskBlock = testExecutor.testRunPolicy(testPolicyName, testPolicy,
            new LatchedActionListener<>(noOpListener, secondTaskComplete));
        secondTaskBlock.countDown();
        secondTaskComplete.await();
    }
}
