/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enrich;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.containsString;

public class EnrichPolicyExecutorTests extends ESTestCase {

    private static ThreadPool testThreadPool;
    private static final ActionListener<ExecuteEnrichPolicyAction.Response> noOpListener = new ActionListener<>() {
        @Override
        public void onResponse(ExecuteEnrichPolicyAction.Response ignored) {}

        @Override
        public void onFailure(Exception e) {}
    };

    @BeforeClass
    public static void beforeCLass() {
        testThreadPool = new TestThreadPool("EnrichPolicyExecutorTests");
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(testThreadPool, 30, TimeUnit.SECONDS);
    }

    public void testNonConcurrentPolicyCoordination() throws InterruptedException {
        String testPolicyName = "test_policy";
        CountDownLatch latch = new CountDownLatch(1);
        Client client = getClient(latch);
        final EnrichPolicyExecutor testExecutor = new EnrichPolicyExecutor(
            Settings.EMPTY,
            null,
            client,
            testThreadPool,
            TestIndexNameExpressionResolver.newInstance(testThreadPool.getThreadContext()),
            new EnrichPolicyLocks(),
            ESTestCase::randomNonNegativeLong
        );

        // Launch a fake policy run that will block until firstTaskBlock is counted down.
        final CountDownLatch firstTaskComplete = new CountDownLatch(1);
        testExecutor.coordinatePolicyExecution(
            new ExecuteEnrichPolicyAction.Request(testPolicyName),
            new LatchedActionListener<>(noOpListener, firstTaskComplete)
        );

        // Launch a second fake run that should fail immediately because the lock is obtained.
        EsRejectedExecutionException expected = expectThrows(
            EsRejectedExecutionException.class,
            "Expected exception but nothing was thrown",
            () -> {
                testExecutor.coordinatePolicyExecution(new ExecuteEnrichPolicyAction.Request(testPolicyName), noOpListener);
                // Should throw exception on the previous statement, but if it doesn't, be a
                // good citizen and conclude the fake runs to keep the logs clean from interrupted exceptions
                latch.countDown();
                firstTaskComplete.await();
            }
        );

        // Conclude the first mock run
        latch.countDown();
        firstTaskComplete.await();

        // Validate exception from second run
        assertThat(
            expected.getMessage(),
            containsString("Could not obtain lock because policy execution for [" + testPolicyName + "] is already in progress.")
        );

        // Ensure that the lock from the previous run has been cleared
        CountDownLatch secondTaskComplete = new CountDownLatch(1);
        testExecutor.coordinatePolicyExecution(
            new ExecuteEnrichPolicyAction.Request(testPolicyName),
            new LatchedActionListener<>(noOpListener, secondTaskComplete)
        );
        secondTaskComplete.await();
    }

    public void testMaximumPolicyExecutionLimit() throws InterruptedException {
        String testPolicyBaseName = "test_policy_";
        Settings testSettings = Settings.builder().put(EnrichPlugin.ENRICH_MAX_CONCURRENT_POLICY_EXECUTIONS.getKey(), 2).build();
        CountDownLatch latch = new CountDownLatch(1);
        Client client = getClient(latch);
        final EnrichPolicyExecutor testExecutor = new EnrichPolicyExecutor(
            testSettings,
            null,
            client,
            testThreadPool,
            TestIndexNameExpressionResolver.newInstance(testThreadPool.getThreadContext()),
            new EnrichPolicyLocks(),
            ESTestCase::randomNonNegativeLong
        );

        // Launch a two fake policy runs that will block until counted down to use up the maximum concurrent
        final CountDownLatch firstTaskComplete = new CountDownLatch(1);
        testExecutor.coordinatePolicyExecution(
            new ExecuteEnrichPolicyAction.Request(testPolicyBaseName + "1"),
            new LatchedActionListener<>(noOpListener, firstTaskComplete)
        );

        final CountDownLatch secondTaskComplete = new CountDownLatch(1);
        testExecutor.coordinatePolicyExecution(
            new ExecuteEnrichPolicyAction.Request(testPolicyBaseName + "2"),
            new LatchedActionListener<>(noOpListener, secondTaskComplete)
        );

        // Launch a third fake run that should fail immediately because the lock is obtained.
        EsRejectedExecutionException expected = expectThrows(
            EsRejectedExecutionException.class,
            "Expected exception but nothing was thrown",
            () -> {
                testExecutor.coordinatePolicyExecution(new ExecuteEnrichPolicyAction.Request(testPolicyBaseName + "3"), noOpListener);
                // Should throw exception on the previous statement, but if it doesn't, be a
                // good citizen and conclude the fake runs to keep the logs clean from interrupted exceptions
                latch.countDown();
                firstTaskComplete.await();
                secondTaskComplete.await();
            }
        );

        // Conclude the first mock run
        latch.countDown();
        firstTaskComplete.await();
        secondTaskComplete.await();

        // Validate exception from second run
        assertThat(
            expected.getMessage(),
            containsString(
                "Policy execution failed. Policy execution for [test_policy_3] would exceed " + "maximum concurrent policy executions [2]"
            )
        );

        // Ensure that the lock from the previous run has been cleared
        CountDownLatch finalTaskComplete = new CountDownLatch(1);
        testExecutor.coordinatePolicyExecution(
            new ExecuteEnrichPolicyAction.Request(testPolicyBaseName + "1"),
            new LatchedActionListener<>(noOpListener, finalTaskComplete)
        );
        finalTaskComplete.await();
    }

    private Client getClient(CountDownLatch latch) {
        return new NoOpClient(testThreadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                testThreadPool.generic().execute(() -> {
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    super.doExecute(action, request, listener);
                });
            }
        };
    }
}
