/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enrich;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ElasticsearchTimeoutException;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.get.TransportGetTaskAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.enrich.EnrichMetadata;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.action.InternalExecutePolicyAction;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.empty;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EnrichPolicyExecutorTests extends ESTestCase {

    private static ThreadPool testThreadPool;

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
            new ExecuteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, testPolicyName),
            new LatchedActionListener<>(ActionListener.noop(), firstTaskComplete)
        );

        // Launch a second fake run that should fail immediately because the lock is obtained.
        EsRejectedExecutionException expected = expectThrows(
            EsRejectedExecutionException.class,
            "Expected exception but nothing was thrown",
            () -> {
                testExecutor.coordinatePolicyExecution(
                    new ExecuteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, testPolicyName),
                    ActionListener.noop()
                );
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
            new ExecuteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, testPolicyName),
            new LatchedActionListener<>(ActionListener.noop(), secondTaskComplete)
        );
        secondTaskComplete.await();
    }

    public void testMaximumPolicyExecutionLimit() throws InterruptedException {
        String testPolicyBaseName = "test_policy_";
        Settings testSettings = Settings.builder().put(EnrichPlugin.ENRICH_MAX_CONCURRENT_POLICY_EXECUTIONS.getKey(), 2).build();
        CountDownLatch latch = new CountDownLatch(1);
        Client client = getClient(latch);
        EnrichPolicyLocks locks = new EnrichPolicyLocks();
        final EnrichPolicyExecutor testExecutor = new EnrichPolicyExecutor(
            testSettings,
            null,
            null,
            client,
            testThreadPool,
            TestIndexNameExpressionResolver.newInstance(testThreadPool.getThreadContext()),
            locks,
            ESTestCase::randomNonNegativeLong
        );

        // Launch a two fake policy runs that will block until counted down to use up the maximum concurrent
        final CountDownLatch firstTaskComplete = new CountDownLatch(1);
        testExecutor.coordinatePolicyExecution(
            new ExecuteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, testPolicyBaseName + "1"),
            new LatchedActionListener<>(ActionListener.noop(), firstTaskComplete)
        );

        final CountDownLatch secondTaskComplete = new CountDownLatch(1);
        testExecutor.coordinatePolicyExecution(
            new ExecuteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, testPolicyBaseName + "2"),
            new LatchedActionListener<>(ActionListener.noop(), secondTaskComplete)
        );

        // Launch a third fake run that should fail immediately because the lock is obtained.
        EsRejectedExecutionException expected = expectThrows(
            EsRejectedExecutionException.class,
            "Expected exception but nothing was thrown",
            () -> {
                testExecutor.coordinatePolicyExecution(
                    new ExecuteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, testPolicyBaseName + "3"),
                    ActionListener.noop()
                );
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
        assertThat(locks.lockedPolices(), is(empty()));
        CountDownLatch finalTaskComplete = new CountDownLatch(1);
        testExecutor.coordinatePolicyExecution(
            new ExecuteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, testPolicyBaseName + "1"),
            new LatchedActionListener<>(ActionListener.noop(), finalTaskComplete)
        );
        finalTaskComplete.await();
    }

    public void testWaitForCompletionConditionRemainsLocked() throws Exception {
        String testPolicyName = "test_policy";
        String testTaskId = randomAlphaOfLength(10) + ":" + randomIntBetween(100, 300);
        boolean completeWithResourceNotFound = randomBoolean();

        // Client calls are forked to a different thread which will await on this latch before actually running anything
        CountDownLatch clientBlockingLatch = new CountDownLatch(1);
        // When the client is called with a GetTask call a second time, it should count down this latch, so we can check the lock status.
        CountDownLatch secondGetTaskWasCalled = new CountDownLatch(1);
        // A barrier to repeatedly control when the async client will respond with Get Task API results.
        CyclicBarrier getTaskActionBlockingBarrier = new CyclicBarrier(2);
        // State flag to ensure first Get Task API call will fail.
        AtomicBoolean shouldGetTaskApiReturnTimeout = new AtomicBoolean(true);

        // Create the async testing client
        Client client = new NoOpClient(testThreadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                // Validate the request on the submitting thread before forking its execution.
                if (request instanceof InternalExecutePolicyAction.Request) {
                    assertFalse(((InternalExecutePolicyAction.Request) request).isWaitForCompletion());
                }
                // Execute all client operations on another thread.
                testThreadPool.generic().execute(() -> {
                    try {
                        // All client operations should wait until we're ready in the test.
                        clientBlockingLatch.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }

                    if (TransportGetTaskAction.TYPE.equals(action)) {
                        if (shouldGetTaskApiReturnTimeout.get() == false) {
                            // This is the second call to the Get Task API, so count down the latch to let the main test logic know.
                            secondGetTaskWasCalled.countDown();
                        }
                        // Enrich uses GetTaskAction to detect when the task completes during wait_for_completion. The first call will
                        // throw a timeout, and all remaining calls will return normally.
                        try {
                            // Wait until the signal is given to respond to the get task action
                            getTaskActionBlockingBarrier.await();
                        } catch (InterruptedException | BrokenBarrierException e) {
                            throw new RuntimeException(e);
                        }
                        // First call is a timeout to test the recovery logic. Remaining calls will no-op which should complete
                        // the execution.
                        if (shouldGetTaskApiReturnTimeout.getAndSet(false)) {
                            listener.onFailure(new ElasticsearchTimeoutException("Test call has timed out"));
                        } else if (completeWithResourceNotFound) {
                            listener.onFailure(new ElasticsearchException("Test wrapping", new ResourceNotFoundException("test")));
                        } else {
                            listener.onResponse(null);
                        }
                    } else if (InternalExecutePolicyAction.INSTANCE.equals(action)) {
                        // Return a fake task id for the run
                        @SuppressWarnings("unchecked")
                        Response response = (Response) new ExecuteEnrichPolicyAction.Response(new TaskId(testTaskId));
                        listener.onResponse(response);
                    } else {
                        listener.onResponse(null);
                    }
                });
            }
        };

        // Set up
        final EnrichPolicyLocks enrichPolicyLocks = new EnrichPolicyLocks();
        final EnrichPolicyExecutor testExecutor = new EnrichPolicyExecutor(
            Settings.EMPTY,
            null,
            null,
            client,
            testThreadPool,
            TestIndexNameExpressionResolver.newInstance(testThreadPool.getThreadContext()),
            enrichPolicyLocks,
            ESTestCase::randomNonNegativeLong
        );

        // Launch a fake policy run that will block until firstTaskBlock is counted down.
        PlainActionFuture<ExecuteEnrichPolicyAction.Response> firstTaskResult = new PlainActionFuture<>();
        testExecutor.coordinatePolicyExecution(
            new ExecuteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, testPolicyName).setWaitForCompletion(false),
            firstTaskResult
        );

        // Check to make sure the policy is locked. Do this instead of an assertTrue so that we can clean up if something breaks.
        if (enrichPolicyLocks.lockedPolices().contains(testPolicyName) == false) {
            // If this fails, be a good citizen and conclude the fake runs to keep the logs clean from interrupted exceptions during cleanup
            clientBlockingLatch.countDown();
            try {
                firstTaskResult.get(3, TimeUnit.SECONDS);
            } catch (Exception e) {
                logger.error("Encountered ignorable exception during test cleanup");
            }
            try {
                // Wait on the timing out request
                getTaskActionBlockingBarrier.await(3, TimeUnit.SECONDS);
                // Wait on the response request
                getTaskActionBlockingBarrier.await(3, TimeUnit.SECONDS);
            } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                logger.error("Encountered ignorable barrier wait exception during test cleanup");
            }
            fail("Enrich policy was not locked during task submission when it should have been");
        }

        // Free the client to execute
        clientBlockingLatch.countDown();

        // Wait for task id to be returned
        try {
            ExecuteEnrichPolicyAction.Response response = firstTaskResult.actionGet();
            assertThat(response.getStatus(), is(nullValue()));
            assertThat(response.getTaskId(), is(notNullValue()));
        } catch (AssertionError e) {
            // conclude the fake runs
            try {
                // Wait on the timing out request
                getTaskActionBlockingBarrier.await(3, TimeUnit.SECONDS);
                // Wait on the response request
                getTaskActionBlockingBarrier.await(3, TimeUnit.SECONDS);
            } catch (InterruptedException | BrokenBarrierException | TimeoutException be) {
                logger.error("Encountered ignorable barrier wait exception during test cleanup");
            }
            throw e;
        }

        // Check to make sure the policy is locked still
        if (enrichPolicyLocks.lockedPolices().contains(testPolicyName) == false) {
            // keep the logs clean
            try {
                // Wait on the timing out request
                getTaskActionBlockingBarrier.await(3, TimeUnit.SECONDS);
                // Wait on the response request
                getTaskActionBlockingBarrier.await(3, TimeUnit.SECONDS);
            } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                logger.error("Encountered ignorable barrier wait exception during test cleanup");
            }
            fail("Enrich policy was not locked after task response when it should have been");
        }

        // Now lets return a timeout response on the getTaskAPI
        try {
            getTaskActionBlockingBarrier.await(3, TimeUnit.SECONDS);
        } catch (BrokenBarrierException e) {
            throw new RuntimeException("Unexpected broken barrier exception", e);
        }

        // Wait for the executor to call back to the client with a new get task action
        try {
            // Don't need to clean up any barrier states here because the client was never called again
            assertTrue(
                "Expected task API to be called a second time by the executor after first call timed out",
                secondGetTaskWasCalled.await(3, TimeUnit.SECONDS)
            );
        } catch (InterruptedException e) {
            // We were interrupted, which means we shouldn't wait on any barriers.
            Assert.fail("Thread interrupted while waiting for background executor to call task API");
        }

        // Ensure that the policy remained locked
        if (enrichPolicyLocks.lockedPolices().contains(testPolicyName) == false) {
            // Another thread is waiting to send a task API response, signal it before failing test to keep the logs clean.
            try {
                getTaskActionBlockingBarrier.await(3, TimeUnit.SECONDS);
            } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                logger.error("Encountered ignorable barrier wait exception during test cleanup");
            }
            fail("Enrich policy was not locked after timeout when it should have been");
        }

        // If the lock has remained, then the client should have resubmitted the task wait operation. Signal a new response that will
        // complete the task wait
        try {
            getTaskActionBlockingBarrier.await(3, TimeUnit.SECONDS);
        } catch (BrokenBarrierException e) {
            throw new RuntimeException("Unexpected broken barrier exception", e);
        }

        // At this point the task should complete and unlock the policy correctly
        assertBusy(() -> assertFalse(enrichPolicyLocks.lockedPolices().contains(testPolicyName)), 3, TimeUnit.SECONDS);
    }

    public void testRunPolicyLocallyMissingPolicy() {
        EnrichPolicy enrichPolicy = EnrichPolicyTests.randomEnrichPolicy(XContentType.JSON);
        final var projectId = randomProjectIdOrDefault();
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
            .putProjectMetadata(
                ProjectMetadata.builder(projectId).putCustom(EnrichMetadata.TYPE, new EnrichMetadata(Map.of("id", enrichPolicy))).build()
            )
            .build();
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);

        final EnrichPolicyExecutor testExecutor = new EnrichPolicyExecutor(
            Settings.EMPTY,
            clusterService,
            null,
            null,
            testThreadPool,
            TestIndexNameExpressionResolver.newInstance(testThreadPool.getThreadContext()),
            new EnrichPolicyLocks(),
            ESTestCase::randomNonNegativeLong
        );

        ExecuteEnrichPolicyTask task = mock(ExecuteEnrichPolicyTask.class);
        Exception e = expectThrows(
            ResourceNotFoundException.class,
            () -> testExecutor.runPolicyLocally(projectId, task, "my-policy", ".enrich-my-policy-123456789", null)
        );
        assertThat(e.getMessage(), equalTo("policy [my-policy] does not exist"));
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
