/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.authorization;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.action.RefreshAuthorizedEndpointsAction;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMFeature;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMService;
import org.junit.Before;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.elastic.ccm.CCMFeatureTests.createMockCCMFeature;
import static org.elasticsearch.xpack.inference.services.elastic.ccm.CCMServiceTests.createMockCCMService;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AuthorizationPollerTests extends ESTestCase {
    private static final AuthorizationPoller.TaskFields TASK_FIELDS = new AuthorizationPoller.TaskFields(
        0,
        "abc",
        "abc",
        "abc",
        new TaskId("abc", 0),
        Map.of()
    );

    private DeterministicTaskQueue taskQueue;
    private Client mockClient;
    private ElasticInferenceServiceSettings eisSettings;
    private CCMFeature ccmFeature;
    private CCMService ccmService;

    @Before
    public void init() throws Exception {
        taskQueue = new DeterministicTaskQueue();
        mockClient = mock(Client.class);
        when(mockClient.threadPool()).thenReturn(taskQueue.getThreadPool());
        eisSettings = ElasticInferenceServiceSettingsTests.create("", TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true);
        // Default: CCM is not a supported environment, so the poller always proceeds to send.
        ccmFeature = createMockCCMFeature(false);
        ccmService = createMockCCMService(false);

        // Default: the action completes normally so the callback + latch fire.
        doAnswer(invocation -> {
            ActionListener<ActionResponse.Empty> listener = invocation.getArgument(2);
            listener.onResponse(ActionResponse.Empty.INSTANCE);
            return null;
        }).when(mockClient).execute(eq(RefreshAuthorizedEndpointsAction.INSTANCE), any(), any());
    }

    public void testOnlyMarksCompletedOnce() {
        // CCM is supported but disabled: the first call to scheduleAndSendAuthorizationRequest
        // should complete the task. The second call hits the shutdown guard and returns
        // immediately, not triggering a second completion.
        var poller = createPoller(null, createMockCCMFeature(true), createMockCCMService(false));

        var persistentTaskId = "id";
        var allocationId = 0L;

        var mockPersistentTasksService = mock(PersistentTasksService.class);
        poller.init(mockPersistentTasksService, mock(TaskManager.class), persistentTaskId, allocationId);

        poller.scheduleAndSendAuthorizationRequest();
        poller.scheduleAndSendAuthorizationRequest();

        verify(mockPersistentTasksService).sendCompletionRequest(eq(persistentTaskId), eq(allocationId), isNull(), isNull(), any(), any());
    }

    public void testSendsTwoAuthorizationRequests() throws InterruptedException {
        var callbackCount = new AtomicInteger(0);
        var latch = new CountDownLatch(2);
        final var pollerRef = new AtomicReference<AuthorizationPoller>();

        Runnable callback = () -> {
            var count = callbackCount.incrementAndGet();
            latch.countDown();

            // we only want to run the tasks twice, so advance the time on the queue
            // which flags the scheduled authorization request to be ready to run
            if (count == 1) {
                taskQueue.advanceTime();
            } else {
                pollerRef.get().shutdown();
            }
        };

        var poller = createPoller(callback);

        pollerRef.set(poller);
        poller.start();
        taskQueue.runAllRunnableTasks();
        latch.await(TimeValue.THIRTY_SECONDS.getSeconds(), TimeUnit.SECONDS);

        assertThat(callbackCount.get(), is(2));
    }

    public void testCallsShutdownAndMarksTaskAsCompleted_WhenSchedulingFails() throws InterruptedException {
        var callbackCount = new AtomicInteger(0);
        var latch = new CountDownLatch(1);

        Runnable callback = () -> {
            callbackCount.incrementAndGet();
            latch.countDown();
        };

        var exception = new IllegalStateException("failing");
        givenSchedulingFailure(exception);

        var poller = createPoller(callback);

        var persistentTaskId = "id";
        var allocationId = 0L;

        var mockPersistentTasksService = mock(PersistentTasksService.class);
        poller.init(mockPersistentTasksService, mock(TaskManager.class), persistentTaskId, allocationId);
        poller.start();
        taskQueue.runAllRunnableTasks();
        latch.await(TimeValue.THIRTY_SECONDS.getSeconds(), TimeUnit.SECONDS);

        assertThat(callbackCount.get(), is(1));
        assertTrue(poller.isShutdown());
        verify(mockPersistentTasksService).sendCompletionRequest(
            eq(persistentTaskId),
            eq(allocationId),
            eq(exception),
            eq(null),
            any(),
            any()
        );

        poller.waitForAuthorizationToComplete(TimeValue.THIRTY_SECONDS);
        verify(mockPersistentTasksService, never()).sendCompletionRequest(
            eq(persistentTaskId),
            eq(allocationId),
            isNull(),
            isNull(),
            any(),
            any()
        );
    }

    public void testDoesNotSendAuthorizationRequest_WhenCCMIsSupportedButDisabled() {
        var poller = createPoller(null, createMockCCMFeature(true), createMockCCMService(false));

        var persistentTaskId = "id";
        var allocationId = 0L;
        var mockPersistentTasksService = mock(PersistentTasksService.class);
        poller.init(mockPersistentTasksService, mock(TaskManager.class), persistentTaskId, allocationId);

        poller.scheduleAndSendAuthorizationRequest();

        assertTrue(poller.isShutdown());
        verify(mockClient, never()).execute(eq(RefreshAuthorizedEndpointsAction.INSTANCE), any(), any());
        verify(mockPersistentTasksService).sendCompletionRequest(eq(persistentTaskId), eq(allocationId), isNull(), isNull(), any(), any());
    }

    public void testSendsAuthorizationRequest_WhenCCMIsSupportedAndEnabled() {
        var poller = createPoller(null, createMockCCMFeature(true), createMockCCMService(true));

        poller.scheduleAndSendAuthorizationRequest();

        verify(mockClient).execute(eq(RefreshAuthorizedEndpointsAction.INSTANCE), any(), any());
        assertFalse(poller.isShutdown());
    }

    public void testSendsAuthorizationRequest_WhenCCMIsNotASupportedEnvironment() {
        var poller = createPoller(null, createMockCCMFeature(false), createMockCCMService(false));

        poller.scheduleAndSendAuthorizationRequest();

        verify(mockClient).execute(eq(RefreshAuthorizedEndpointsAction.INSTANCE), any(), any());
        assertFalse(poller.isShutdown());
    }

    public void testSchedulesNextPoll_WhenCCMEnabledCheckFails() {
        var ccmServiceWithFailure = mock(CCMService.class);
        doAnswer(invocation -> {
            ActionListener<Boolean> listener = invocation.getArgument(0);
            listener.onFailure(new RuntimeException("isEnabled failed"));
            return null;
        }).when(ccmServiceWithFailure).isEnabled(any());

        var poller = createPoller(null, createMockCCMFeature(true), ccmServiceWithFailure);

        poller.scheduleAndSendAuthorizationRequest();

        assertFalse(poller.isShutdown());
        verify(mockClient, never()).execute(eq(RefreshAuthorizedEndpointsAction.INSTANCE), any(), any());
        assertTrue(taskQueue.hasDeferredTasks());
    }

    private AuthorizationPoller createPoller(Runnable callback) {
        return createPoller(callback, ccmFeature, ccmService);
    }

    private AuthorizationPoller createPoller(Runnable callback, CCMFeature ccmFeature, CCMService ccmService) {
        return new AuthorizationPoller(
            TASK_FIELDS,
            createWithEmptySettings(taskQueue.getThreadPool()),
            eisSettings,
            mockClient,
            ccmFeature,
            ccmService,
            callback
        );
    }

    private void givenSchedulingFailure(Exception exception) {
        // Simulate scheduling failure by having the settings throw an exception when queried.
        // Throwing an exception should cause the poller to shutdown and mark itself as failed.
        eisSettings = mock(ElasticInferenceServiceSettings.class);
        when(eisSettings.isPeriodicAuthorizationEnabled()).thenThrow(exception);
    }
}
