/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.authorization;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.action.StoreInferenceEndpointsAction;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceComponents;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.elastic.InternalPreconfiguredEndpoints;
import org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntity;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AuthorizationPollerTests extends ESTestCase {
    private DeterministicTaskQueue taskQueue;

    @Before
    public void init() throws Exception {
        taskQueue = new DeterministicTaskQueue();
    }

    public void testDoesNotSendAuthorizationRequest_WhenModelRegistryIsNotReady() {
        var mockRegistry = mock(ModelRegistry.class);
        when(mockRegistry.isReady()).thenReturn(false);

        var authorizationRequestHandler = mock(ElasticInferenceServiceAuthorizationRequestHandler.class);

        var poller = new AuthorizationPoller(
            new AuthorizationPoller.TaskFields(0, "abc", "abc", "abc", new TaskId("abc", 0), Map.of()),
            createWithEmptySettings(taskQueue.getThreadPool()),
            authorizationRequestHandler,
            mock(Sender.class),
            ElasticInferenceServiceSettingsTests.create("", TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
            mockRegistry,
            mock(Client.class),
            createMockCCMFeature(false),
            createMockCCMService(false),
            null
        );

        var persistentTaskId = "id";
        var allocationId = 0L;

        var mockPersistentTasksService = mock(PersistentTasksService.class);
        poller.init(mockPersistentTasksService, mock(TaskManager.class), persistentTaskId, allocationId);

        poller.sendAuthorizationRequest();

        verify(authorizationRequestHandler, never()).getAuthorization(any(), any());
        verify(mockPersistentTasksService, never()).sendCompletionRequest(
            eq(persistentTaskId),
            eq(allocationId),
            isNull(),
            isNull(),
            any(),
            any()
        );
    }

    public void testDoesNotSendAuthorizationRequest_WhenCCMIsDisabled() {
        var mockRegistry = mock(ModelRegistry.class);
        when(mockRegistry.isReady()).thenReturn(true);

        var authorizationRequestHandler = mock(ElasticInferenceServiceAuthorizationRequestHandler.class);

        var poller = new AuthorizationPoller(
            new AuthorizationPoller.TaskFields(0, "abc", "abc", "abc", new TaskId("abc", 0), Map.of()),
            createWithEmptySettings(taskQueue.getThreadPool()),
            authorizationRequestHandler,
            mock(Sender.class),
            ElasticInferenceServiceSettingsTests.create("", TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
            mockRegistry,
            mock(Client.class),
            createMockCCMFeature(true),
            createMockCCMService(false),
            null
        );

        var persistentTaskId = "id";
        var allocationId = 0L;

        var mockPersistentTasksService = mock(PersistentTasksService.class);
        poller.init(mockPersistentTasksService, mock(TaskManager.class), persistentTaskId, allocationId);

        poller.sendAuthorizationRequest();

        verify(authorizationRequestHandler, never()).getAuthorization(any(), any());
        verify(mockPersistentTasksService, times(1)).sendCompletionRequest(
            eq(persistentTaskId),
            eq(allocationId),
            isNull(),
            isNull(),
            any(),
            any()
        );
    }

    public void testOnlyMarksCompletedOnce() {
        var mockRegistry = mock(ModelRegistry.class);
        when(mockRegistry.isReady()).thenReturn(true);

        var authorizationRequestHandler = mock(ElasticInferenceServiceAuthorizationRequestHandler.class);

        var poller = new AuthorizationPoller(
            new AuthorizationPoller.TaskFields(0, "abc", "abc", "abc", new TaskId("abc", 0), Map.of()),
            createWithEmptySettings(taskQueue.getThreadPool()),
            authorizationRequestHandler,
            mock(Sender.class),
            ElasticInferenceServiceSettingsTests.create("", TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
            mockRegistry,
            mock(Client.class),
            createMockCCMFeature(true),
            createMockCCMService(false),
            null
        );

        var persistentTaskId = "id";
        var allocationId = 0L;

        var mockPersistentTasksService = mock(PersistentTasksService.class);
        poller.init(mockPersistentTasksService, mock(TaskManager.class), persistentTaskId, allocationId);

        poller.sendAuthorizationRequest();
        poller.sendAuthorizationRequest();

        verify(authorizationRequestHandler, never()).getAuthorization(any(), any());
        verify(mockPersistentTasksService, times(1)).sendCompletionRequest(
            eq(persistentTaskId),
            eq(allocationId),
            isNull(),
            isNull(),
            any(),
            any()
        );
    }

    public void testSendsAuthorizationRequest_WhenModelRegistryIsReady() {
        var mockRegistry = mock(ModelRegistry.class);
        when(mockRegistry.isReady()).thenReturn(true);
        when(mockRegistry.getInferenceIds()).thenReturn(Set.of("id1", "id2"));

        var mockAuthHandler = mock(ElasticInferenceServiceAuthorizationRequestHandler.class);
        doAnswer(invocation -> {
            ActionListener<ElasticInferenceServiceAuthorizationModel> listener = invocation.getArgument(0);
            listener.onResponse(
                ElasticInferenceServiceAuthorizationModel.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity(
                        List.of(
                            new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                                InternalPreconfiguredEndpoints.DEFAULT_ELSER_2_MODEL_ID,
                                EnumSet.of(TaskType.SPARSE_EMBEDDING)
                            )
                        )
                    )
                )
            );
            return Void.TYPE;
        }).when(mockAuthHandler).getAuthorization(any(), any());

        var mockClient = mock(Client.class);
        when(mockClient.threadPool()).thenReturn(taskQueue.getThreadPool());

        var poller = new AuthorizationPoller(
            new AuthorizationPoller.TaskFields(0, "abc", "abc", "abc", new TaskId("abc", 0), Map.of()),
            createWithEmptySettings(taskQueue.getThreadPool()),
            mockAuthHandler,
            mock(Sender.class),
            ElasticInferenceServiceSettingsTests.create("", TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
            mockRegistry,
            mockClient,
            createMockCCMFeature(true),
            createMockCCMService(true),
            null
        );

        var persistentTaskId = "id";
        var allocationId = 0L;

        var mockPersistentTasksService = mock(PersistentTasksService.class);
        poller.init(mockPersistentTasksService, mock(TaskManager.class), persistentTaskId, allocationId);

        var requestArgCaptor = ArgumentCaptor.forClass(StoreInferenceEndpointsAction.Request.class);

        poller.sendAuthorizationRequest();
        verify(mockClient).execute(eq(StoreInferenceEndpointsAction.INSTANCE), requestArgCaptor.capture(), any());
        var capturedRequest = requestArgCaptor.getValue();
        assertThat(
            capturedRequest.getModels(),
            is(
                List.of(
                    PreconfiguredEndpointModelAdapter.createModel(
                        InternalPreconfiguredEndpoints.getWithInferenceId(InternalPreconfiguredEndpoints.DEFAULT_ELSER_ENDPOINT_ID_V2),
                        new ElasticInferenceServiceComponents("")
                    )
                )
            )
        );

        verify(mockPersistentTasksService, never()).sendCompletionRequest(
            eq(persistentTaskId),
            eq(allocationId),
            any(),
            any(),
            any(),
            any()
        );
    }

    public void testSendsAuthorizationRequest_WhenCCMIsNotConfigurable() {
        var mockRegistry = mock(ModelRegistry.class);
        when(mockRegistry.isReady()).thenReturn(true);
        when(mockRegistry.getInferenceIds()).thenReturn(Set.of("id1", "id2"));

        var mockAuthHandler = mock(ElasticInferenceServiceAuthorizationRequestHandler.class);
        doAnswer(invocation -> {
            ActionListener<ElasticInferenceServiceAuthorizationModel> listener = invocation.getArgument(0);
            listener.onResponse(
                ElasticInferenceServiceAuthorizationModel.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity(
                        List.of(
                            new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                                InternalPreconfiguredEndpoints.DEFAULT_ELSER_2_MODEL_ID,
                                EnumSet.of(TaskType.SPARSE_EMBEDDING)
                            )
                        )
                    )
                )
            );
            return Void.TYPE;
        }).when(mockAuthHandler).getAuthorization(any(), any());

        var mockClient = mock(Client.class);
        when(mockClient.threadPool()).thenReturn(taskQueue.getThreadPool());

        var poller = new AuthorizationPoller(
            new AuthorizationPoller.TaskFields(0, "abc", "abc", "abc", new TaskId("abc", 0), Map.of()),
            createWithEmptySettings(taskQueue.getThreadPool()),
            mockAuthHandler,
            mock(Sender.class),
            ElasticInferenceServiceSettingsTests.create("", TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
            mockRegistry,
            mockClient,
            // CCM is not configurable so we should send the request because it doesn't depend on an api key
            createMockCCMFeature(false),
            createMockCCMService(false),
            null
        );

        var persistentTaskId = "id";
        var allocationId = 0L;

        var mockPersistentTasksService = mock(PersistentTasksService.class);
        poller.init(mockPersistentTasksService, mock(TaskManager.class), persistentTaskId, allocationId);

        var requestArgCaptor = ArgumentCaptor.forClass(StoreInferenceEndpointsAction.Request.class);

        poller.sendAuthorizationRequest();
        verify(mockClient).execute(eq(StoreInferenceEndpointsAction.INSTANCE), requestArgCaptor.capture(), any());
        var capturedRequest = requestArgCaptor.getValue();
        assertThat(
            capturedRequest.getModels(),
            is(
                List.of(
                    PreconfiguredEndpointModelAdapter.createModel(
                        InternalPreconfiguredEndpoints.getWithInferenceId(InternalPreconfiguredEndpoints.DEFAULT_ELSER_ENDPOINT_ID_V2),
                        new ElasticInferenceServiceComponents("")
                    )
                )
            )
        );

        verify(mockPersistentTasksService, never()).sendCompletionRequest(
            eq(persistentTaskId),
            eq(allocationId),
            any(),
            any(),
            any(),
            any()
        );
    }

    public void testSendsAuthorizationRequest_ButDoesNotStoreAnyModels_WhenTheirInferenceIdAlreadyExists() {
        var mockRegistry = mock(ModelRegistry.class);
        when(mockRegistry.isReady()).thenReturn(true);
        when(mockRegistry.getInferenceIds()).thenReturn(Set.of(InternalPreconfiguredEndpoints.DEFAULT_ELSER_ENDPOINT_ID_V2, "id2"));

        var mockAuthHandler = mock(ElasticInferenceServiceAuthorizationRequestHandler.class);
        doAnswer(invocation -> {
            ActionListener<ElasticInferenceServiceAuthorizationModel> listener = invocation.getArgument(0);
            listener.onResponse(
                ElasticInferenceServiceAuthorizationModel.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity(
                        List.of(
                            new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                                InternalPreconfiguredEndpoints.DEFAULT_ELSER_2_MODEL_ID,
                                EnumSet.of(TaskType.SPARSE_EMBEDDING)
                            )
                        )
                    )
                )
            );
            return Void.TYPE;
        }).when(mockAuthHandler).getAuthorization(any(), any());

        var mockClient = mock(Client.class);
        when(mockClient.threadPool()).thenReturn(taskQueue.getThreadPool());

        var poller = new AuthorizationPoller(
            new AuthorizationPoller.TaskFields(0, "abc", "abc", "abc", new TaskId("abc", 0), Map.of()),
            createWithEmptySettings(taskQueue.getThreadPool()),
            mockAuthHandler,
            mock(Sender.class),
            ElasticInferenceServiceSettingsTests.create("", TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
            mockRegistry,
            mockClient,
            createMockCCMFeature(true),
            createMockCCMService(true),
            null
        );

        poller.sendAuthorizationRequest();
        verify(mockClient, never()).execute(eq(StoreInferenceEndpointsAction.INSTANCE), any(), any());
    }

    public void testDoesNotAttemptToStoreModelIds_ThatDoNotExistInThePreconfiguredMapping() {
        var mockRegistry = mock(ModelRegistry.class);
        when(mockRegistry.isReady()).thenReturn(true);
        when(mockRegistry.getInferenceIds()).thenReturn(Set.of("id1", "id2"));

        var mockAuthHandler = mock(ElasticInferenceServiceAuthorizationRequestHandler.class);
        doAnswer(invocation -> {
            ActionListener<ElasticInferenceServiceAuthorizationModel> listener = invocation.getArgument(0);
            listener.onResponse(
                ElasticInferenceServiceAuthorizationModel.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity(
                        List.of(
                            new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                                // This is a model id that does not exist in the preconfigured endpoints map so it will not be stored
                                "abc",
                                EnumSet.of(TaskType.SPARSE_EMBEDDING)
                            )
                        )
                    )
                )
            );
            return Void.TYPE;
        }).when(mockAuthHandler).getAuthorization(any(), any());

        var mockClient = mock(Client.class);
        when(mockClient.threadPool()).thenReturn(taskQueue.getThreadPool());

        var poller = new AuthorizationPoller(
            new AuthorizationPoller.TaskFields(0, "abc", "abc", "abc", new TaskId("abc", 0), Map.of()),
            createWithEmptySettings(taskQueue.getThreadPool()),
            mockAuthHandler,
            mock(Sender.class),
            ElasticInferenceServiceSettingsTests.create("", TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
            mockRegistry,
            mockClient,
            createMockCCMFeature(true),
            createMockCCMService(true),
            null
        );

        poller.sendAuthorizationRequest();
        verify(mockClient, never()).execute(eq(StoreInferenceEndpointsAction.INSTANCE), any(), any());
    }

    public void testDoesNotAttemptToStoreModelIds_ThatHaveATaskTypeThatTheEISIntegration_DoesNotSupport() {
        var mockRegistry = mock(ModelRegistry.class);
        when(mockRegistry.isReady()).thenReturn(true);
        when(mockRegistry.getInferenceIds()).thenReturn(Set.of("id1", "id2"));

        var mockAuthHandler = mock(ElasticInferenceServiceAuthorizationRequestHandler.class);
        doAnswer(invocation -> {
            ActionListener<ElasticInferenceServiceAuthorizationModel> listener = invocation.getArgument(0);
            listener.onResponse(
                ElasticInferenceServiceAuthorizationModel.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity(
                        List.of(
                            new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                                InternalPreconfiguredEndpoints.DEFAULT_ELSER_2_MODEL_ID,
                                EnumSet.noneOf(TaskType.class)
                            )
                        )
                    )
                )
            );
            return Void.TYPE;
        }).when(mockAuthHandler).getAuthorization(any(), any());

        var mockClient = mock(Client.class);
        when(mockClient.threadPool()).thenReturn(taskQueue.getThreadPool());

        var poller = new AuthorizationPoller(
            new AuthorizationPoller.TaskFields(0, "abc", "abc", "abc", new TaskId("abc", 0), Map.of()),
            createWithEmptySettings(taskQueue.getThreadPool()),
            mockAuthHandler,
            mock(Sender.class),
            ElasticInferenceServiceSettingsTests.create("", TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
            mockRegistry,
            mockClient,
            createMockCCMFeature(true),
            createMockCCMService(true),
            null
        );

        poller.sendAuthorizationRequest();
        verify(mockClient, never()).execute(eq(StoreInferenceEndpointsAction.INSTANCE), any(), any());
    }

    public void testSendsTwoAuthorizationRequests() throws InterruptedException {
        var mockRegistry = mock(ModelRegistry.class);
        when(mockRegistry.isReady()).thenReturn(true);
        when(mockRegistry.getInferenceIds()).thenReturn(Set.of("id1", "id2"));

        var mockAuthHandler = mock(ElasticInferenceServiceAuthorizationRequestHandler.class);
        doAnswer(invocation -> {
            ActionListener<ElasticInferenceServiceAuthorizationModel> listener = invocation.getArgument(0);
            listener.onResponse(
                ElasticInferenceServiceAuthorizationModel.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity(
                        List.of(
                            new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                                // this is an unknown model id so it won't trigger storing an inference endpoint because
                                // it doesn't map to a known one
                                "abc",
                                EnumSet.of(TaskType.SPARSE_EMBEDDING)
                            )
                        )
                    )
                )
            );
            return Void.TYPE;
        }).when(mockAuthHandler).getAuthorization(any(), any());

        var mockClient = mock(Client.class);

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

        var poller = new AuthorizationPoller(
            new AuthorizationPoller.TaskFields(0, "abc", "abc", "abc", new TaskId("abc", 0), Map.of()),
            createWithEmptySettings(taskQueue.getThreadPool()),
            mockAuthHandler,
            mock(Sender.class),
            ElasticInferenceServiceSettingsTests.create("", TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
            mockRegistry,
            mockClient,
            createMockCCMFeature(true),
            createMockCCMService(true),
            callback
        );
        pollerRef.set(poller);
        poller.start();
        taskQueue.runAllRunnableTasks();
        latch.await(TimeValue.THIRTY_SECONDS.getSeconds(), TimeUnit.SECONDS);

        assertThat(callbackCount.get(), is(2));
        verify(mockClient, never()).execute(eq(StoreInferenceEndpointsAction.INSTANCE), any(), any());
    }

    public void testCallsShutdownAndMarksTaskAsCompleted_WhenSchedulingFails() throws InterruptedException {
        var mockRegistry = mock(ModelRegistry.class);
        when(mockRegistry.isReady()).thenReturn(true);
        when(mockRegistry.getInferenceIds()).thenReturn(Set.of("id1", "id2"));

        var mockAuthHandler = mock(ElasticInferenceServiceAuthorizationRequestHandler.class);
        doAnswer(invocation -> {
            ActionListener<ElasticInferenceServiceAuthorizationModel> listener = invocation.getArgument(0);
            listener.onResponse(
                ElasticInferenceServiceAuthorizationModel.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity(
                        List.of(
                            new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                                // this is an unknown model id so it won't trigger storing an inference endpoint because
                                // it doesn't map to a known one
                                "abc",
                                EnumSet.of(TaskType.SPARSE_EMBEDDING)
                            )
                        )
                    )
                )
            );
            return Void.TYPE;
        }).when(mockAuthHandler).getAuthorization(any(), any());

        var mockClient = mock(Client.class);

        var callbackCount = new AtomicInteger(0);
        var latch = new CountDownLatch(1);

        Runnable callback = () -> {
            callbackCount.incrementAndGet();
            latch.countDown();
        };

        var exception = new IllegalStateException("failing");
        // Simulate scheduling failure by having the settings throw an exception when queried
        // Throwing an exception should cause the poller to shutdown and mark itself as completed
        var settingsMock = mock(ElasticInferenceServiceSettings.class);
        when(settingsMock.isPeriodicAuthorizationEnabled()).thenThrow(exception);

        var poller = new AuthorizationPoller(
            new AuthorizationPoller.TaskFields(0, "abc", "abc", "abc", new TaskId("abc", 0), Map.of()),
            createWithEmptySettings(taskQueue.getThreadPool()),
            mockAuthHandler,
            mock(Sender.class),
            settingsMock,
            mockRegistry,
            mockClient,
            createMockCCMFeature(true),
            createMockCCMService(true),
            callback
        );

        var persistentTaskId = "id";
        var allocationId = 0L;

        var mockPersistentTasksService = mock(PersistentTasksService.class);
        poller.init(mockPersistentTasksService, mock(TaskManager.class), persistentTaskId, allocationId);
        poller.start();
        taskQueue.runAllRunnableTasks();
        latch.await(TimeValue.THIRTY_SECONDS.getSeconds(), TimeUnit.SECONDS);

        assertThat(callbackCount.get(), is(1));
        assertTrue(poller.isShutdown());
        verify(mockPersistentTasksService, times(1)).sendCompletionRequest(
            eq(persistentTaskId),
            eq(allocationId),
            eq(exception),
            eq(null),
            any(),
            any()
        );
        verify(mockClient, never()).execute(eq(StoreInferenceEndpointsAction.INSTANCE), any(), any());

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
}
