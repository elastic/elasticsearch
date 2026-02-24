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
import org.elasticsearch.inference.StatusHeuristic;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.metadata.EndpointMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.action.StoreInferenceEndpointsAction;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsBuilder;
import org.elasticsearch.xpack.inference.InferenceFeatures;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.features.InferenceFeatureService;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceComponents;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntity;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsServiceSettings;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.elastic.authorization.EndpointSchemaMigration.ENDPOINT_SCHEMA_VERSION;
import static org.elasticsearch.xpack.inference.services.elastic.ccm.CCMFeatureTests.createMockCCMFeature;
import static org.elasticsearch.xpack.inference.services.elastic.ccm.CCMServiceTests.createMockCCMService;
import static org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntityTests.createAuthorizedEndpoint;
import static org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntityTests.createInvalidTaskTypeAuthorizedEndpoint;
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
    private static final AuthorizationPoller.TaskFields TASK_FIELDS = new AuthorizationPoller.TaskFields(
        0,
        "abc",
        "abc",
        "abc",
        new TaskId("abc", 0),
        Map.of()
    );

    private DeterministicTaskQueue taskQueue;
    private InferenceFeatureService inferenceFeatureServiceMock;

    @Before
    public void init() throws Exception {
        taskQueue = new DeterministicTaskQueue();
        inferenceFeatureServiceMock = mock(InferenceFeatureService.class);
        when(inferenceFeatureServiceMock.hasFeature(InferenceFeatures.ENDPOINT_METADATA_FIELD)).thenReturn(true);
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
            null,
            inferenceFeatureServiceMock
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
            TASK_FIELDS,
            createWithEmptySettings(taskQueue.getThreadPool()),
            authorizationRequestHandler,
            mock(Sender.class),
            ElasticInferenceServiceSettingsTests.create("", TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
            mockRegistry,
            mock(Client.class),
            createMockCCMFeature(true),
            createMockCCMService(false),
            null,
            inferenceFeatureServiceMock
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

    public void testDoesNotSendAuthorizationRequest_WhenClusterDoesNotIncludeMetadata_MappingUpdate() {
        var mockRegistry = mock(ModelRegistry.class);
        when(mockRegistry.isReady()).thenReturn(true);

        var inferenceFeatureService = mock(InferenceFeatureService.class);
        when(inferenceFeatureService.hasFeature(InferenceFeatures.ENDPOINT_METADATA_FIELD)).thenReturn(false);

        var authorizationRequestHandler = mock(ElasticInferenceServiceAuthorizationRequestHandler.class);

        var poller = new AuthorizationPoller(
            TASK_FIELDS,
            createWithEmptySettings(taskQueue.getThreadPool()),
            authorizationRequestHandler,
            mock(Sender.class),
            ElasticInferenceServiceSettingsTests.create("", TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
            mockRegistry,
            mock(Client.class),
            createMockCCMFeature(true),
            createMockCCMService(true),
            null,
            inferenceFeatureService
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
            null,
            inferenceFeatureServiceMock
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
        when(mockRegistry.getInferenceIds()).thenReturn(Set.of());

        var url = "eis-url";
        var sparseModel = createAuthorizedEndpoint(TaskType.SPARSE_EMBEDDING);
        var mockAuthHandler = mock(ElasticInferenceServiceAuthorizationRequestHandler.class);
        doAnswer(invocation -> {
            ActionListener<ElasticInferenceServiceAuthorizationModel> listener = invocation.getArgument(0);
            listener.onResponse(
                ElasticInferenceServiceAuthorizationModel.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity(List.of(sparseModel)),
                    url
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
            null,
            inferenceFeatureServiceMock
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
                    createSparseEndpoint(
                        sparseModel.id(),
                        sparseModel.modelName(),
                        url,
                        new EndpointMetadata(
                            new EndpointMetadata.Heuristics(
                                List.of(),
                                StatusHeuristic.fromString(sparseModel.status()),
                                sparseModel.releaseDate(),
                                sparseModel.endOfLifeDate()
                            ),
                            new EndpointMetadata.Internal(sparseModel.fingerprint(), ENDPOINT_SCHEMA_VERSION),
                            sparseModel.displayName() != null ? new EndpointMetadata.Display(sparseModel.displayName()) : null
                        )
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

    private ElasticInferenceServiceSparseEmbeddingsModel createSparseEndpoint(
        String endpointId,
        String modelName,
        String url,
        EndpointMetadata endpointMetadata
    ) {
        return new ElasticInferenceServiceSparseEmbeddingsModel(
            endpointId,
            TaskType.SPARSE_EMBEDDING,
            new ElasticInferenceServiceSparseEmbeddingsServiceSettings(modelName, null, null),
            new ElasticInferenceServiceComponents(url),
            ChunkingSettingsBuilder.DEFAULT_SETTINGS,
            endpointMetadata
        );
    }

    public void testSendsAuthorizationRequest_WhenCCMIsNotConfigurable() {
        var mockRegistry = mock(ModelRegistry.class);
        when(mockRegistry.isReady()).thenReturn(true);
        when(mockRegistry.getInferenceIds()).thenReturn(Set.of());

        var url = "eis-url";
        var sparseModel = createAuthorizedEndpoint(TaskType.SPARSE_EMBEDDING);

        var mockAuthHandler = mock(ElasticInferenceServiceAuthorizationRequestHandler.class);
        doAnswer(invocation -> {
            ActionListener<ElasticInferenceServiceAuthorizationModel> listener = invocation.getArgument(0);
            listener.onResponse(
                ElasticInferenceServiceAuthorizationModel.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity(List.of(sparseModel)),
                    url
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
            null,
            inferenceFeatureServiceMock
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
                    createSparseEndpoint(
                        sparseModel.id(),
                        sparseModel.modelName(),
                        url,
                        new EndpointMetadata(
                            new EndpointMetadata.Heuristics(
                                List.of(),
                                StatusHeuristic.fromString(sparseModel.status()),
                                sparseModel.releaseDate(),
                                sparseModel.endOfLifeDate()
                            ),
                            new EndpointMetadata.Internal(sparseModel.fingerprint(), ENDPOINT_SCHEMA_VERSION),
                            sparseModel.displayName() != null ? new EndpointMetadata.Display(sparseModel.displayName()) : null
                        )
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
        var url = "eis-url";
        var sparseModel = createAuthorizedEndpoint(TaskType.SPARSE_EMBEDDING);

        var mockRegistry = mock(ModelRegistry.class);
        when(mockRegistry.isReady()).thenReturn(true);
        when(mockRegistry.getInferenceIds()).thenReturn(Set.of(sparseModel.id(), "id2"));

        var mockAuthHandler = mock(ElasticInferenceServiceAuthorizationRequestHandler.class);
        doAnswer(invocation -> {
            ActionListener<ElasticInferenceServiceAuthorizationModel> listener = invocation.getArgument(0);
            listener.onResponse(
                ElasticInferenceServiceAuthorizationModel.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity(List.of(sparseModel)),
                    url
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
            null,
            inferenceFeatureServiceMock
        );

        poller.sendAuthorizationRequest();
        verify(mockClient, never()).execute(eq(StoreInferenceEndpointsAction.INSTANCE), any(), any());
    }

    public void testDoesNotAttemptToStoreModelIds_ThatHaveATaskTypeThatTheEISIntegration_DoesNotSupport() {
        var url = "eis-url";
        var invalidTaskTypeEndpoint = createInvalidTaskTypeAuthorizedEndpoint();

        var mockRegistry = mock(ModelRegistry.class);
        when(mockRegistry.isReady()).thenReturn(true);
        when(mockRegistry.getInferenceIds()).thenReturn(Set.of());

        var mockAuthHandler = mock(ElasticInferenceServiceAuthorizationRequestHandler.class);
        doAnswer(invocation -> {
            ActionListener<ElasticInferenceServiceAuthorizationModel> listener = invocation.getArgument(0);
            listener.onResponse(
                ElasticInferenceServiceAuthorizationModel.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity(List.of(invalidTaskTypeEndpoint)),
                    url
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
            null,
            inferenceFeatureServiceMock
        );

        poller.sendAuthorizationRequest();
        verify(mockClient, never()).execute(eq(StoreInferenceEndpointsAction.INSTANCE), any(), any());
    }

    public void testSendsTwoAuthorizationRequests() throws InterruptedException {
        var url = "eis-url";
        var sparseModel = createAuthorizedEndpoint(TaskType.SPARSE_EMBEDDING);

        var mockRegistry = mock(ModelRegistry.class);
        when(mockRegistry.isReady()).thenReturn(true);
        // Since the registry is already aware of the sparse endpoint, the authorization poller will not consider it a new
        // one and not attempt to store it.
        when(mockRegistry.getInferenceIds()).thenReturn(Set.of(sparseModel.id()));

        var mockAuthHandler = mock(ElasticInferenceServiceAuthorizationRequestHandler.class);
        doAnswer(invocation -> {
            ActionListener<ElasticInferenceServiceAuthorizationModel> listener = invocation.getArgument(0);
            listener.onResponse(
                ElasticInferenceServiceAuthorizationModel.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity(List.of(sparseModel)),
                    url
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
            callback,
            inferenceFeatureServiceMock
        );
        pollerRef.set(poller);
        poller.start();
        taskQueue.runAllRunnableTasks();
        latch.await(TimeValue.THIRTY_SECONDS.getSeconds(), TimeUnit.SECONDS);

        assertThat(callbackCount.get(), is(2));
        verify(mockClient, never()).execute(eq(StoreInferenceEndpointsAction.INSTANCE), any(), any());
    }

    public void testCallsShutdownAndMarksTaskAsCompleted_WhenSchedulingFails() throws InterruptedException {
        var url = "eis-url";
        var sparseModel = createAuthorizedEndpoint(TaskType.SPARSE_EMBEDDING);

        var mockRegistry = mock(ModelRegistry.class);
        when(mockRegistry.isReady()).thenReturn(true);
        // Since the registry is already aware of the sparse endpoint, the authorization poller will not consider it a new
        // one and not attempt to store it.
        when(mockRegistry.getInferenceIds()).thenReturn(Set.of("id1", sparseModel.id()));

        var mockAuthHandler = mock(ElasticInferenceServiceAuthorizationRequestHandler.class);
        doAnswer(invocation -> {
            ActionListener<ElasticInferenceServiceAuthorizationModel> listener = invocation.getArgument(0);
            listener.onResponse(
                ElasticInferenceServiceAuthorizationModel.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity(List.of(sparseModel)),
                    url
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
            callback,
            inferenceFeatureServiceMock
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
