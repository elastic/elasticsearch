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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.Model;
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
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMFeature;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMService;
import org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntity;
import org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsServiceSettings;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
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
import static org.hamcrest.Matchers.containsInAnyOrder;
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
    private InferenceFeatureService inferenceFeatureServiceMock;
    private ModelRegistry mockRegistry;
    private ElasticInferenceServiceAuthorizationRequestHandler mockAuthHandler;
    private CCMFeature ccmFeature;
    private CCMService ccmService;
    private Client mockClient;
    private ElasticInferenceServiceSettings eisSettings;

    @Before
    public void init() throws Exception {
        taskQueue = new DeterministicTaskQueue();
        inferenceFeatureServiceMock = mock(InferenceFeatureService.class);
        when(inferenceFeatureServiceMock.hasFeature(InferenceFeatures.ENDPOINT_METADATA_FIELD)).thenReturn(true);
        mockRegistry = mock(ModelRegistry.class);
        mockAuthHandler = mock(ElasticInferenceServiceAuthorizationRequestHandler.class);
        mockClient = mock(Client.class);
        when(mockClient.threadPool()).thenReturn(taskQueue.getThreadPool());
        eisSettings = ElasticInferenceServiceSettingsTests.create("", TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true);
    }

    public void testDoesNotSendAuthorizationRequest_WhenModelRegistryIsNotReady() {
        when(mockRegistry.isReady()).thenReturn(false);
        ccmFeature = createMockCCMFeature(false);
        ccmService = createMockCCMService(false);
        var poller = createPoller();

        var persistentTaskId = "id";
        var allocationId = 0L;

        var mockPersistentTasksService = mock(PersistentTasksService.class);
        poller.init(mockPersistentTasksService, mock(TaskManager.class), persistentTaskId, allocationId);

        poller.sendAuthorizationRequest();

        verify(mockAuthHandler, never()).getAuthorization(any(), any());
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
        when(mockRegistry.isReady()).thenReturn(true);
        ccmFeature = createMockCCMFeature(true);
        ccmService = createMockCCMService(false);
        var poller = createPoller();

        var persistentTaskId = "id";
        var allocationId = 0L;

        var mockPersistentTasksService = mock(PersistentTasksService.class);
        poller.init(mockPersistentTasksService, mock(TaskManager.class), persistentTaskId, allocationId);

        poller.sendAuthorizationRequest();

        verify(mockAuthHandler, never()).getAuthorization(any(), any());
        verify(mockPersistentTasksService).sendCompletionRequest(eq(persistentTaskId), eq(allocationId), isNull(), isNull(), any(), any());
    }

    public void testDoesNotSendAuthorizationRequest_WhenClusterDoesNotIncludeMetadata_MappingUpdate() {
        when(mockRegistry.isReady()).thenReturn(true);
        ccmFeature = createMockCCMFeature(true);
        ccmService = createMockCCMService(true);
        when(inferenceFeatureServiceMock.hasFeature(InferenceFeatures.ENDPOINT_METADATA_FIELD)).thenReturn(false);
        var poller = createPoller();

        var persistentTaskId = "id";
        var allocationId = 0L;

        var mockPersistentTasksService = mock(PersistentTasksService.class);
        poller.init(mockPersistentTasksService, mock(TaskManager.class), persistentTaskId, allocationId);

        poller.sendAuthorizationRequest();

        verify(mockAuthHandler, never()).getAuthorization(any(), any());
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
        when(mockRegistry.isReady()).thenReturn(true);
        ccmFeature = createMockCCMFeature(true);
        ccmService = createMockCCMService(false);
        var poller = createPoller();

        var persistentTaskId = "id";
        var allocationId = 0L;

        var mockPersistentTasksService = mock(PersistentTasksService.class);
        poller.init(mockPersistentTasksService, mock(TaskManager.class), persistentTaskId, allocationId);

        poller.sendAuthorizationRequest();
        poller.sendAuthorizationRequest();

        verify(mockAuthHandler, never()).getAuthorization(any(), any());
        verify(mockPersistentTasksService).sendCompletionRequest(eq(persistentTaskId), eq(allocationId), isNull(), isNull(), any(), any());
    }

    public void testSendsAuthorizationRequest_WhenModelRegistryIsReady() {
        when(mockRegistry.isReady()).thenReturn(true);
        ccmFeature = createMockCCMFeature(true);
        ccmService = createMockCCMService(true);

        var url = "eis-url";
        var sparseModel = createAuthorizedEndpoint(TaskType.SPARSE_EMBEDDING, () -> null);
        givenAuthHandlerReturnsEndpointsForUrl(url, List.of(sparseModel));

        var poller = createPoller();

        var persistentTaskId = "id";
        var allocationId = 0L;

        var mockPersistentTasksService = mock(PersistentTasksService.class);
        poller.init(mockPersistentTasksService, mock(TaskManager.class), persistentTaskId, allocationId);

        sendAuthRequestAndVerifyStoreActionCalledForSparseEndpoints(url, poller, sparseModel);

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
        when(mockRegistry.isReady()).thenReturn(true);
        ccmFeature = createMockCCMFeature(false);
        ccmService = createMockCCMService(false);

        var url = "eis-url";
        var sparseModel = createAuthorizedEndpoint(TaskType.SPARSE_EMBEDDING, () -> null);

        givenAuthHandlerReturnsEndpointsForUrl(url, List.of(sparseModel));

        var poller = createPoller();

        var persistentTaskId = "id";
        var allocationId = 0L;

        var mockPersistentTasksService = mock(PersistentTasksService.class);
        poller.init(mockPersistentTasksService, mock(TaskManager.class), persistentTaskId, allocationId);

        sendAuthRequestAndVerifyStoreActionCalledForSparseEndpoints(url, poller, sparseModel);

        verify(mockPersistentTasksService, never()).sendCompletionRequest(
            eq(persistentTaskId),
            eq(allocationId),
            any(),
            any(),
            any(),
            any()
        );
    }

    public void testSendsAuthorizationRequest_ShouldNotStoreAnyModels_WhenTheyAlreadyExistAndHaveMatchingFingerprintsAndVersions() {
        var url = "eis-url";
        var sparseModel1 = createAuthorizedEndpoint(TaskType.SPARSE_EMBEDDING, () -> null);
        var sparseModel2 = createAuthorizedEndpoint(TaskType.SPARSE_EMBEDDING, () -> "my_matching_fingerprint_2");
        var sparseModel3 = createAuthorizedEndpoint(TaskType.SPARSE_EMBEDDING, () -> "my_matching_fingerprint_3");

        when(mockRegistry.isReady()).thenReturn(true);
        when(mockRegistry.getMinimalServiceSettings(Set.of(sparseModel1.id(), sparseModel2.id(), sparseModel3.id()), false)).thenReturn(
            Map.of(
                sparseModel1.id(),
                createEisSparseSettingsWithFingerprintAndVersion(null, ENDPOINT_SCHEMA_VERSION),
                sparseModel2.id(),
                createEisSparseSettingsWithFingerprintAndVersion("my_matching_fingerprint_2", ENDPOINT_SCHEMA_VERSION),
                sparseModel3.id(),
                createEisSparseSettingsWithFingerprintAndVersion("my_matching_fingerprint_3", ENDPOINT_SCHEMA_VERSION)
            )
        );
        ccmFeature = createMockCCMFeature(true);
        ccmService = createMockCCMService(true);

        givenAuthHandlerReturnsEndpointsForUrl(url, List.of(sparseModel1, sparseModel2, sparseModel3));

        var poller = createPoller();

        poller.sendAuthorizationRequest();
        verify(mockClient, never()).execute(eq(StoreInferenceEndpointsAction.INSTANCE), any(), any());
    }

    public void testSendsAuthorizationRequest_ShouldStoreNewModels() {
        var url = "eis-url";
        var sparseModel1 = createAuthorizedEndpoint(TaskType.SPARSE_EMBEDDING, () -> null);
        var sparseModel2 = createAuthorizedEndpoint(TaskType.SPARSE_EMBEDDING, () -> "my_matching_fingerprint_2");
        var sparseModel3 = createAuthorizedEndpoint(TaskType.SPARSE_EMBEDDING, () -> "some_fingerprint_3");

        when(mockRegistry.isReady()).thenReturn(true);
        when(mockRegistry.getMinimalServiceSettings(Set.of(sparseModel1.id(), sparseModel2.id(), sparseModel3.id()), false)).thenReturn(
            Map.of(
                sparseModel2.id(),
                createEisSparseSettingsWithFingerprintAndVersion("my_matching_fingerprint_2", ENDPOINT_SCHEMA_VERSION)
            )
        );
        ccmFeature = createMockCCMFeature(true);
        ccmService = createMockCCMService(true);

        givenAuthHandlerReturnsEndpointsForUrl(url, List.of(sparseModel1, sparseModel2, sparseModel3));

        var poller = createPoller();

        sendAuthRequestAndVerifyStoreActionCalledForSparseEndpoints(url, poller, sparseModel1, sparseModel3);
    }

    public void testSendsAuthorizationRequest_ShouldStoreModelsWithChangedFingerprint() {
        var url = "eis-url";
        var sparseModel1 = createAuthorizedEndpoint(TaskType.SPARSE_EMBEDDING, () -> "non_null_1");
        var sparseModel2 = createAuthorizedEndpoint(TaskType.SPARSE_EMBEDDING, () -> "my_matching_fingerprint_2");
        var sparseModel3 = createAuthorizedEndpoint(TaskType.SPARSE_EMBEDDING, () -> "my_changed_fingerprint_3");

        when(mockRegistry.isReady()).thenReturn(true);
        when(mockRegistry.getMinimalServiceSettings(Set.of(sparseModel1.id(), sparseModel2.id(), sparseModel3.id()), false)).thenReturn(
            Map.of(
                sparseModel1.id(),
                createEisSparseSettingsWithFingerprintAndVersion(null, ENDPOINT_SCHEMA_VERSION),
                sparseModel2.id(),
                createEisSparseSettingsWithFingerprintAndVersion("my_matching_fingerprint_2", ENDPOINT_SCHEMA_VERSION),
                sparseModel3.id(),
                createEisSparseSettingsWithFingerprintAndVersion("my_original_fingerprint_3", ENDPOINT_SCHEMA_VERSION)
            )
        );
        ccmFeature = createMockCCMFeature(true);
        ccmService = createMockCCMService(true);

        givenAuthHandlerReturnsEndpointsForUrl(url, List.of(sparseModel1, sparseModel2, sparseModel3));

        var poller = createPoller();

        sendAuthRequestAndVerifyStoreActionCalledForSparseEndpoints(url, poller, sparseModel1, sparseModel3);
    }

    public void testSendsAuthorizationRequest_ShouldStoreModelsWithChangedVersion() {
        var url = "eis-url";
        var sparseModel1 = createAuthorizedEndpoint(TaskType.SPARSE_EMBEDDING, () -> null);
        var sparseModel2 = createAuthorizedEndpoint(TaskType.SPARSE_EMBEDDING, () -> "my_matching_fingerprint_2");
        var sparseModel3 = createAuthorizedEndpoint(TaskType.SPARSE_EMBEDDING, () -> "my_matching_fingerprint_3");

        when(mockRegistry.isReady()).thenReturn(true);
        when(mockRegistry.getMinimalServiceSettings(Set.of(sparseModel1.id(), sparseModel2.id(), sparseModel3.id()), false)).thenReturn(
            Map.of(
                sparseModel1.id(),
                createEisSparseSettingsWithFingerprintAndVersion(null, -1L),
                sparseModel2.id(),
                createEisSparseSettingsWithFingerprintAndVersion("my_matching_fingerprint_2", -1L),
                sparseModel3.id(),
                createEisSparseSettingsWithFingerprintAndVersion("my_matching_fingerprint_3", ENDPOINT_SCHEMA_VERSION)
            )
        );
        ccmFeature = createMockCCMFeature(true);
        ccmService = createMockCCMService(true);

        givenAuthHandlerReturnsEndpointsForUrl(url, List.of(sparseModel1, sparseModel2, sparseModel3));

        var poller = createPoller();

        sendAuthRequestAndVerifyStoreActionCalledForSparseEndpoints(url, poller, sparseModel1, sparseModel2);
    }

    public void testDoesNotAttemptToStoreModelIds_ThatHaveATaskTypeThatTheEISIntegration_DoesNotSupport() {
        var url = "eis-url";
        var invalidTaskTypeEndpoint = createInvalidTaskTypeAuthorizedEndpoint();

        when(mockRegistry.isReady()).thenReturn(true);
        ccmFeature = createMockCCMFeature(true);
        ccmService = createMockCCMService(true);

        givenAuthHandlerReturnsEndpointsForUrl(url, List.of(invalidTaskTypeEndpoint));

        var poller = createPoller();

        poller.sendAuthorizationRequest();
        verify(mockClient, never()).execute(eq(StoreInferenceEndpointsAction.INSTANCE), any(), any());
    }

    public void testSendsTwoAuthorizationRequests() throws InterruptedException {
        var url = "eis-url";
        var sparseModel = createAuthorizedEndpoint(TaskType.SPARSE_EMBEDDING, () -> null);

        when(mockRegistry.isReady()).thenReturn(true);
        // Since the registry is already aware of the sparse endpoint, the authorization poller will not consider it a new
        // one and not attempt to store it.
        when(mockRegistry.getMinimalServiceSettings(Set.of(sparseModel.id()), false)).thenReturn(
            Map.of(
                sparseModel.id(),
                new MinimalServiceSettings(
                    "eis",
                    TaskType.SPARSE_EMBEDDING,
                    null,
                    null,
                    null,
                    new EndpointMetadata(
                        EndpointMetadata.Heuristics.EMPTY_INSTANCE,
                        new EndpointMetadata.Internal(null, ENDPOINT_SCHEMA_VERSION),
                        EndpointMetadata.Display.EMPTY_INSTANCE
                    )
                )
            )
        );
        ccmFeature = createMockCCMFeature(true);
        ccmService = createMockCCMService(true);

        givenAuthHandlerReturnsEndpointsForUrl(url, List.of(sparseModel));

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
        verify(mockClient, never()).execute(eq(StoreInferenceEndpointsAction.INSTANCE), any(), any());
    }

    public void testCallsShutdownAndMarksTaskAsCompleted_WhenSchedulingFails() throws InterruptedException {
        var url = "eis-url";
        var sparseModel = createAuthorizedEndpoint(TaskType.SPARSE_EMBEDDING, () -> null);

        when(mockRegistry.isReady()).thenReturn(true);
        ccmFeature = createMockCCMFeature(true);
        ccmService = createMockCCMService(true);

        givenAuthHandlerReturnsEndpointsForUrl(url, List.of(sparseModel));

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

    public void testSendsAuthorizationRequest_ShouldDeleteRemovedEndpoints() {
        Set<String> endpointsToDelete = Set.of("id-1", "id-2");

        when(mockRegistry.isReady()).thenReturn(true);
        when(mockRegistry.getInferenceIds()).thenReturn(Set.of("id-1", "id-2", "id-3"));
        ccmFeature = createMockCCMFeature(true);
        ccmService = createMockCCMService(true);

        givenAuthHandlerRespondsForUrl(randomAlphaOfLength(10), List.of(), endpointsToDelete);

        var poller = createPoller();
        poller.sendAuthorizationRequest();

        verify(mockRegistry).deleteModels(eq(endpointsToDelete), any());
    }

    public void testSendsAuthorizationRequest_ShouldIgnoreRemovedEndpointsNotInRegistry() {
        Set<String> endpointsToDelete = Set.of("id-1", "id-3");

        when(mockRegistry.isReady()).thenReturn(true);
        when(mockRegistry.getInferenceIds()).thenReturn(endpointsToDelete);
        ccmFeature = createMockCCMFeature(true);
        ccmService = createMockCCMService(true);

        givenAuthHandlerRespondsForUrl(randomAlphaOfLength(10), List.of(), Set.of("id-1", "id-2", "id-3", "id-4"));

        var poller = createPoller();
        poller.sendAuthorizationRequest();

        verify(mockRegistry).deleteModels(eq(endpointsToDelete), any());
    }

    public void testSendsAuthorizationRequest_ShouldNotDeleteAnyWhenNoRemovedEndpointIsPresentInRegistry() {
        when(mockRegistry.isReady()).thenReturn(true);
        when(mockRegistry.getInferenceIds()).thenReturn(Set.of("id-1", "id-2"));
        ccmFeature = createMockCCMFeature(true);
        ccmService = createMockCCMService(true);

        givenAuthHandlerRespondsForUrl(randomAlphaOfLength(10), List.of(), Set.of("id-3", "id-4"));

        var poller = createPoller();
        poller.sendAuthorizationRequest();

        verify(mockRegistry, never()).deleteModels(any(), any());
    }

    private AuthorizationPoller createPoller() {
        return createPoller(null);
    }

    private AuthorizationPoller createPoller(Runnable callback) {
        return new AuthorizationPoller(
            TASK_FIELDS,
            createWithEmptySettings(taskQueue.getThreadPool()),
            mockAuthHandler,
            mock(Sender.class),
            eisSettings,
            mockRegistry,
            mockClient,
            ccmFeature,
            ccmService,
            callback,
            inferenceFeatureServiceMock
        );
    }

    private void givenAuthHandlerReturnsEndpointsForUrl(String url, List<AuthorizedEndpoint> endpoints) {
        givenAuthHandlerRespondsForUrl(url, endpoints, Set.of());
    }

    private void givenAuthHandlerRespondsForUrl(String url, List<AuthorizedEndpoint> endpoints, Set<String> removedEndpoints) {
        doAnswer(invocation -> {
            ActionListener<ElasticInferenceServiceAuthorizationModel> listener = invocation.getArgument(0);
            listener.onResponse(
                ElasticInferenceServiceAuthorizationModel.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity(endpoints, removedEndpoints),
                    url
                )
            );
            return Void.TYPE;
        }).when(mockAuthHandler).getAuthorization(any(), any());
    }

    private void givenSchedulingFailure(Exception exception) {
        // Simulate scheduling failure by having the settings throw an exception when queried
        // Throwing an exception should cause the poller to shutdown and mark itself as completed
        eisSettings = mock(ElasticInferenceServiceSettings.class);
        when(eisSettings.isPeriodicAuthorizationEnabled()).thenThrow(exception);
    }

    private void sendAuthRequestAndVerifyStoreActionCalledForSparseEndpoints(
        String url,
        AuthorizationPoller authorizationPoller,
        AuthorizedEndpoint... endpoints
    ) {
        var requestArgCaptor = ArgumentCaptor.forClass(StoreInferenceEndpointsAction.Request.class);
        authorizationPoller.sendAuthorizationRequest();
        verify(mockClient).execute(eq(StoreInferenceEndpointsAction.INSTANCE), requestArgCaptor.capture(), any());
        var capturedRequest = requestArgCaptor.getValue();

        List<? extends Model> expectedModels = Arrays.stream(endpoints)
            .map(
                endpoint -> createSparseEndpoint(
                    endpoint.id(),
                    endpoint.modelName(),
                    url,
                    new EndpointMetadata(
                        new EndpointMetadata.Heuristics(
                            List.of(),
                            StatusHeuristic.fromString(endpoint.status()),
                            endpoint.releaseDate(),
                            endpoint.endOfLifeDate()
                        ),
                        new EndpointMetadata.Internal(endpoint.fingerprint(), ENDPOINT_SCHEMA_VERSION),
                        endpoint.display()
                    )
                )
            )
            .toList();

        assertThat(capturedRequest.getModels(), containsInAnyOrder(expectedModels.toArray()));
    }

    private static EndpointMetadata createEndpointMetadataWithInternal(EndpointMetadata.Internal internal) {
        return new EndpointMetadata(EndpointMetadata.Heuristics.EMPTY_INSTANCE, internal, EndpointMetadata.Display.EMPTY_INSTANCE);
    }

    private static MinimalServiceSettings createEisSparseSettingsWithFingerprintAndVersion(
        @Nullable String fingerprint,
        @Nullable Long version
    ) {
        return new MinimalServiceSettings(
            "eis",
            TaskType.SPARSE_EMBEDDING,
            null,
            null,
            null,
            createEndpointMetadataWithInternal(new EndpointMetadata.Internal(fingerprint, version))
        );
    }
}
