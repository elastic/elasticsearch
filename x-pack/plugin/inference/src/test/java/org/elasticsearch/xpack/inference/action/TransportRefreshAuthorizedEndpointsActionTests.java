/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.StatusHeuristic;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.metadata.EndpointMetadata;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.InternalDeleteInferenceEndpointsAction;
import org.elasticsearch.xpack.core.inference.action.RefreshAuthorizedEndpointsAction;
import org.elasticsearch.xpack.core.inference.action.StoreInferenceEndpointsAction;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsBuilder;
import org.elasticsearch.xpack.core.inference.results.ModelStoreResponse;
import org.elasticsearch.xpack.inference.InferenceFeatures;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.features.InferenceFeatureService;
import org.elasticsearch.xpack.inference.registry.ClearInferenceEndpointCacheAction;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceComponents;
import org.elasticsearch.xpack.inference.services.elastic.authorization.ElasticInferenceServiceAuthorizationModel;
import org.elasticsearch.xpack.inference.services.elastic.authorization.ElasticInferenceServiceAuthorizationRequestHandler;
import org.elasticsearch.xpack.inference.services.elastic.compatibility.CompletionCompatibilityService;
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

import static org.elasticsearch.xpack.inference.services.elastic.authorization.EndpointSchemaMigration.ENDPOINT_SCHEMA_VERSION;
import static org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntityTests.createAuthorizedEndpoint;
import static org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntityTests.createInvalidTaskTypeAuthorizedEndpoint;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportRefreshAuthorizedEndpointsActionTests extends ESTestCase {

    private static final String URL = "eis-url";
    private static final String STORED_INFERENCE_ID = "stored-inference-id";
    private static final String FAILED_INFERENCE_ID = "failed-inference-id";

    private InferenceFeatureService inferenceFeatureServiceMock;
    private ModelRegistry mockRegistry;
    private ElasticInferenceServiceAuthorizationRequestHandler mockAuthHandler;
    private Client mockClient;

    @Before
    public void init() throws Exception {
        inferenceFeatureServiceMock = mock(InferenceFeatureService.class);
        when(inferenceFeatureServiceMock.hasFeature(InferenceFeatures.ENDPOINT_METADATA_FIELD)).thenReturn(true);
        when(inferenceFeatureServiceMock.hasFeature(InferenceFeatures.INTERNAL_DELETE_INFERENCE_ENDPOINTS_ACTION)).thenReturn(true);
        mockRegistry = mock(ModelRegistry.class);
        mockAuthHandler = mock(ElasticInferenceServiceAuthorizationRequestHandler.class);
        mockClient = mock(Client.class);
        var mockThreadPool = mock(ThreadPool.class);
        when(mockThreadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(mockClient.threadPool()).thenReturn(mockThreadPool);
    }

    public void testDoesNotSendAuthorizationRequest_WhenModelRegistryIsNotReady() {
        when(mockRegistry.isReady()).thenReturn(false);
        var action = createAction();

        var future = new TestPlainActionFuture<ActionResponse.Empty>();
        action.doExecute(null, new RefreshAuthorizedEndpointsAction.Request(), future);

        assertThat(future.actionGet(), is(ActionResponse.Empty.INSTANCE));
        verify(mockAuthHandler, never()).getAuthorization(any(), any());
    }

    public void testDoesNotSendAuthorizationRequest_WhenClusterDoesNotIncludeMetadata_MappingUpdate() {
        when(mockRegistry.isReady()).thenReturn(true);
        when(inferenceFeatureServiceMock.hasFeature(InferenceFeatures.ENDPOINT_METADATA_FIELD)).thenReturn(false);
        var action = createAction();

        var future = new TestPlainActionFuture<ActionResponse.Empty>();
        action.doExecute(null, new RefreshAuthorizedEndpointsAction.Request(), future);

        assertThat(future.actionGet(), is(ActionResponse.Empty.INSTANCE));
        verify(mockAuthHandler, never()).getAuthorization(any(), any());
    }

    public void testDoesNotSendAuthorizationRequest_WhenClusterMissingInternalDeleteEndpointsFeature() {
        when(mockRegistry.isReady()).thenReturn(true);
        when(inferenceFeatureServiceMock.hasFeature(InferenceFeatures.INTERNAL_DELETE_INFERENCE_ENDPOINTS_ACTION)).thenReturn(false);
        var action = createAction();

        var future = new TestPlainActionFuture<ActionResponse.Empty>();
        action.doExecute(null, new RefreshAuthorizedEndpointsAction.Request(), future);

        assertThat(future.actionGet(), is(ActionResponse.Empty.INSTANCE));
        verify(mockAuthHandler, never()).getAuthorization(any(), any());
    }

    public void testSendsAuthorizationRequest_WhenModelRegistryIsReady() {
        when(mockRegistry.isReady()).thenReturn(true);

        var url = "eis-url";
        var sparseModel = createAuthorizedEndpoint(TaskType.SPARSE_EMBEDDING, () -> null);
        givenAuthHandlerReturnsEndpointsForUrl(url, List.of(sparseModel));

        var action = createAction();

        sendAuthRequestAndVerifyStoreActionCalledForSparseEndpoints(url, action, sparseModel);
    }

    public void testRefreshesCacheOnAllNodes_WhenAtLeastOneEndpointIsStored() {
        when(mockRegistry.isReady()).thenReturn(true);

        var sparseModel = createAuthorizedEndpoint(TaskType.SPARSE_EMBEDDING, () -> null);
        givenAuthHandlerReturnsEndpointsForUrl(URL, List.of(sparseModel));
        givenStoreActionRespondsWith(
            new ModelStoreResponse(STORED_INFERENCE_ID, RestStatus.OK, null),
            new ModelStoreResponse(FAILED_INFERENCE_ID, RestStatus.INTERNAL_SERVER_ERROR, new RuntimeException("boom"))
        );

        var action = createAction();
        action.doExecute(null, new RefreshAuthorizedEndpointsAction.Request(), new TestPlainActionFuture<>());

        verify(mockClient).execute(eq(ClearInferenceEndpointCacheAction.INSTANCE), any(), any());
    }

    public void testDoesNotRefreshCacheOnAllNodes_WhenNoEndpointsAreStored() {
        when(mockRegistry.isReady()).thenReturn(true);

        var sparseModel = createAuthorizedEndpoint(TaskType.SPARSE_EMBEDDING, () -> null);
        givenAuthHandlerReturnsEndpointsForUrl(URL, List.of(sparseModel));
        givenStoreActionRespondsWith();

        var action = createAction();
        action.doExecute(null, new RefreshAuthorizedEndpointsAction.Request(), new TestPlainActionFuture<>());

        verify(mockClient, never()).execute(eq(ClearInferenceEndpointCacheAction.INSTANCE), any(), any());
    }

    public void testDoesNotRefreshCacheOnAllNodes_WhenAllEndpointsFailToStore() {
        when(mockRegistry.isReady()).thenReturn(true);

        var sparseModel = createAuthorizedEndpoint(TaskType.SPARSE_EMBEDDING, () -> null);
        givenAuthHandlerReturnsEndpointsForUrl(URL, List.of(sparseModel));
        givenStoreActionRespondsWith(
            new ModelStoreResponse(FAILED_INFERENCE_ID, RestStatus.INTERNAL_SERVER_ERROR, new RuntimeException("boom"))
        );

        var action = createAction();
        action.doExecute(null, new RefreshAuthorizedEndpointsAction.Request(), new TestPlainActionFuture<>());

        verify(mockClient, never()).execute(eq(ClearInferenceEndpointCacheAction.INSTANCE), any(), any());
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

        givenAuthHandlerReturnsEndpointsForUrl(url, List.of(sparseModel1, sparseModel2, sparseModel3));

        var action = createAction();

        action.doExecute(null, new RefreshAuthorizedEndpointsAction.Request(), new TestPlainActionFuture<>());
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

        givenAuthHandlerReturnsEndpointsForUrl(url, List.of(sparseModel1, sparseModel2, sparseModel3));

        var action = createAction();

        sendAuthRequestAndVerifyStoreActionCalledForSparseEndpoints(url, action, sparseModel1, sparseModel3);
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

        givenAuthHandlerReturnsEndpointsForUrl(url, List.of(sparseModel1, sparseModel2, sparseModel3));

        var action = createAction();

        sendAuthRequestAndVerifyStoreActionCalledForSparseEndpoints(url, action, sparseModel1, sparseModel3);
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

        givenAuthHandlerReturnsEndpointsForUrl(url, List.of(sparseModel1, sparseModel2, sparseModel3));

        var action = createAction();

        sendAuthRequestAndVerifyStoreActionCalledForSparseEndpoints(url, action, sparseModel1, sparseModel2);
    }

    public void testDoesNotAttemptToStoreModelIds_ThatHaveATaskTypeThatTheEISIntegration_DoesNotSupport() {
        var url = "eis-url";
        var invalidTaskTypeEndpoint = createInvalidTaskTypeAuthorizedEndpoint();

        when(mockRegistry.isReady()).thenReturn(true);

        givenAuthHandlerReturnsEndpointsForUrl(url, List.of(invalidTaskTypeEndpoint));

        var action = createAction();

        action.doExecute(null, new RefreshAuthorizedEndpointsAction.Request(), new TestPlainActionFuture<>());
        verify(mockClient, never()).execute(eq(StoreInferenceEndpointsAction.INSTANCE), any(), any());
    }

    public void testSendsAuthorizationRequest_ShouldDeleteRemovedEndpoints() {
        Set<String> endpointsToDelete = Set.of("id-1", "id-2");

        when(mockRegistry.isReady()).thenReturn(true);
        when(mockRegistry.getInferenceIds()).thenReturn(Set.of("id-1", "id-2", "id-3"));

        givenAuthHandlerRespondsForUrl(randomAlphaOfLength(10), List.of(), endpointsToDelete);
        givenDeleteActionRespondsWith(AcknowledgedResponse.TRUE);

        var action = createAction();
        action.doExecute(null, new RefreshAuthorizedEndpointsAction.Request(), new TestPlainActionFuture<>());

        var requestArgCaptor = ArgumentCaptor.forClass(InternalDeleteInferenceEndpointsAction.Request.class);
        verify(mockClient).execute(eq(InternalDeleteInferenceEndpointsAction.INSTANCE), requestArgCaptor.capture(), any());
        assertThat(requestArgCaptor.getValue().getInferenceEntityIds(), is(endpointsToDelete));
        verify(mockRegistry, never()).deleteModel(any(), any());
    }

    public void testSendsAuthorizationRequest_ShouldIgnoreRemovedEndpointsNotInRegistry() {
        Set<String> endpointsToDelete = Set.of("id-1", "id-3");

        when(mockRegistry.isReady()).thenReturn(true);
        when(mockRegistry.getInferenceIds()).thenReturn(endpointsToDelete);

        givenAuthHandlerRespondsForUrl(randomAlphaOfLength(10), List.of(), Set.of("id-1", "id-2", "id-3", "id-4"));
        givenDeleteActionRespondsWith(AcknowledgedResponse.TRUE);

        var action = createAction();
        action.doExecute(null, new RefreshAuthorizedEndpointsAction.Request(), new TestPlainActionFuture<>());

        var requestArgCaptor = ArgumentCaptor.forClass(InternalDeleteInferenceEndpointsAction.Request.class);
        verify(mockClient).execute(eq(InternalDeleteInferenceEndpointsAction.INSTANCE), requestArgCaptor.capture(), any());
        assertThat(requestArgCaptor.getValue().getInferenceEntityIds(), is(endpointsToDelete));
        verify(mockRegistry, never()).deleteModel(any(), any());
    }

    public void testSendsAuthorizationRequest_ShouldNotDeleteAnyWhenNoRemovedEndpointIsPresentInRegistry() {
        when(mockRegistry.isReady()).thenReturn(true);
        when(mockRegistry.getInferenceIds()).thenReturn(Set.of("id-1", "id-2"));

        givenAuthHandlerRespondsForUrl(randomAlphaOfLength(10), List.of(), Set.of("id-3", "id-4"));

        var action = createAction();
        action.doExecute(null, new RefreshAuthorizedEndpointsAction.Request(), new TestPlainActionFuture<>());

        verify(mockClient, never()).execute(eq(InternalDeleteInferenceEndpointsAction.INSTANCE), any(), any());
    }

    private TransportRefreshAuthorizedEndpointsAction createAction() {
        return new TransportRefreshAuthorizedEndpointsAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            mockRegistry,
            mockAuthHandler,
            mock(Sender.class),
            inferenceFeatureServiceMock,
            mockClient
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

    private void givenAuthHandlerReturnsEndpointsForUrl(String url, List<AuthorizedEndpoint> endpoints) {
        givenAuthHandlerRespondsForUrl(url, endpoints, Set.of());
    }

    private void givenAuthHandlerRespondsForUrl(String url, List<AuthorizedEndpoint> endpoints, Set<String> removedEndpoints) {
        doAnswer(invocation -> {
            var listener = invocation.<ActionListener<ElasticInferenceServiceAuthorizationModel>>getArgument(0);
            listener.onResponse(
                ElasticInferenceServiceAuthorizationModel.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity(endpoints, removedEndpoints),
                    url,
                    new CompletionCompatibilityService(mock(ClusterService.class), mock(FeatureService.class))
                )
            );
            return Void.TYPE;
        }).when(mockAuthHandler).getAuthorization(any(), any());
    }

    private void givenStoreActionRespondsWith(ModelStoreResponse... results) {
        doAnswer(invocation -> {
            ActionListener<StoreInferenceEndpointsAction.Response> listener = invocation.getArgument(2);
            listener.onResponse(new StoreInferenceEndpointsAction.Response(List.of(results)));
            return null;
        }).when(mockClient).execute(eq(StoreInferenceEndpointsAction.INSTANCE), any(), any());
    }

    private void givenDeleteActionRespondsWith(AcknowledgedResponse response) {
        doAnswer(invocation -> {
            ActionListener<AcknowledgedResponse> listener = invocation.getArgument(2);
            listener.onResponse(response);
            return null;
        }).when(mockClient).execute(eq(InternalDeleteInferenceEndpointsAction.INSTANCE), any(), any());
    }

    private void sendAuthRequestAndVerifyStoreActionCalledForSparseEndpoints(
        String url,
        TransportRefreshAuthorizedEndpointsAction action,
        AuthorizedEndpoint... endpoints
    ) {
        var requestArgCaptor = ArgumentCaptor.forClass(StoreInferenceEndpointsAction.Request.class);
        action.doExecute(null, new RefreshAuthorizedEndpointsAction.Request(), new TestPlainActionFuture<>());
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
                        endpoint.display(),
                        List.of(),
                        false
                    )
                )
            )
            .toList();

        assertThat(capturedRequest.getModels(), containsInAnyOrder(expectedModels.toArray()));
    }

    private static EndpointMetadata createEndpointMetadataWithInternal(EndpointMetadata.Internal internal) {
        return new EndpointMetadata(
            EndpointMetadata.Heuristics.EMPTY_INSTANCE,
            internal,
            EndpointMetadata.Display.EMPTY_INSTANCE,
            List.of(),
            false
        );
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
