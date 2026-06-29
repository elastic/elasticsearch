/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.StatusHeuristic;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.metadata.EndpointMetadata;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.AuthorizationAction;
import org.elasticsearch.xpack.core.inference.action.StoreInferenceEndpointsAction;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsBuilder;
import org.elasticsearch.xpack.inference.InferenceFeatures;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.features.InferenceFeatureService;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceComponents;
import org.elasticsearch.xpack.inference.services.elastic.authorization.CcmDisabledException;
import org.elasticsearch.xpack.inference.services.elastic.authorization.ElasticInferenceServiceAuthorizationModel;
import org.elasticsearch.xpack.inference.services.elastic.authorization.ElasticInferenceServiceAuthorizationRequestHandler;
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

import static org.elasticsearch.xpack.inference.services.elastic.authorization.EndpointSchemaMigration.ENDPOINT_SCHEMA_VERSION;
import static org.elasticsearch.xpack.inference.services.elastic.ccm.CCMFeatureTests.createMockCCMFeature;
import static org.elasticsearch.xpack.inference.services.elastic.ccm.CCMServiceTests.createMockCCMService;
import static org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntityTests.createAuthorizedEndpoint;
import static org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntityTests.createInvalidTaskTypeAuthorizedEndpoint;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportElasticInferenceServiceAuthorizationActionTests extends ESTestCase {

    private InferenceFeatureService inferenceFeatureServiceMock;
    private ModelRegistry mockRegistry;
    private ElasticInferenceServiceAuthorizationRequestHandler mockAuthHandler;
    private CCMFeature ccmFeature;
    private CCMService ccmService;
    private Client mockClient;

    @Before
    public void init() throws Exception {
        inferenceFeatureServiceMock = mock(InferenceFeatureService.class);
        when(inferenceFeatureServiceMock.hasFeature(InferenceFeatures.ENDPOINT_METADATA_FIELD)).thenReturn(true);
        mockRegistry = mock(ModelRegistry.class);
        mockAuthHandler = mock(ElasticInferenceServiceAuthorizationRequestHandler.class);
        mockClient = mock(Client.class);
        var mockThreadPool = mock(ThreadPool.class);
        when(mockThreadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(mockClient.threadPool()).thenReturn(mockThreadPool);
    }

    public void testDoesNotSendAuthorizationRequest_WhenModelRegistryIsNotReady() {
        when(mockRegistry.isReady()).thenReturn(false);
        ccmFeature = createMockCCMFeature(false);
        ccmService = createMockCCMService(false);
        var action = createAction();

        var future = new TestPlainActionFuture<StoreInferenceEndpointsAction.Response>();
        action.doExecute(null, new AuthorizationAction.Request(), future);

        assertThat(future.actionGet(), is(TransportElasticInferenceServiceAuthorizationAction.EMPTY_RESPONSE));
        verify(mockAuthHandler, never()).getAuthorization(any(), any());
    }

    public void testDoesNotSendAuthorizationRequest_WhenCCMIsDisabled() {
        when(mockRegistry.isReady()).thenReturn(true);
        ccmFeature = createMockCCMFeature(true);
        ccmService = createMockCCMService(false);
        var action = createAction();

        var future = new TestPlainActionFuture<StoreInferenceEndpointsAction.Response>();
        action.doExecute(null, new AuthorizationAction.Request(), future);

        var ex = expectThrows(Exception.class, future::actionGet);
        Throwable cause = ex;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        assertThat(cause, instanceOf(CcmDisabledException.class));
        verify(mockAuthHandler, never()).getAuthorization(any(), any());
    }

    public void testDoesNotSendAuthorizationRequest_WhenClusterDoesNotIncludeMetadata_MappingUpdate() {
        when(mockRegistry.isReady()).thenReturn(true);
        ccmFeature = createMockCCMFeature(true);
        ccmService = createMockCCMService(true);
        when(inferenceFeatureServiceMock.hasFeature(InferenceFeatures.ENDPOINT_METADATA_FIELD)).thenReturn(false);
        var action = createAction();

        var future = new TestPlainActionFuture<StoreInferenceEndpointsAction.Response>();
        action.doExecute(null, new AuthorizationAction.Request(), future);

        assertThat(future.actionGet(), is(TransportElasticInferenceServiceAuthorizationAction.EMPTY_RESPONSE));
        verify(mockAuthHandler, never()).getAuthorization(any(), any());
    }

    public void testSendsAuthorizationRequest_WhenModelRegistryIsReady() {
        when(mockRegistry.isReady()).thenReturn(true);
        ccmFeature = createMockCCMFeature(true);
        ccmService = createMockCCMService(true);

        var url = "eis-url";
        var sparseModel = createAuthorizedEndpoint(TaskType.SPARSE_EMBEDDING, () -> null);
        givenAuthHandlerReturnsEndpointsForUrl(url, List.of(sparseModel));

        var action = createAction();

        sendAuthRequestAndVerifyStoreActionCalledForSparseEndpoints(url, action, sparseModel);
    }

    public void testSendsAuthorizationRequest_WhenCCMIsNotConfigurable() {
        when(mockRegistry.isReady()).thenReturn(true);
        ccmFeature = createMockCCMFeature(false);
        ccmService = createMockCCMService(false);

        var url = "eis-url";
        var sparseModel = createAuthorizedEndpoint(TaskType.SPARSE_EMBEDDING, () -> null);
        givenAuthHandlerReturnsEndpointsForUrl(url, List.of(sparseModel));

        var action = createAction();

        sendAuthRequestAndVerifyStoreActionCalledForSparseEndpoints(url, action, sparseModel);
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

        var action = createAction();

        action.doExecute(null, new AuthorizationAction.Request(), new TestPlainActionFuture<>());
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
        ccmFeature = createMockCCMFeature(true);
        ccmService = createMockCCMService(true);

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
        ccmFeature = createMockCCMFeature(true);
        ccmService = createMockCCMService(true);

        givenAuthHandlerReturnsEndpointsForUrl(url, List.of(sparseModel1, sparseModel2, sparseModel3));

        var action = createAction();

        sendAuthRequestAndVerifyStoreActionCalledForSparseEndpoints(url, action, sparseModel1, sparseModel2);
    }

    public void testDoesNotAttemptToStoreModelIds_ThatHaveATaskTypeThatTheEISIntegration_DoesNotSupport() {
        var url = "eis-url";
        var invalidTaskTypeEndpoint = createInvalidTaskTypeAuthorizedEndpoint();

        when(mockRegistry.isReady()).thenReturn(true);
        ccmFeature = createMockCCMFeature(true);
        ccmService = createMockCCMService(true);

        givenAuthHandlerReturnsEndpointsForUrl(url, List.of(invalidTaskTypeEndpoint));

        var action = createAction();

        action.doExecute(null, new AuthorizationAction.Request(), new TestPlainActionFuture<>());
        verify(mockClient, never()).execute(eq(StoreInferenceEndpointsAction.INSTANCE), any(), any());
    }

    public void testSendsAuthorizationRequest_ShouldDeleteRemovedEndpoints() {
        Set<String> endpointsToDelete = Set.of("id-1", "id-2");

        when(mockRegistry.isReady()).thenReturn(true);
        when(mockRegistry.getInferenceIds()).thenReturn(Set.of("id-1", "id-2", "id-3"));
        ccmFeature = createMockCCMFeature(true);
        ccmService = createMockCCMService(true);

        givenAuthHandlerRespondsForUrl(randomAlphaOfLength(10), List.of(), endpointsToDelete);

        var action = createAction();
        action.doExecute(null, new AuthorizationAction.Request(), new TestPlainActionFuture<>());

        verify(mockRegistry).deleteModels(eq(endpointsToDelete), any());
    }

    public void testSendsAuthorizationRequest_ShouldIgnoreRemovedEndpointsNotInRegistry() {
        Set<String> endpointsToDelete = Set.of("id-1", "id-3");

        when(mockRegistry.isReady()).thenReturn(true);
        when(mockRegistry.getInferenceIds()).thenReturn(endpointsToDelete);
        ccmFeature = createMockCCMFeature(true);
        ccmService = createMockCCMService(true);

        givenAuthHandlerRespondsForUrl(randomAlphaOfLength(10), List.of(), Set.of("id-1", "id-2", "id-3", "id-4"));

        var action = createAction();
        action.doExecute(null, new AuthorizationAction.Request(), new TestPlainActionFuture<>());

        verify(mockRegistry).deleteModels(eq(endpointsToDelete), any());
    }

    public void testSendsAuthorizationRequest_ShouldNotDeleteAnyWhenNoRemovedEndpointIsPresentInRegistry() {
        when(mockRegistry.isReady()).thenReturn(true);
        when(mockRegistry.getInferenceIds()).thenReturn(Set.of("id-1", "id-2"));
        ccmFeature = createMockCCMFeature(true);
        ccmService = createMockCCMService(true);

        givenAuthHandlerRespondsForUrl(randomAlphaOfLength(10), List.of(), Set.of("id-3", "id-4"));

        var action = createAction();
        action.doExecute(null, new AuthorizationAction.Request(), new TestPlainActionFuture<>());

        verify(mockRegistry, never()).deleteModels(any(), any());
    }

    private TransportElasticInferenceServiceAuthorizationAction createAction() {
        return new TransportElasticInferenceServiceAuthorizationAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            mockRegistry,
            mockAuthHandler,
            mock(Sender.class),
            ccmFeature,
            ccmService,
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
            var listener = invocation.<org.elasticsearch.action.ActionListener<ElasticInferenceServiceAuthorizationModel>>getArgument(0);
            listener.onResponse(
                ElasticInferenceServiceAuthorizationModel.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity(endpoints, removedEndpoints),
                    url
                )
            );
            return Void.TYPE;
        }).when(mockAuthHandler).getAuthorization(any(), any());
    }

    private void sendAuthRequestAndVerifyStoreActionCalledForSparseEndpoints(
        String url,
        TransportElasticInferenceServiceAuthorizationAction action,
        AuthorizedEndpoint... endpoints
    ) {
        var requestArgCaptor = ArgumentCaptor.forClass(StoreInferenceEndpointsAction.Request.class);
        action.doExecute(null, new AuthorizationAction.Request(), new TestPlainActionFuture<>());
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
