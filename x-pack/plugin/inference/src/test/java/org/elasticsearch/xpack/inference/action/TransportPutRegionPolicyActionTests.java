/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.metadata.EndpointMetadata;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.PutRegionPolicyAction;
import org.elasticsearch.xpack.core.inference.action.RefreshAuthorizedEndpointsAction;
import org.elasticsearch.xpack.core.inference.action.RegionPolicyResponse;
import org.elasticsearch.xpack.core.inference.regionpolicy.RegionPolicy;
import org.elasticsearch.xpack.inference.common.InferencePreferencesCache;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.elastic.authorization.ElasticInferenceServiceAuthorizationModel;
import org.elasticsearch.xpack.inference.services.elastic.authorization.ElasticInferenceServiceAuthorizationRequestHandler;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportPutRegionPolicyActionTests extends ESTestCase {

    private static final String DENIED_ENDPOINT_ID = "denied-endpoint";

    private ThreadPool threadPool;

    @Before
    public void init() {
        threadPool = createThreadPool(inferenceUtilityExecutors());
    }

    @After
    public void shutdown() throws IOException {
        terminate(threadPool);
    }

    @SuppressWarnings("unchecked")
    public void testSuccessfulPut_InvalidatesCache() {
        var cache = mock(InferencePreferencesCache.class);
        var invalidateCount = new AtomicInteger();
        doAnswer(invocation -> {
            invalidateCount.incrementAndGet();
            ((ActionListener<Void>) invocation.getArgument(0)).onResponse(null);
            return null;
        }).when(cache).invalidate(any());

        var client = clientRespondingToAllRequests();
        var clusterService = mockClusterService(ClusterState.EMPTY_STATE);

        var action = createAction(client, clusterService, cache);

        var listener = new TestPlainActionFuture<RegionPolicyResponse>();
        action.doExecute(mock(Task.class), new PutRegionPolicyAction.Request(new RegionPolicy(List.of("eu"), null)), listener);

        listener.actionGet(TEST_REQUEST_TIMEOUT);
        assertThat(invalidateCount.get(), is(1));
    }

    @SuppressWarnings("unchecked")
    public void testSuccessfulPut_RefreshesAuthorizedEndpoints() {
        var cache = mock(InferencePreferencesCache.class);
        doAnswer(invocation -> {
            ((ActionListener<Void>) invocation.getArgument(0)).onResponse(null);
            return null;
        }).when(cache).invalidate(any());

        var refreshCount = new AtomicInteger();
        var client = new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (request instanceof SearchRequest) {
                    var searchResponse = mock(SearchResponse.class);
                    when(searchResponse.getHits()).thenReturn(SearchHits.EMPTY_WITHOUT_TOTAL_HITS);
                    listener.onResponse((Response) searchResponse);
                } else if (request instanceof IndexRequest) {
                    listener.onResponse((Response) mock(IndexResponse.class));
                } else if (request instanceof RefreshAuthorizedEndpointsAction.Request) {
                    refreshCount.incrementAndGet();
                    listener.onResponse((Response) ActionResponse.Empty.INSTANCE);
                } else {
                    fail("unexpected request type: " + request);
                }
            }
        };

        var clusterService = mockClusterService(ClusterState.EMPTY_STATE);

        var action = createAction(client, clusterService, cache);

        var listener = new TestPlainActionFuture<RegionPolicyResponse>();
        action.doExecute(mock(Task.class), new PutRegionPolicyAction.Request(new RegionPolicy(List.of("eu"), null)), listener);

        listener.actionGet(TEST_REQUEST_TIMEOUT);
        assertThat(refreshCount.get(), is(1));
    }

    @SuppressWarnings("unchecked")
    public void testSuccessfulPut_SwallowsRefreshFailure() {
        var cache = mock(InferencePreferencesCache.class);
        doAnswer(invocation -> {
            ((ActionListener<Void>) invocation.getArgument(0)).onResponse(null);
            return null;
        }).when(cache).invalidate(any());

        var client = new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (request instanceof SearchRequest) {
                    var searchResponse = mock(SearchResponse.class);
                    when(searchResponse.getHits()).thenReturn(SearchHits.EMPTY_WITHOUT_TOTAL_HITS);
                    listener.onResponse((Response) searchResponse);
                } else if (request instanceof IndexRequest) {
                    listener.onResponse((Response) mock(IndexResponse.class));
                } else if (request instanceof RefreshAuthorizedEndpointsAction.Request) {
                    listener.onFailure(new RuntimeException("refresh failed"));
                } else {
                    fail("unexpected request type: " + request);
                }
            }
        };

        var clusterService = mockClusterService(ClusterState.EMPTY_STATE);

        var action = createAction(client, clusterService, cache);

        var listener = new TestPlainActionFuture<RegionPolicyResponse>();
        action.doExecute(mock(Task.class), new PutRegionPolicyAction.Request(new RegionPolicy(List.of("eu"), null)), listener);

        // The put should still succeed even though the refresh failed
        var response = listener.actionGet(TEST_REQUEST_TIMEOUT);
        assertThat(response.regionPolicy().regionPolicy().allowedGeos(), is(List.of("eu")));
    }

    @SuppressWarnings("unchecked")
    public void testPut_WhenForceTrue_SkipsCheck() {
        var cache = noopInvalidatingCache();
        var client = clientRespondingToAllRequests();
        var clusterService = mockClusterService(ClusterState.EMPTY_STATE);
        var authorizationHandler = mock(ElasticInferenceServiceAuthorizationRequestHandler.class);

        var action = new TransportPutRegionPolicyAction(
            Settings.EMPTY,
            mock(TransportService.class),
            threadPool,
            mock(ActionFilters.class),
            client,
            clusterService,
            mock(FeatureService.class),
            cache,
            authorizationHandler,
            mock(Sender.class)
        );

        var listener = new TestPlainActionFuture<RegionPolicyResponse>();
        action.doExecute(mock(Task.class), new PutRegionPolicyAction.Request(new RegionPolicy(List.of("eu"), null), true), listener);

        listener.actionGet(TEST_REQUEST_TIMEOUT);
        verify(authorizationHandler, never()).getAuthorizationWithPreferences(any(), any(), any());
    }

    @SuppressWarnings("unchecked")
    public void testPut_WhenNoDeniedEndpoints_Succeeds() {
        var cache = noopInvalidatingCache();
        var client = clientRespondingToAllRequests();
        var clusterService = mockClusterService(ClusterState.EMPTY_STATE);

        var authModel = mock(ElasticInferenceServiceAuthorizationModel.class);
        when(authModel.getEndpointIds()).thenReturn(Set.of());
        when(authModel.getEndpoints(any())).thenReturn(List.of());

        var authorizationHandler = mockAuthorizationHandlerReturning(authModel);

        var action = new TransportPutRegionPolicyAction(
            Settings.EMPTY,
            mock(TransportService.class),
            threadPool,
            mock(ActionFilters.class),
            client,
            clusterService,
            mock(FeatureService.class),
            cache,
            authorizationHandler,
            mock(Sender.class)
        );

        var listener = new TestPlainActionFuture<RegionPolicyResponse>();
        action.doExecute(mock(Task.class), new PutRegionPolicyAction.Request(new RegionPolicy(List.of("eu"), null)), listener);

        var response = listener.actionGet(TEST_REQUEST_TIMEOUT);
        assertThat(response.regionPolicy().regionPolicy().allowedGeos(), is(List.of("eu")));
    }

    @SuppressWarnings("unchecked")
    public void testPut_WhenDeniedEndpointIsNotInUse_Succeeds() {
        var cache = noopInvalidatingCache();
        var client = clientRespondingToAllRequests();
        var clusterService = mockClusterService(ClusterState.EMPTY_STATE);

        var authModel = authModelWithDeniedEndpoint(DENIED_ENDPOINT_ID);
        var authorizationHandler = mockAuthorizationHandlerReturning(authModel);

        var action = new TransportPutRegionPolicyAction(
            Settings.EMPTY,
            mock(TransportService.class),
            threadPool,
            mock(ActionFilters.class),
            client,
            clusterService,
            mock(FeatureService.class),
            cache,
            authorizationHandler,
            mock(Sender.class)
        );

        var listener = new TestPlainActionFuture<RegionPolicyResponse>();
        action.doExecute(mock(Task.class), new PutRegionPolicyAction.Request(new RegionPolicy(List.of("eu"), null)), listener);

        var response = listener.actionGet(TEST_REQUEST_TIMEOUT);
        assertThat(response.regionPolicy().regionPolicy().allowedGeos(), is(List.of("eu")));
    }

    @SuppressWarnings("unchecked")
    public void testPut_WhenDeniedEndpointIsInUse_FailsWith409() throws IOException {
        var cache = noopInvalidatingCache();
        var client = clientRespondingToAllRequests();
        var clusterService = mockClusterService(clusterStateWithPipelineReferencing(DENIED_ENDPOINT_ID));

        var authModel = authModelWithDeniedEndpoint(DENIED_ENDPOINT_ID);
        var authorizationHandler = mockAuthorizationHandlerReturning(authModel);

        var action = new TransportPutRegionPolicyAction(
            Settings.EMPTY,
            mock(TransportService.class),
            threadPool,
            mock(ActionFilters.class),
            client,
            clusterService,
            mock(FeatureService.class),
            cache,
            authorizationHandler,
            mock(Sender.class)
        );

        var listener = new TestPlainActionFuture<RegionPolicyResponse>();
        action.doExecute(mock(Task.class), new PutRegionPolicyAction.Request(new RegionPolicy(List.of("eu"), null)), listener);

        var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
        assertThat(exception, instanceOf(ElasticsearchStatusException.class));
        assertThat(exception.status(), is(RestStatus.CONFLICT));
        assertThat(exception.getMessage(), containsString(DENIED_ENDPOINT_ID));
        assertThat(exception.getMetadata(TransportPutRegionPolicyAction.DENIED_ENDPOINT_IDS_METADATA_KEY), contains(DENIED_ENDPOINT_ID));
        assertThat(
            exception.getMetadata(TransportPutRegionPolicyAction.REFERENCING_PIPELINES_METADATA_KEY),
            contains(DENIED_ENDPOINT_ID + ":pipeline_referencing_" + DENIED_ENDPOINT_ID)
        );
        assertThat(exception.getMetadata(TransportPutRegionPolicyAction.REFERENCING_INDEXES_METADATA_KEY), empty());
    }

    private static ElasticInferenceServiceAuthorizationModel authModelWithDeniedEndpoint(String endpointId) {
        var endpointMetadata = new EndpointMetadata(
            EndpointMetadata.Heuristics.EMPTY_INSTANCE,
            EndpointMetadata.Internal.EMPTY_INSTANCE,
            EndpointMetadata.Display.EMPTY_INSTANCE,
            List.of(),
            true
        );

        var configurations = mock(ModelConfigurations.class);
        when(configurations.getEndpointMetadataOrEmpty()).thenReturn(endpointMetadata);

        var model = mock(Model.class);
        when(model.getInferenceEntityId()).thenReturn(endpointId);
        when(model.getConfigurations()).thenReturn(configurations);

        var authModel = mock(ElasticInferenceServiceAuthorizationModel.class);
        when(authModel.getEndpointIds()).thenReturn(Set.of(endpointId));
        when(authModel.getEndpoints(eq(Set.of(endpointId)))).thenReturn(List.of(model));
        return authModel;
    }

    private ElasticInferenceServiceAuthorizationRequestHandler mockAuthorizationHandlerReturning(
        ElasticInferenceServiceAuthorizationModel authModel
    ) {
        var authorizationHandler = mock(ElasticInferenceServiceAuthorizationRequestHandler.class);
        doAnswer(invocation -> {
            ActionListener<ElasticInferenceServiceAuthorizationModel> listener = invocation.getArgument(0);
            listener.onResponse(authModel);
            return null;
        }).when(authorizationHandler).getAuthorizationWithPreferences(any(), any(), any());
        return authorizationHandler;
    }

    private static ClusterState clusterStateWithPipelineReferencing(String endpointId) throws IOException {
        Map<String, Object> inferenceProcessor = Map.of(
            "inference",
            Map.of("model_id", endpointId, "target_field", "new_field", "field_map", Map.of("source", "dest"))
        );

        try (
            var xContentBuilder = XContentFactory.jsonBuilder()
                .map(Collections.singletonMap("processors", Collections.singletonList(inferenceProcessor)))
        ) {
            var pipelineConfiguration = new PipelineConfiguration(
                "pipeline_referencing_" + endpointId,
                BytesReference.bytes(xContentBuilder),
                XContentType.JSON
            );
            var ingestMetadata = new IngestMetadata(Map.of(pipelineConfiguration.getId(), pipelineConfiguration));
            var projectMetadata = ProjectMetadata.builder(Metadata.DEFAULT_PROJECT_ID).putCustom(IngestMetadata.TYPE, ingestMetadata);
            var metadata = Metadata.builder().put(projectMetadata).build();
            return ClusterState.builder(new ClusterName("_name")).metadata(metadata).build();
        }
    }

    private ClusterService mockClusterService(ClusterState clusterState) {
        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        return clusterService;
    }

    @SuppressWarnings("unchecked")
    private InferencePreferencesCache noopInvalidatingCache() {
        var cache = mock(InferencePreferencesCache.class);
        doAnswer(invocation -> {
            ((ActionListener<Void>) invocation.getArgument(0)).onResponse(null);
            return null;
        }).when(cache).invalidate(any());
        return cache;
    }

    private NoOpClient clientRespondingToAllRequests() {
        return new NoOpClient(threadPool) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (request instanceof SearchRequest) {
                    var searchResponse = mock(SearchResponse.class);
                    when(searchResponse.getHits()).thenReturn(SearchHits.EMPTY_WITHOUT_TOTAL_HITS);
                    listener.onResponse((Response) searchResponse);
                } else if (request instanceof IndexRequest) {
                    listener.onResponse((Response) mock(IndexResponse.class));
                } else if (request instanceof RefreshAuthorizedEndpointsAction.Request) {
                    listener.onResponse((Response) ActionResponse.Empty.INSTANCE);
                } else {
                    fail("unexpected request type: " + request);
                }
            }
        };
    }

    private TransportPutRegionPolicyAction createAction(NoOpClient client, ClusterService clusterService, InferencePreferencesCache cache) {
        return new TransportPutRegionPolicyAction(
            Settings.EMPTY,
            mock(TransportService.class),
            threadPool,
            mock(ActionFilters.class),
            client,
            clusterService,
            mock(FeatureService.class),
            cache,
            mockAuthorizationHandlerReturning(ElasticInferenceServiceAuthorizationModel.unauthorized()),
            mock(Sender.class)
        );
    }
}
