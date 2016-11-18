/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.persistence;

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;

public class MockClientBuilder {
    @Mock
    private Client client;

    @Mock
    private AdminClient adminClient;
    @Mock
    private ClusterAdminClient clusterAdminClient;
    @Mock
    private IndicesAdminClient indicesAdminClient;
    @Mock
    private ActionFuture<IndicesExistsResponse> indexNotExistsResponseFuture;

    public MockClientBuilder(String clusterName) {
        client = mock(Client.class);
        adminClient = mock(AdminClient.class);
        clusterAdminClient = mock(ClusterAdminClient.class);
        indicesAdminClient = mock(IndicesAdminClient.class);

        when(client.admin()).thenReturn(adminClient);
        when(adminClient.cluster()).thenReturn(clusterAdminClient);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
        Settings settings = Settings.builder().put("cluster.name", clusterName).build();
        when(client.settings()).thenReturn(settings);
    }

    public MockClientBuilder addClusterStatusYellowResponse(String index, TimeValue timeout)
            throws InterruptedException, ExecutionException {
        ClusterHealthRequestBuilder clusterHealthRequestBuilder = mock(ClusterHealthRequestBuilder.class);
        when(clusterAdminClient.prepareHealth(index)).thenReturn(clusterHealthRequestBuilder);
        when(clusterHealthRequestBuilder.get(timeout)).thenReturn(mock(ClusterHealthResponse.class));
        return this;
    }

    public MockClientBuilder addClusterStatusYellowResponse(String index, TimeValue timeout, Exception e)
            throws InterruptedException, ExecutionException {
        ClusterHealthRequestBuilder clusterHealthRequestBuilder = mock(ClusterHealthRequestBuilder.class);
        when(clusterAdminClient.prepareHealth(index)).thenReturn(clusterHealthRequestBuilder);
        doAnswer(invocation -> {
            throw e;
        }).when(clusterHealthRequestBuilder).get(eq(timeout));
        return this;
    }

    @SuppressWarnings({ "unchecked" })
    public MockClientBuilder addClusterStatusYellowResponse() throws InterruptedException, ExecutionException {
        ListenableActionFuture<ClusterHealthResponse> actionFuture = mock(ListenableActionFuture.class);
        ClusterHealthRequestBuilder clusterHealthRequestBuilder = mock(ClusterHealthRequestBuilder.class);

        when(clusterAdminClient.prepareHealth()).thenReturn(clusterHealthRequestBuilder);
        when(clusterHealthRequestBuilder.setWaitForYellowStatus()).thenReturn(clusterHealthRequestBuilder);
        when(clusterHealthRequestBuilder.execute()).thenReturn(actionFuture);
        when(actionFuture.actionGet()).thenReturn(mock(ClusterHealthResponse.class));
        return this;
    }

    @SuppressWarnings({ "unchecked" })
    public MockClientBuilder addClusterStatusYellowResponse(String index) throws InterruptedException, ExecutionException {
        ListenableActionFuture<ClusterHealthResponse> actionFuture = mock(ListenableActionFuture.class);
        ClusterHealthRequestBuilder clusterHealthRequestBuilder = mock(ClusterHealthRequestBuilder.class);

        when(clusterAdminClient.prepareHealth(index)).thenReturn(clusterHealthRequestBuilder);
        when(clusterHealthRequestBuilder.setWaitForYellowStatus()).thenReturn(clusterHealthRequestBuilder);
        when(clusterHealthRequestBuilder.execute()).thenReturn(actionFuture);
        when(actionFuture.actionGet()).thenReturn(mock(ClusterHealthResponse.class));
        return this;
    }

    @SuppressWarnings({ "unchecked" })
    public MockClientBuilder addClusterStatusRedResponse() throws InterruptedException, ExecutionException {
        ListenableActionFuture<ClusterHealthResponse> actionFuture = mock(ListenableActionFuture.class);
        ClusterHealthRequestBuilder clusterHealthRequestBuilder = mock(ClusterHealthRequestBuilder.class);

        when(clusterAdminClient.prepareHealth()).thenReturn(clusterHealthRequestBuilder);
        when(clusterHealthRequestBuilder.setWaitForYellowStatus()).thenReturn(clusterHealthRequestBuilder);
        when(clusterHealthRequestBuilder.execute()).thenReturn(actionFuture);
        ClusterHealthResponse response = mock(ClusterHealthResponse.class);
        when(response.getStatus()).thenReturn(ClusterHealthStatus.RED);
        when(actionFuture.actionGet()).thenReturn(response);
        return this;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public MockClientBuilder addIndicesExistsResponse(String index, boolean exists) throws InterruptedException, ExecutionException {
        ActionFuture actionFuture = mock(ActionFuture.class);
        ArgumentCaptor<IndicesExistsRequest> requestCaptor = ArgumentCaptor.forClass(IndicesExistsRequest.class);

        when(indicesAdminClient.exists(requestCaptor.capture())).thenReturn(actionFuture);
        doAnswer(invocation -> {
            IndicesExistsRequest request = (IndicesExistsRequest) invocation.getArguments()[0];
            return request.indices()[0].equals(index) ? actionFuture : null;
        }).when(indicesAdminClient).exists(any(IndicesExistsRequest.class));
        when(actionFuture.get()).thenReturn(new IndicesExistsResponse(exists));
        when(actionFuture.actionGet()).thenReturn(new IndicesExistsResponse(exists));
        return this;
    }

    @SuppressWarnings({ "unchecked" })
    public MockClientBuilder addIndicesDeleteResponse(String index, boolean exists, boolean exception,
            ActionListener<Boolean> actionListener) throws InterruptedException, ExecutionException, IOException {
        DeleteIndexResponse response = DeleteIndexAction.INSTANCE.newResponse();
        StreamInput si = mock(StreamInput.class);
        when(si.readByte()).thenReturn((byte) 0x41);
        when(si.readMap()).thenReturn(mock(Map.class));
        response.readFrom(si);

        doAnswer(invocation -> {
            DeleteIndexRequest deleteIndexRequest = (DeleteIndexRequest) invocation.getArguments()[0];
            assertArrayEquals(new String[] { index }, deleteIndexRequest.indices());
            if (exception) {
                actionListener.onFailure(new InterruptedException());
            } else {
                actionListener.onResponse(true);
            }
            return null;
        }).when(indicesAdminClient).delete(any(DeleteIndexRequest.class), any(ActionListener.class));
        return this;
    }

    public MockClientBuilder prepareGet(String index, String type, String id, GetResponse response) {
        GetRequestBuilder getRequestBuilder = mock(GetRequestBuilder.class);
        when(getRequestBuilder.get()).thenReturn(response);
        when(getRequestBuilder.setFetchSource(false)).thenReturn(getRequestBuilder);
        when(client.prepareGet(index, type, id)).thenReturn(getRequestBuilder);
        return this;
    }

    public MockClientBuilder prepareGet(String index, String type, String id, Exception exception) {
        GetRequestBuilder getRequestBuilder = mock(GetRequestBuilder.class);
        doAnswer(invocation -> {
            throw exception;
        }).when(getRequestBuilder).get();
        when(getRequestBuilder.setFetchSource(false)).thenReturn(getRequestBuilder);
        when(client.prepareGet(index, type, id)).thenReturn(getRequestBuilder);
        return this;
    }

    public MockClientBuilder prepareCreate(String index) {
        CreateIndexRequestBuilder createIndexRequestBuilder = mock(CreateIndexRequestBuilder.class);
        CreateIndexResponse response = mock(CreateIndexResponse.class);
        when(createIndexRequestBuilder.setSettings(any(Settings.Builder.class))).thenReturn(createIndexRequestBuilder);
        when(createIndexRequestBuilder.addMapping(any(String.class), any(XContentBuilder.class))).thenReturn(createIndexRequestBuilder);
        when(createIndexRequestBuilder.get()).thenReturn(response);
        when(indicesAdminClient.prepareCreate(eq(index))).thenReturn(createIndexRequestBuilder);
        return this;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public MockClientBuilder createIndexRequest(String index) {
        ArgumentMatcher<CreateIndexRequest> argumentMatcher = new ArgumentMatcher<CreateIndexRequest>() {
            @Override
            public boolean matches(Object o) {
                return index.equals(((CreateIndexRequest) o).index());
            }
        };
        doAnswer(invocation -> {
            ((ActionListener) invocation.getArguments()[1]).onResponse(mock(CreateIndexResponse.class));
            return null;
        }).when(indicesAdminClient).create(argThat(argumentMatcher), any(ActionListener.class));
        return this;
    }

    public MockClientBuilder prepareCreate(String index, RuntimeException e) {
        CreateIndexRequestBuilder createIndexRequestBuilder = mock(CreateIndexRequestBuilder.class);
        when(createIndexRequestBuilder.setSettings(any(Settings.Builder.class))).thenReturn(createIndexRequestBuilder);
        when(createIndexRequestBuilder.addMapping(any(String.class), any(XContentBuilder.class))).thenReturn(createIndexRequestBuilder);
        doThrow(e).when(createIndexRequestBuilder).get();
        when(indicesAdminClient.prepareCreate(eq(index))).thenReturn(createIndexRequestBuilder);
        return this;
    }

    public MockClientBuilder prepareCreate(String index, Exception e) {
        CreateIndexRequestBuilder createIndexRequestBuilder = mock(CreateIndexRequestBuilder.class);
        when(createIndexRequestBuilder.setSettings(any(Settings.Builder.class))).thenReturn(createIndexRequestBuilder);
        when(createIndexRequestBuilder.addMapping(any(String.class), any(XContentBuilder.class))).thenReturn(createIndexRequestBuilder);
        doAnswer(invocation -> {
            throw e;
        }).when(createIndexRequestBuilder).get();
        when(indicesAdminClient.prepareCreate(eq(index))).thenReturn(createIndexRequestBuilder);
        return this;
    }

    public MockClientBuilder prepareSearch(String index, String type, int from, int size, SearchResponse response) {
        SearchRequestBuilder searchRequestBuilder = mock(SearchRequestBuilder.class);
        when(searchRequestBuilder.get()).thenReturn(response);
        when(searchRequestBuilder.setTypes(eq(type))).thenReturn(searchRequestBuilder);
        when(searchRequestBuilder.setFrom(eq(from))).thenReturn(searchRequestBuilder);
        when(searchRequestBuilder.setSize(eq(size))).thenReturn(searchRequestBuilder);
        when(searchRequestBuilder.addSort(any(SortBuilder.class))).thenReturn(searchRequestBuilder);
        when(client.prepareSearch(eq(index))).thenReturn(searchRequestBuilder);
        return this;
    }

    public MockClientBuilder prepareSearch(String index, String type, int from, int size, SearchResponse response,
            ArgumentCaptor<QueryBuilder> filter) {
        SearchRequestBuilder builder = mock(SearchRequestBuilder.class);
        when(builder.setTypes(eq(type))).thenReturn(builder);
        when(builder.addSort(any(SortBuilder.class))).thenReturn(builder);
        when(builder.setQuery(filter.capture())).thenReturn(builder);
        when(builder.setPostFilter(filter.capture())).thenReturn(builder);
        when(builder.setFrom(eq(from))).thenReturn(builder);
        when(builder.setSize(eq(size))).thenReturn(builder);
        when(builder.setFetchSource(eq(true))).thenReturn(builder);
        when(builder.addDocValueField(any(String.class))).thenReturn(builder);
        when(builder.addSort(any(String.class), any(SortOrder.class))).thenReturn(builder);
        when(builder.get()).thenReturn(response);
        when(client.prepareSearch(eq(index))).thenReturn(builder);
        return this;
    }

    public MockClientBuilder prepareSearchAnySize(String index, String type, SearchResponse response, ArgumentCaptor<QueryBuilder> filter) {
        SearchRequestBuilder builder = mock(SearchRequestBuilder.class);
        when(builder.setTypes(eq(type))).thenReturn(builder);
        when(builder.addSort(any(SortBuilder.class))).thenReturn(builder);
        when(builder.setQuery(filter.capture())).thenReturn(builder);
        when(builder.setPostFilter(filter.capture())).thenReturn(builder);
        when(builder.setFrom(any(Integer.class))).thenReturn(builder);
        when(builder.setSize(any(Integer.class))).thenReturn(builder);
        when(builder.setFetchSource(eq(true))).thenReturn(builder);
        when(builder.addDocValueField(any(String.class))).thenReturn(builder);
        when(builder.addSort(any(String.class), any(SortOrder.class))).thenReturn(builder);
        when(builder.get()).thenReturn(response);
        when(client.prepareSearch(eq(index))).thenReturn(builder);
        return this;
    }

    @SuppressWarnings("unchecked")
    public MockClientBuilder prepareIndex(String index, String source) {
        IndexRequestBuilder builder = mock(IndexRequestBuilder.class);
        ListenableActionFuture<IndexResponse> actionFuture = mock(ListenableActionFuture.class);

        when(client.prepareIndex(eq(index), any(), any())).thenReturn(builder);
        when(builder.setSource(eq(source))).thenReturn(builder);
        when(builder.setParent(any(String.class))).thenReturn(builder);
        when(builder.setRefreshPolicy(eq(RefreshPolicy.IMMEDIATE))).thenReturn(builder);
        when(builder.execute()).thenReturn(actionFuture);
        when(actionFuture.actionGet()).thenReturn(mock(IndexResponse.class));
        return this;
    }

    @SuppressWarnings("unchecked")
    public MockClientBuilder prepareIndex(String index, ArgumentCaptor<String> getSource) {
        IndexRequestBuilder builder = mock(IndexRequestBuilder.class);
        ListenableActionFuture<IndexResponse> actionFuture = mock(ListenableActionFuture.class);

        when(client.prepareIndex(eq(index), any(), any())).thenReturn(builder);
        when(builder.setSource(getSource.capture())).thenReturn(builder);
        when(builder.setRefreshPolicy(eq(RefreshPolicy.IMMEDIATE))).thenReturn(builder);
        when(builder.setParent(any(String.class))).thenReturn(builder);
        when(builder.execute()).thenReturn(actionFuture);
        when(actionFuture.actionGet()).thenReturn(mock(IndexResponse.class));
        return this;
    }

    @SuppressWarnings("unchecked")
    public MockClientBuilder prepareIndex(String index, String type, String responseId, ArgumentCaptor<XContentBuilder> getSource) {
        IndexRequestBuilder builder = mock(IndexRequestBuilder.class);
        ListenableActionFuture<IndexResponse> actionFuture = mock(ListenableActionFuture.class);
        IndexResponse response = mock(IndexResponse.class);
        when(response.getId()).thenReturn(responseId);

        when(client.prepareIndex(eq(index), eq(type))).thenReturn(builder);
        when(client.prepareIndex(eq(index), eq(type), any(String.class))).thenReturn(builder);
        when(builder.setSource(getSource.capture())).thenReturn(builder);
        when(builder.setParent(any(String.class))).thenReturn(builder);
        when(builder.setRefreshPolicy(eq(RefreshPolicy.IMMEDIATE))).thenReturn(builder);
        when(builder.execute()).thenReturn(actionFuture);
        when(actionFuture.actionGet()).thenReturn(response);
        return this;
    }

    @SuppressWarnings("unchecked")
    public MockClientBuilder prepareBulk(BulkResponse response) {
        ListenableActionFuture<BulkResponse> actionFuture = mock(ListenableActionFuture.class);
        BulkRequestBuilder builder = mock(BulkRequestBuilder.class);
        when(client.prepareBulk()).thenReturn(builder);
        when(builder.execute()).thenReturn(actionFuture);
        when(actionFuture.actionGet()).thenReturn(response);
        return this;
    }

    public MockClientBuilder prepareUpdate(String index, String type, String id, ArgumentCaptor<Map<String, Object>> getSource) {
        UpdateRequestBuilder builder = mock(UpdateRequestBuilder.class);
        when(client.prepareUpdate(index, type, id)).thenReturn(builder);
        when(builder.setDoc(getSource.capture())).thenReturn(builder);
        when(builder.setRetryOnConflict(any(int.class))).thenReturn(builder);
        when(builder.get()).thenReturn(mock(UpdateResponse.class));
        return this;
    }

    public MockClientBuilder prepareUpdate(String index, String type, String id, ArgumentCaptor<Map<String, Object>> getSource,
            Exception e) {
        UpdateRequestBuilder builder = mock(UpdateRequestBuilder.class);
        when(client.prepareUpdate(index, type, id)).thenReturn(builder);
        when(builder.setDoc(getSource.capture())).thenReturn(builder);
        when(builder.setRetryOnConflict(any(int.class))).thenReturn(builder);
        doAnswer(invocation -> {
            throw e;
        }).when(builder).get();
        return this;
    }

    public MockClientBuilder prepareUpdateScript(String index, String type, String id, ArgumentCaptor<Script> getSource,
            ArgumentCaptor<Map<String, Object>> getParams) {
        UpdateRequestBuilder builder = mock(UpdateRequestBuilder.class);
        when(client.prepareUpdate(index, type, id)).thenReturn(builder);
        when(builder.setScript(getSource.capture())).thenReturn(builder);
        when(builder.setUpsert(getParams.capture())).thenReturn(builder);
        when(builder.setRetryOnConflict(any(int.class))).thenReturn(builder);
        when(builder.get()).thenReturn(mock(UpdateResponse.class));
        return this;
    }

    public MockClientBuilder prepareUpdateScript(String index, String type, String id, ArgumentCaptor<Script> getSource,
            ArgumentCaptor<Map<String, Object>> getParams, Exception e) {
        UpdateRequestBuilder builder = mock(UpdateRequestBuilder.class);
        when(client.prepareUpdate(index, type, id)).thenReturn(builder);
        when(builder.setScript(getSource.capture())).thenReturn(builder);
        when(builder.setUpsert(getParams.capture())).thenReturn(builder);
        when(builder.setRetryOnConflict(any(int.class))).thenReturn(builder);
        doAnswer(invocation -> {
            throw e;
        }).when(builder).get();
        return this;
    }

    public MockClientBuilder throwMissingIndexOnPrepareGet(String index, String type, String id) {
        doThrow(new IndexNotFoundException(index)).when(client).prepareGet(index, type, id);
        return this;
    }

    public Client build() {
        return client;
    }

    public void verifyIndexCreated(String index) {
        verify(indicesAdminClient).prepareCreate(eq(index));
    }

    public void resetIndices() {
        reset(indicesAdminClient);
    }

}
