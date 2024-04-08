/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.AdminClient;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ClusterAdminClient;
import org.elasticsearch.client.internal.IndicesAdminClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockClientBuilder {
    private Client client;

    private AdminClient adminClient;
    private ClusterAdminClient clusterAdminClient;
    private IndicesAdminClient indicesAdminClient;

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
        ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
    }

    @SuppressWarnings({ "unchecked" })
    public MockClientBuilder addClusterStatusYellowResponse() {
        PlainActionFuture<ClusterHealthResponse> actionFuture = mock(PlainActionFuture.class);
        ClusterHealthRequestBuilder clusterHealthRequestBuilder = mock(ClusterHealthRequestBuilder.class);

        when(clusterAdminClient.prepareHealth()).thenReturn(clusterHealthRequestBuilder);
        when(clusterHealthRequestBuilder.setWaitForYellowStatus()).thenReturn(clusterHealthRequestBuilder);
        when(clusterHealthRequestBuilder.execute()).thenReturn(actionFuture);
        when(actionFuture.actionGet()).thenReturn(mock(ClusterHealthResponse.class));
        return this;
    }

    @SuppressWarnings("unchecked")
    public MockClientBuilder get(GetResponse response) {
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocationOnMock) {
                ActionListener<GetResponse> listener = (ActionListener<GetResponse>) invocationOnMock.getArguments()[1];
                listener.onResponse(response);
                return null;
            }
        }).when(client).get(any(), any());

        return this;
    }

    public MockClientBuilder prepareCreate(String index) {
        CreateIndexRequestBuilder createIndexRequestBuilder = mock(CreateIndexRequestBuilder.class);
        CreateIndexResponse response = mock(CreateIndexResponse.class);
        when(createIndexRequestBuilder.setSettings(any(Settings.Builder.class))).thenReturn(createIndexRequestBuilder);
        when(createIndexRequestBuilder.setMapping(any(XContentBuilder.class))).thenReturn(createIndexRequestBuilder);
        when(createIndexRequestBuilder.get()).thenReturn(response);
        when(indicesAdminClient.prepareCreate(eq(index))).thenReturn(createIndexRequestBuilder);
        return this;
    }

    public MockClientBuilder prepareSearches(String index, SearchRequestBuilder first, SearchRequestBuilder... searches) {
        when(client.prepareSearch(eq(index))).thenReturn(first, searches);
        return this;
    }

    /**
     * Creates a {@link SearchResponse} with a {@link SearchHit} for each element of {@code docs}
     * @param indexName Index being searched
     * @param docs Returned in the SearchResponse
     * @return this
     */
    @SuppressWarnings("unchecked")
    public MockClientBuilder prepareSearch(String indexName, List<BytesReference> docs) {
        SearchRequestBuilder builder = mock(SearchRequestBuilder.class);
        when(builder.setIndicesOptions(any())).thenReturn(builder);
        when(builder.setQuery(any())).thenReturn(builder);
        when(builder.setSource(any())).thenReturn(builder);
        when(builder.setSize(anyInt())).thenReturn(builder);
        SearchRequest request = new SearchRequest(indexName);
        when(builder.request()).thenReturn(request);

        when(client.prepareSearch(eq(indexName))).thenReturn(builder);

        SearchHit hits[] = new SearchHit[docs.size()];
        for (int i = 0; i < docs.size(); i++) {
            SearchHit hit = new SearchHit(10);
            hit.sourceRef(docs.get(i));
            hits[i] = hit;
        }

        SearchResponse response = mock(SearchResponse.class);
        SearchHits searchHits = new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 0.0f);
        when(response.getHits()).thenReturn(searchHits.asUnpooled());
        searchHits.decRef();

        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocationOnMock) {
                ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocationOnMock.getArguments()[1];
                listener.onResponse(response);
                return null;
            }
        }).when(client).search(eq(request), any());

        return this;
    }

    /*
     * Mock a search that returns search hits with fields.
     * The number of hits is the size of fields
     */
    @SuppressWarnings("unchecked")
    public MockClientBuilder prepareSearchFields(String indexName, List<Map<String, DocumentField>> fields) {
        SearchRequestBuilder builder = mock(SearchRequestBuilder.class);
        when(builder.setIndicesOptions(any())).thenReturn(builder);
        when(builder.setQuery(any())).thenReturn(builder);
        when(builder.setSource(any())).thenReturn(builder);
        when(builder.setSize(anyInt())).thenReturn(builder);
        SearchRequest request = new SearchRequest(indexName);
        when(builder.request()).thenReturn(request);

        when(client.prepareSearch(eq(indexName))).thenReturn(builder);

        SearchHit hits[] = new SearchHit[fields.size()];
        for (int i = 0; i < hits.length; i++) {
            SearchHit hit = new SearchHit(10, null);
            hit.addDocumentFields(Map.of(), fields.get(i));
            hits[i] = hit;
        }

        SearchResponse response = mock(SearchResponse.class);
        SearchHits searchHits = new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 0.0f);
        when(response.getHits()).thenReturn(searchHits.asUnpooled());
        searchHits.decRef();

        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocationOnMock) {
                ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocationOnMock.getArguments()[1];
                listener.onResponse(response);
                return null;
            }
        }).when(client).search(eq(request), any());

        return this;
    }

    @SuppressWarnings("unchecked")
    public MockClientBuilder prepareBulk(BulkResponse response) {
        PlainActionFuture<BulkResponse> actionFuture = mock(PlainActionFuture.class);
        BulkRequestBuilder builder = mock(BulkRequestBuilder.class);
        when(client.prepareBulk()).thenReturn(builder);
        when(builder.execute()).thenReturn(actionFuture);
        when(actionFuture.actionGet()).thenReturn(response);
        return this;
    }

    @SuppressWarnings("unchecked")
    public MockClientBuilder bulk(BulkResponse response) {
        ActionFuture<BulkResponse> actionFuture = mock(ActionFuture.class);
        when(client.bulk(any(BulkRequest.class))).thenReturn(actionFuture);
        when(actionFuture.actionGet()).thenReturn(response);
        return this;
    }

    public Client build() {
        return client;
    }
}
