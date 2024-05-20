/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class TransportSimulateShardBulkActionIT extends ESIntegTestCase {
    @SuppressWarnings("unchecked")
    public void testNoPersistentChanges() {
        /*
         * This test indexes a document into an index. Then it simulates a BulkShardRequest of another two documents. Then we make sure
         * that the index only contains the one document, and that the index's mapping in the cluster state has not been updated with the
         *  two new fields.
         */
        String indexName = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        IndexRequest indexRequest = new IndexRequest(indexName);
        indexRequest.source("""
            {
                "foo1": "bar"
            }
            """, XContentType.JSON);
        ShardId shardId = client().index(indexRequest).actionGet().getShardId();
        BulkItemRequest[] items = new BulkItemRequest[2];
        items[0] = new BulkItemRequest(0, new IndexRequest(indexName).source("""
            {
                "foo2": "baz"
            }
            """, XContentType.JSON).id(randomUUID()));
        items[1] = new BulkItemRequest(0, new IndexRequest(indexName).source("""
            {
                "foo3": "baz"
            }
            """, XContentType.JSON).id(randomUUID()));
        BulkShardRequest bulkShardRequest = new BulkShardRequest(shardId, WriteRequest.RefreshPolicy.NONE, items, true);
        BulkShardResponse response = client().execute(TransportSimulateShardBulkAction.TYPE, bulkShardRequest).actionGet();
        assertThat(response.getResponses().length, equalTo(2));
        assertThat(response.getResponses()[0].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        assertThat(response.getResponses()[1].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        indicesAdmin().refresh(new RefreshRequest(indexName)).actionGet();
        SearchResponse searchResponse = client().search(new SearchRequest(indexName)).actionGet();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        searchResponse.decRef();
        ClusterStateResponse clusterStateResponse = admin().cluster().state(new ClusterStateRequest()).actionGet();
        Map<String, Object> indexMapping = clusterStateResponse.getState().metadata().index(indexName).mapping().sourceAsMap();
        Map<String, Object> fields = (Map<String, Object>) indexMapping.get("properties");
        assertThat(fields.size(), equalTo(1));
    }

    @SuppressWarnings("unchecked")
    public void testMappingValidation() {
        /*
         * This test indexes a document into an index. Then it simulates a BulkShardRequest of another two documents. Then we make sure
         * that the index only contains the one document, and that the index's mapping in the cluster state has not been updated with the
         *  two new fields.
         */
        String indexName = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        String mapping = """
            {
                "_doc":{
                    "dynamic":"strict",
                    "properties":{
                        "foo1":{
                            "type":"text"
                        }
                    }
                }
            }
            """;
        indicesAdmin().create(new CreateIndexRequest(indexName).mapping(mapping)).actionGet();
        IndexRequest indexRequest = new IndexRequest(indexName);
        indexRequest.source("""
            {
                "foo1": "bar"
            }
            """, XContentType.JSON);
        ShardId shardId = client().index(indexRequest).actionGet().getShardId();
        BulkItemRequest[] items = new BulkItemRequest[2];
        items[0] = new BulkItemRequest(0, new IndexRequest(indexName).source("""
            {
                "foo1": "baz"
            }
            """, XContentType.JSON).id(randomUUID()));
        items[1] = new BulkItemRequest(0, new IndexRequest(indexName).source("""
            {
                "foo3": "baz"
            }
            """, XContentType.JSON).id(randomUUID()));
        BulkShardRequest bulkShardRequest = new BulkShardRequest(shardId, WriteRequest.RefreshPolicy.NONE, items, true);
        BulkShardResponse response = client().execute(TransportSimulateShardBulkAction.TYPE, bulkShardRequest).actionGet();
        assertThat(response.getResponses().length, equalTo(2));
        assertThat(response.getResponses()[0].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        assertThat(
            response.getResponses()[1].getFailure().getCause().getMessage(),
            containsString("mapping set to strict, dynamic introduction of")
        );
        indicesAdmin().refresh(new RefreshRequest(indexName)).actionGet();
        SearchResponse searchResponse = client().search(new SearchRequest(indexName)).actionGet();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        assertThat(searchResponse.getHits().getHits()[0].getSourceAsMap().get("foo1"), equalTo("bar"));
        searchResponse.decRef();
        ClusterStateResponse clusterStateResponse = admin().cluster().state(new ClusterStateRequest()).actionGet();
        Map<String, Object> indexMapping = clusterStateResponse.getState().metadata().index(indexName).mapping().sourceAsMap();
        Map<String, Object> fields = (Map<String, Object>) indexMapping.get("properties");
        assertThat(fields.size(), equalTo(1));
    }
}
