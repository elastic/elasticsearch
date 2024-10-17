/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.retriever;

import org.elasticsearch.action.admin.cluster.node.capabilities.NodesCapabilitiesRequest;
import org.elasticsearch.action.admin.cluster.node.capabilities.NodesCapabilitiesResponse;
import org.elasticsearch.action.admin.cluster.stats.SearchUsageStats;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RetrieverTelemetryIT extends ESIntegTestCase {

    private static final String INDEX_NAME = "test_index";

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Before
    public void setup() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("vector")
            .field("type", "dense_vector")
            .field("dims", 1)
            .field("index", true)
            .field("similarity", "l2_norm")
            .startObject("index_options")
            .field("type", "hnsw")
            .endObject()
            .endObject()
            .startObject("text")
            .field("type", "text")
            .endObject()
            .startObject("integer")
            .field("type", "integer")
            .endObject()
            .startObject("topic")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject();

        assertAcked(prepareCreate(INDEX_NAME).setMapping(builder));
        ensureGreen(INDEX_NAME);
    }

    private void performSearch(SearchSourceBuilder source) throws IOException {
        Request request = new Request("GET", INDEX_NAME + "/_search");
        request.setJsonEntity(Strings.toString(source));
        getRestClient().performRequest(request);
    }

    public void testTelemetryForRetrievers() throws IOException {

        if (false == isRetrieverTelemetryEnabled()) {
            return;
        }

        // search#1 - this will record 1 entry for "retriever" in `sections`, and 1 for "knn" under `retrievers`
        {
            performSearch(new SearchSourceBuilder().retriever(new KnnRetrieverBuilder("vector", new float[] { 1.0f }, null, 10, 15, null)));
        }

        // search#2 - this will record 1 entry for "retriever" in `sections`, 1 for "standard" under `retrievers`, and 1 for "range" under
        // `queries`
        {
            performSearch(new SearchSourceBuilder().retriever(new StandardRetrieverBuilder(QueryBuilders.rangeQuery("integer").gte(2))));
        }

        // search#3 - this will record 1 entry for "retriever" in `sections`, and 1 for "standard" under `retrievers`, and 1 for "knn" under
        // `queries`
        {
            performSearch(
                new SearchSourceBuilder().retriever(
                    new StandardRetrieverBuilder(new KnnVectorQueryBuilder("vector", new float[] { 1.0f }, 10, 15, null))
                )
            );
        }

        // search#4 - this will record 1 entry for "retriever" in `sections`, and 1 for "standard" under `retrievers`, and 1 for "term"
        // under `queries`
        {
            performSearch(new SearchSourceBuilder().retriever(new StandardRetrieverBuilder(QueryBuilders.termQuery("topic", "foo"))));
        }

        // search#5 - t
        // his will record 1 entry for "knn" in `sections`
        {
            performSearch(new SearchSourceBuilder().knnSearch(List.of(new KnnSearchBuilder("vector", new float[] { 1.0f }, 10, 15, null))));
        }

        // search#6 - this will record 1 entry for "query" in `sections`, and 1 for "match_all" under `queries`
        {
            performSearch(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()));
        }

        // cluster stats
        {
            SearchUsageStats stats = clusterAdmin().prepareClusterStats().get().getIndicesStats().getSearchUsageStats();
            assertEquals(6, stats.getTotalSearchCount());

            assertThat(stats.getSectionsUsage().size(), equalTo(3));
            assertThat(stats.getSectionsUsage().get("retriever"), equalTo(4L));
            assertThat(stats.getSectionsUsage().get("query"), equalTo(1L));
            assertThat(stats.getSectionsUsage().get("knn"), equalTo(1L));

            assertThat(stats.getRetrieversUsage().size(), equalTo(2));
            assertThat(stats.getRetrieversUsage().get("standard"), equalTo(3L));
            assertThat(stats.getRetrieversUsage().get("knn"), equalTo(1L));

            assertThat(stats.getQueryUsage().size(), equalTo(4));
            assertThat(stats.getQueryUsage().get("range"), equalTo(1L));
            assertThat(stats.getQueryUsage().get("term"), equalTo(1L));
            assertThat(stats.getQueryUsage().get("match_all"), equalTo(1L));
            assertThat(stats.getQueryUsage().get("knn"), equalTo(1L));
        }
    }

    private boolean isRetrieverTelemetryEnabled() throws IOException {
        NodesCapabilitiesResponse res = clusterAdmin().nodesCapabilities(
            new NodesCapabilitiesRequest().method(RestRequest.Method.GET).path("_cluster/stats").capabilities("retrievers-usage-stats")
        ).actionGet();
        return res != null && res.isSupported().orElse(false);
    }
}
