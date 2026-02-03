/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.search.vectors.RescoreVectorBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class KnnDfsRescoringIT extends ESIntegTestCase {

    private static final String INDEX_NAME = "test_knn_dfs_rescore";
    private static final String VECTOR_FIELD = "vector";
    private static final int VECTOR_DIMS = 64;

    private float[] createVector(int xValue) {
        float[] vector = new float[VECTOR_DIMS];
        vector[0] = xValue;
        return vector;
    }

    private float[] createQueryVector() {
        return new float[VECTOR_DIMS];
    }

    private XContentBuilder createQuantizedMapping() throws Exception {
        return XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(VECTOR_FIELD)
            .field("type", "dense_vector")
            .field("dims", VECTOR_DIMS)
            .field("index", true)
            .field("similarity", "l2_norm")
            .startObject("index_options")
            .field("type", "bbq_hnsw")
            .startObject("rescore_vector")
            .field("oversample", 2.0)
            .endObject()
            .endObject()
            .endObject()
            .startObject("value")
            .field("type", "integer")
            .endObject()
            .endObject()
            .endObject();
    }

    private XContentBuilder createNonQuantizedMapping() throws Exception {
        return XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(VECTOR_FIELD)
            .field("type", "dense_vector")
            .field("dims", VECTOR_DIMS)
            .field("index", true)
            .field("similarity", "l2_norm")
            .startObject("index_options")
            .field("type", "hnsw")
            .endObject()
            .endObject()
            .startObject("value")
            .field("type", "integer")
            .endObject()
            .endObject()
            .endObject();
    }

    public void testKnnSearchWithOversamplingMultipleShards() throws Exception {
        int numShards = randomIntBetween(2, 5);
        Client client = client();
        client.admin()
            .indices()
            .prepareCreate(INDEX_NAME)
            .setSettings(Settings.builder().put("index.number_of_shards", numShards).put("index.number_of_replicas", 0))
            .setMapping(createQuantizedMapping())
            .get();

        int numDocs = 100;
        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            XContentBuilder source = XContentFactory.jsonBuilder()
                .startObject()
                .field(VECTOR_FIELD, createVector(i))
                .field("value", i)
                .endObject();
            indexRequests.add(client.prepareIndex(INDEX_NAME).setId(String.valueOf(i)).setSource(source));
        }
        indexRandom(true, indexRequests);
        refresh(INDEX_NAME);

        int k = 2;
        int numCands = 10;
        float oversample = 2.0f;
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.knnSearch(
            List.of(new KnnSearchBuilder(VECTOR_FIELD, createQueryVector(), k, numCands, null, new RescoreVectorBuilder(oversample), null))
        );

        SearchResponse response = client.search(client.prepareSearch(INDEX_NAME).setSource(sourceBuilder).request()).actionGet();
        try {
            // With delayed rescoring: DFS collects k*oversample, merges globally, rescores, returns top k
            assertThat(response.getHits().getHits().length, lessThanOrEqualTo(k));

            // Verify results are ordered by score (descending)
            float prevScore = Float.MAX_VALUE;
            for (var hit : response.getHits().getHits()) {
                assertThat("Scores should be in descending order", hit.getScore(), lessThanOrEqualTo(prevScore));
                prevScore = hit.getScore();
            }

            // Verify the closest documents to origin are in the results
            Set<String> topDocIds = new HashSet<>();
            for (var hit : response.getHits().getHits()) {
                topDocIds.add(hit.getId());
            }
            // Doc 0 (exact match at origin) should be in results
            assertTrue("Doc 0 (closest to query) should be in results", topDocIds.contains("0"));
        } finally {
            response.decRef();
        }
    }

    public void testKnnSearchWithoutOversampling() throws Exception {
        String indexName = INDEX_NAME + "_no_oversample";
        int numShards = randomIntBetween(2, 4);
        Client client = client();
        client.admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(Settings.builder().put("index.number_of_shards", numShards).put("index.number_of_replicas", 0))
            .setMapping(createNonQuantizedMapping())
            .get();

        int numDocs = 50;
        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            XContentBuilder source = XContentFactory.jsonBuilder()
                .startObject()
                .field(VECTOR_FIELD, createVector(i))
                .field("value", i)
                .endObject();
            indexRequests.add(client.prepareIndex(indexName).setId(String.valueOf(i)).setSource(source));
        }
        indexRandom(true, indexRequests);
        refresh(indexName);

        int k = 5;
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.knnSearch(List.of(new KnnSearchBuilder(VECTOR_FIELD, createQueryVector(), k, 50, null, null, null)));

        SearchResponse response = client.search(client.prepareSearch(indexName).setSource(sourceBuilder).request()).actionGet();
        try {
            assertThat(response.getHits().getHits().length, lessThanOrEqualTo(k));
            float prevScore = Float.MAX_VALUE;
            for (var hit : response.getHits().getHits()) {
                assertThat(hit.getScore(), lessThanOrEqualTo(prevScore));
                prevScore = hit.getScore();
            }
        } finally {
            response.decRef();
        }
    }

    public void testKnnSearchConsistentResultsAcrossRetries() throws Exception {
        String indexName = INDEX_NAME + "_single_segment";
        int numShards = 2;
        Client client = client();
        client.admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(Settings.builder().put("index.number_of_shards", numShards).put("index.number_of_replicas", 0))
            .setMapping(createQuantizedMapping())
            .get();

        int numDocs = 50;
        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            XContentBuilder source = XContentFactory.jsonBuilder()
                .startObject()
                .field(VECTOR_FIELD, createVector(i))
                .field("value", i)
                .endObject();
            indexRequests.add(client.prepareIndex(indexName).setId(String.valueOf(i)).setSource(source));
        }
        indexRandom(true, indexRequests);
        refresh(indexName);
        client.admin().indices().prepareForceMerge(indexName).setMaxNumSegments(1).get();
        int k = 10;
        float oversample = 2.0f;
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.knnSearch(
            List.of(new KnnSearchBuilder(VECTOR_FIELD, createQueryVector(), k, 100, null, new RescoreVectorBuilder(oversample), null))
        );
        SetOnce<List<String>> firstRunIds = new SetOnce<>();
        for (int run = 0; run < 3; run++) {
            SearchResponse response = client.search(client.prepareSearch(indexName).setSource(sourceBuilder).request()).actionGet();
            try {
                List<String> currentRunIds = new ArrayList<>();
                for (var hit : response.getHits().getHits()) {
                    currentRunIds.add(hit.getId());
                }
                if (run == 0) {
                    firstRunIds.set(currentRunIds);
                } else {
                    assertThat("Run " + run + " should have same results as run 0", currentRunIds, equalTo(firstRunIds.get()));
                }
            } finally {
                response.decRef();
            }
        }
    }

    /**
     * Test that default rescoring settings are picked up from index mapping
     * when no explicit rescore_vector is specified in the search request.
     */
    public void testDefaultRescoringFromIndexSettings() throws Exception {
        String indexName = INDEX_NAME + "_default_rescore";
        int numShards = randomIntBetween(2, 4);
        Client client = client();
        // Create index with bbq_hnsw and default rescore_vector settings
        client.admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(Settings.builder().put("index.number_of_shards", numShards).put("index.number_of_replicas", 0))
            .setMapping(createQuantizedMapping())  // Has rescore_vector with oversample=2.0
            .get();

        int numDocs = 50;
        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            XContentBuilder source = XContentFactory.jsonBuilder()
                .startObject()
                .field(VECTOR_FIELD, createVector(i))
                .field("value", i)
                .endObject();
            indexRequests.add(client.prepareIndex(indexName).setId(String.valueOf(i)).setSource(source));
        }
        indexRandom(true, indexRequests);
        refresh(indexName);

        int k = 5;
        int numCands = 50;
        // Search WITHOUT explicit rescore_vector - should use index default
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.knnSearch(List.of(new KnnSearchBuilder(VECTOR_FIELD, createQueryVector(), k, numCands, null, null, null)));

        SearchResponse response = client.search(client.prepareSearch(indexName).setSource(sourceBuilder).request()).actionGet();
        try {
            assertThat(response.getHits().getHits().length, lessThanOrEqualTo(k));
            // Verify results are ordered correctly - closest docs to origin should be first
            Set<String> topDocIds = new HashSet<>();
            for (var hit : response.getHits().getHits()) {
                topDocIds.add(hit.getId());
            }
            // The closest k docs to [0,0,0] should be docs 0,1,2,3,4
            for (int i = 0; i < Math.min(k, response.getHits().getHits().length); i++) {
                assertTrue(
                    "Expected doc " + i + " to be in top results when using index default rescoring",
                    topDocIds.contains(String.valueOf(i)) || topDocIds.size() < k
                );
            }
        } finally {
            response.decRef();
        }
    }

    /**
     * Test that rescoring properly reorders results based on true vector similarity.
     * This test creates vectors where quantized approximate scores may differ from
     * true scores, and verifies that rescoring correctly identifies the best matches.
     */
    public void testRescoringReordersResults() throws Exception {
        String indexName = INDEX_NAME + "_rescore_reorder";
        int numShards = randomIntBetween(2, 4);
        Client client = client();
        client.admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(Settings.builder().put("index.number_of_shards", numShards).put("index.number_of_replicas", 0))
            .setMapping(createQuantizedMapping())
            .get();

        // Index documents with vectors along x-axis
        // Doc 0: [0, 0, ..., 0] - exact match to query
        // Doc 1: [1, 0, ..., 0] - very close
        // Doc 2: [2, 0, ..., 0] - close
        // etc.
        int numDocs = 100;
        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            XContentBuilder source = XContentFactory.jsonBuilder()
                .startObject()
                .field(VECTOR_FIELD, createVector(i))
                .field("value", i)
                .endObject();
            indexRequests.add(client.prepareIndex(indexName).setId(String.valueOf(i)).setSource(source));
        }
        indexRandom(true, indexRequests);
        refresh(indexName);

        // Force merge to single segment for deterministic behavior
        client.admin().indices().prepareForceMerge(indexName).setMaxNumSegments(1).get();

        int k = 5;
        float oversample = 3.0f;  // Collect k*3 = 15 candidates, then rescore to top 5
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.knnSearch(
            List.of(new KnnSearchBuilder(VECTOR_FIELD, createQueryVector(), k, 100, null, new RescoreVectorBuilder(oversample), null))
        );

        SearchResponse response = client.search(client.prepareSearch(indexName).setSource(sourceBuilder).request()).actionGet();
        try {
            assertThat(response.getHits().getHits().length, equalTo(k));

            // After rescoring, the true closest documents should be at the top
            // For query [0,0,0] and vectors [i,0,0], the order should be 0,1,2,3,4
            for (int i = 0; i < k; i++) {
                var hit = response.getHits().getHits()[i];
                assertThat("After rescoring, doc " + i + " should be at position " + i, hit.getId(), equalTo(String.valueOf(i)));
            }

            // Verify scores are in descending order (l2_norm similarity: higher is better/closer)
            float prevScore = Float.MAX_VALUE;
            for (var hit : response.getHits().getHits()) {
                assertThat("Scores should be in descending order", hit.getScore(), lessThanOrEqualTo(prevScore));
                prevScore = hit.getScore();
            }
        } finally {
            response.decRef();
        }
    }
}
