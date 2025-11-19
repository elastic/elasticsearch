/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gpu.GPUSupport;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.vectors.ExactKnnQueryBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.hamcrest.Matchers.containsString;

@LuceneTestCase.SuppressCodecs("*") // use our custom codec
public class GPUIndexIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(GPUPlugin.class);
    }

    @BeforeClass
    public static void checkGPUSupport() {
        assumeTrue("cuvs not supported", GPUSupport.isSupported());
    }

    public void testBasic() {
        String indexName = "index1";
        final int dims = randomIntBetween(4, 128);
        final int[] numDocs = new int[] { randomIntBetween(1, 100), 1, 2, randomIntBetween(1, 100) };
        createIndex(indexName, dims, false);
        int totalDocs = 0;
        for (int i = 0; i < numDocs.length; i++) {
            indexDocs(indexName, numDocs[i], dims, i * 100);
            totalDocs += numDocs[i];
        }
        refresh();
        assertSearch(indexName, randomFloatVector(dims), totalDocs);
    }

    public void testSortedIndexReturnsSameResultsAsUnsorted() {
        String indexName1 = "index_unsorted";
        String indexName2 = "index_sorted";
        final int dims = randomIntBetween(4, 128);
        createIndex(indexName1, dims, false);
        createIndex(indexName2, dims, true);

        final int[] numDocs = new int[] { randomIntBetween(300, 999), randomIntBetween(300, 999) };
        for (int i = 0; i < numDocs.length; i++) {
            BulkRequestBuilder bulkRequest1 = client().prepareBulk();
            BulkRequestBuilder bulkRequest2 = client().prepareBulk();
            for (int j = 0; j < numDocs[i]; j++) {
                String id = String.valueOf(i * 1000 + j);
                String keywordValue = String.valueOf(numDocs[i] - j);
                float[] vector = randomFloatVector(dims);
                bulkRequest1.add(prepareIndex(indexName1).setId(id).setSource("my_vector", vector, "my_keyword", keywordValue));
                bulkRequest2.add(prepareIndex(indexName2).setId(id).setSource("my_vector", vector, "my_keyword", keywordValue));
            }
            BulkResponse bulkResponse1 = bulkRequest1.get();
            assertFalse("Bulk request failed: " + bulkResponse1.buildFailureMessage(), bulkResponse1.hasFailures());
            BulkResponse bulkResponse2 = bulkRequest2.get();
            assertFalse("Bulk request failed: " + bulkResponse2.buildFailureMessage(), bulkResponse2.hasFailures());
        }
        refresh();

        float[] queryVector = randomFloatVector(dims);
        int k = 10;
        int numCandidates = k * 5;

        // Test 1: Approximate KNN search - expect at least k-3 out of k matches
        var searchResponse1 = prepareSearch(indexName1).setSize(k)
            .setFetchSource(false)
            .addFetchField("my_keyword")
            .setKnnSearch(List.of(new KnnSearchBuilder("my_vector", queryVector, k, numCandidates, null, null, null)))
            .get();

        var searchResponse2 = prepareSearch(indexName2).setSize(k)
            .setFetchSource(false)
            .addFetchField("my_keyword")
            .setKnnSearch(List.of(new KnnSearchBuilder("my_vector", queryVector, k, numCandidates, null, null, null)))
            .get();

        try {
            SearchHit[] hits1 = searchResponse1.getHits().getHits();
            SearchHit[] hits2 = searchResponse2.getHits().getHits();
            assertAtLeastNOutOfKMatches(hits1, hits2, k - 3, k);
        } finally {
            searchResponse1.decRef();
            searchResponse2.decRef();
        }

        // Test 2: Exact KNN search (brute-force) - expect perfect k out of k matches
        var exactSearchResponse1 = prepareSearch(indexName1).setSize(k)
            .setFetchSource(false)
            .addFetchField("my_keyword")
            .setQuery(new ExactKnnQueryBuilder(VectorData.fromFloats(queryVector), "my_vector", null))
            .get();

        var exactSearchResponse2 = prepareSearch(indexName2).setSize(k)
            .setFetchSource(false)
            .addFetchField("my_keyword")
            .setQuery(new ExactKnnQueryBuilder(VectorData.fromFloats(queryVector), "my_vector", null))
            .get();

        try {
            SearchHit[] exactHits1 = exactSearchResponse1.getHits().getHits();
            SearchHit[] exactHits2 = exactSearchResponse2.getHits().getHits();
            assertExactMatches(exactHits1, exactHits2, k);
        } finally {
            exactSearchResponse1.decRef();
            exactSearchResponse2.decRef();
        }

        // Force merge and search again
        assertNoFailures(indicesAdmin().prepareForceMerge(indexName1).get());
        assertNoFailures(indicesAdmin().prepareForceMerge(indexName2).get());
        ensureGreen();

        // Test 3: Approximate KNN search - expect at least k-3 out of k matches
        var searchResponse3 = prepareSearch(indexName1).setSize(k)
            .setFetchSource(false)
            .addFetchField("my_keyword")
            .setKnnSearch(List.of(new KnnSearchBuilder("my_vector", queryVector, k, numCandidates, null, null, null)))
            .get();

        var searchResponse4 = prepareSearch(indexName2).setSize(k)
            .setFetchSource(false)
            .addFetchField("my_keyword")
            .setKnnSearch(List.of(new KnnSearchBuilder("my_vector", queryVector, k, numCandidates, null, null, null)))
            .get();

        try {
            SearchHit[] hits3 = searchResponse3.getHits().getHits();
            SearchHit[] hits4 = searchResponse4.getHits().getHits();
            assertAtLeastNOutOfKMatches(hits3, hits4, k - 3, k);
        } finally {
            searchResponse3.decRef();
            searchResponse4.decRef();
        }

        // Test 4: Exact KNN search after merge - expect perfect k out of k matches
        var exactSearchResponse3 = prepareSearch(indexName1).setSize(k)
            .setFetchSource(false)
            .addFetchField("my_keyword")
            .setQuery(new ExactKnnQueryBuilder(VectorData.fromFloats(queryVector), "my_vector", null))
            .get();

        var exactSearchResponse4 = prepareSearch(indexName2).setSize(k)
            .setFetchSource(false)
            .addFetchField("my_keyword")
            .setQuery(new ExactKnnQueryBuilder(VectorData.fromFloats(queryVector), "my_vector", null))
            .get();

        try {
            SearchHit[] exactHits3 = exactSearchResponse3.getHits().getHits();
            SearchHit[] exactHits4 = exactSearchResponse4.getHits().getHits();
            assertExactMatches(exactHits3, exactHits4, k);
        } finally {
            exactSearchResponse3.decRef();
            exactSearchResponse4.decRef();
        }
    }

    public void testSearchWithoutGPU() {
        String indexName = "index1";
        final int dims = randomIntBetween(4, 128);
        final int numDocs = randomIntBetween(1, 500);
        createIndex(indexName, dims, false);
        ensureGreen();

        indexDocs(indexName, numDocs, dims, 0);
        refresh();

        // update settings to disable GPU usage
        Settings.Builder settingsBuilder = Settings.builder().put("index.vectors.indexing.use_gpu", false);
        assertAcked(client().admin().indices().prepareUpdateSettings(indexName).setSettings(settingsBuilder.build()));
        ensureGreen();
        assertSearch(indexName, randomFloatVector(dims), numDocs);
    }

    public void testInt8HnswMaxInnerProductProductFails() {
        String indexName = "index_int8_max_inner_product_fails";
        final int dims = randomIntBetween(4, 128);

        Settings.Builder settingsBuilder = Settings.builder().put(indexSettings());
        settingsBuilder.put("index.number_of_shards", 1);
        settingsBuilder.put("index.vectors.indexing.use_gpu", true);

        String mapping = String.format(Locale.ROOT, """
            {
              "properties": {
                "my_vector": {
                  "type": "dense_vector",
                  "dims": %d,
                  "similarity": "max_inner_product",
                  "index_options": {
                    "type": "int8_hnsw"
                  }
                }
              }
            }
            """, dims);

        // Index creation should succeed
        assertAcked(prepareCreate(indexName).setSettings(settingsBuilder.build()).setMapping(mapping));
        ensureGreen();

        // Attempt to index a document and expect it to fail
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> client().prepareIndex(indexName).setId("1").setSource("my_vector", randomFloatVector(dims)).get()
        );
        assertThat(
            ex.getMessage(),
            containsString("GPU vector indexing does not support [max_inner_product] similarity for [int8_hnsw] index type.")
        );
    }

    private void createIndex(String indexName, int dims, boolean sorted) {
        var settings = Settings.builder().put(indexSettings());
        settings.put("index.number_of_shards", 1);
        settings.put("index.vectors.indexing.use_gpu", true);
        if (sorted) {
            settings.put("index.sort.field", "my_keyword");
        }

        String type = randomFrom("hnsw", "int8_hnsw");
        String mapping = String.format(Locale.ROOT, """
            {
              "properties": {
                "my_vector": {
                  "type": "dense_vector",
                  "dims": %d,
                  "similarity": "l2_norm",
                  "index_options": {
                    "type": "%s"
                  }
                },
                "my_keyword": {
                  "type": "keyword"
                }
              }
            }
            """, dims, type);
        assertAcked(prepareCreate(indexName).setSettings(settings.build()).setMapping(mapping));
        ensureGreen();
    }

    private void indexDocs(String indexName, int numDocs, int dims, int startDoc) {
        BulkRequestBuilder bulkRequest = client().prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            String id = String.valueOf(startDoc + i);
            String keywordValue = String.valueOf(numDocs - i);
            var indexRequest = prepareIndex(indexName).setId(id)
                .setSource("my_vector", randomFloatVector(dims), "my_keyword", keywordValue);
            bulkRequest.add(indexRequest);
        }
        BulkResponse bulkResponse = bulkRequest.get();
        assertFalse("Bulk request failed: " + bulkResponse.buildFailureMessage(), bulkResponse.hasFailures());
    }

    private void assertSearch(String indexName, float[] queryVector, int totalDocs) {
        int k = Math.min(randomIntBetween(1, 20), totalDocs);
        int numCandidates = k * 10;
        assertNoFailuresAndResponse(
            prepareSearch(indexName).setSize(k)
                .setFetchSource(false)
                .addFetchField("my_keyword")
                .setKnnSearch(List.of(new KnnSearchBuilder("my_vector", queryVector, k, numCandidates, null, null, null))),
            response -> assertEquals("Expected k hits to be returned", k, response.getHits().getHits().length)
        );
    }

    private static float[] randomFloatVector(int dims) {
        float[] vector = new float[dims];
        for (int i = 0; i < dims; i++) {
            vector[i] = randomFloat();
        }
        return vector;
    }

    /**
     * Asserts that at least N out of K hits have matching IDs between two result sets.
     */
    private static void assertAtLeastNOutOfKMatches(SearchHit[] hits1, SearchHit[] hits2, int minMatches, int k) {
        Assert.assertEquals("Both result sets should have k hits", k, hits1.length);
        Assert.assertEquals("Both result sets should have k hits", k, hits2.length);
        Set<String> ids1 = new HashSet<>();
        Set<String> ids2 = new HashSet<>();

        for (SearchHit hit : hits1) {
            ids1.add(hit.getId());
        }
        for (SearchHit hit : hits2) {
            ids2.add(hit.getId());
        }

        Set<String> intersection = new HashSet<>(ids1);
        intersection.retainAll(ids2);
        Assert.assertTrue(
            String.format(
                Locale.ROOT,
                "Expected at least %d matching IDs out of %d, but found %d. IDs1: %s, IDs2: %s",
                minMatches,
                k,
                intersection.size(),
                ids1,
                ids2
            ),
            intersection.size() >= minMatches
        );
    }

    /**
     * Asserts that two result sets have exactly the same document IDs in the same order with the same scores.
     * Used for exact (brute-force) KNN search which should be deterministic.
     * Expects k out of k matches.
     */
    private static void assertExactMatches(SearchHit[] hits1, SearchHit[] hits2, int k) {
        Assert.assertEquals("Both result sets should have k hits", k, hits1.length);
        Assert.assertEquals("Both result sets should have k hits", k, hits2.length);

        for (int i = 0; i < k; i++) {
            Assert.assertEquals(String.format(Locale.ROOT, "Document ID mismatch at position %d", i), hits1[i].getId(), hits2[i].getId());
            Assert.assertEquals(
                String.format(Locale.ROOT, "Score mismatch for document ID %s at position %d", hits1[i].getId(), i),
                hits1[i].getScore(),
                hits2[i].getScore(),
                0.0001f
            );
        }
    }
}
