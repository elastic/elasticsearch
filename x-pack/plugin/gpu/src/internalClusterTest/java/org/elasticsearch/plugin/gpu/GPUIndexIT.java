/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.plugin.gpu;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.gpu.GPUPlugin;
import org.elasticsearch.xpack.gpu.GPUSupport;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.util.Collection;
import java.util.List;
import java.util.Locale;

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
        assumeTrue("cuvs not supported", GPUSupport.isSupported(false));
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

    @AwaitsFix(bugUrl = "Fix sorted index")
    public void testSortedIndexReturnsSameResultsAsUnsorted() {
        String indexName1 = "index_unsorted";
        String indexName2 = "index_sorted";
        final int dims = randomIntBetween(4, 128);
        createIndex(indexName1, dims, false);
        createIndex(indexName2, dims, true);

        final int[] numDocs = new int[] { randomIntBetween(50, 100), randomIntBetween(50, 100) };
        for (int i = 0; i < numDocs.length; i++) {
            BulkRequestBuilder bulkRequest1 = client().prepareBulk();
            BulkRequestBuilder bulkRequest2 = client().prepareBulk();
            for (int j = 0; j < numDocs[i]; j++) {
                String id = String.valueOf(i * 100 + j);
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
        int numCandidates = k * 10;

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
            Assert.assertEquals(hits1.length, hits2.length);
            for (int i = 0; i < hits1.length; i++) {
                Assert.assertEquals(hits1[i].getId(), hits2[i].getId());
                Assert.assertEquals(hits1[i].field("my_keyword").getValue(), (String) hits2[i].field("my_keyword").getValue());
                Assert.assertEquals(hits1[i].getScore(), hits2[i].getScore(), 0.001f);
            }
        } finally {
            searchResponse1.decRef();
            searchResponse2.decRef();
        }

        // Force merge and search again
        assertNoFailures(indicesAdmin().prepareForceMerge(indexName1).get());
        assertNoFailures(indicesAdmin().prepareForceMerge(indexName2).get());
        ensureGreen();

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
            Assert.assertEquals(hits3.length, hits4.length);
            for (int i = 0; i < hits3.length; i++) {
                Assert.assertEquals(hits3[i].getId(), hits4[i].getId());
                Assert.assertEquals(hits3[i].field("my_keyword").getValue(), (String) hits4[i].field("my_keyword").getValue());
                Assert.assertEquals(hits3[i].getScore(), hits4[i].getScore(), 0.01f);
            }
        } finally {
            searchResponse3.decRef();
            searchResponse4.decRef();
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
}
