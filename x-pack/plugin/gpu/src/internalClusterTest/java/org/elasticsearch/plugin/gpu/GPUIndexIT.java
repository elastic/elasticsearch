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
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.gpu.GPUPlugin;
import org.elasticsearch.xpack.gpu.GPUSupport;

import java.util.Collection;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;

@LuceneTestCase.SuppressCodecs("*") // use our custom codec
public class GPUIndexIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(GPUPlugin.class);
    }

    public void testBasic() {
        assumeTrue("cuvs not supported", GPUSupport.isSupported(false));
        final int dims = randomIntBetween(4, 128);
        final int[] numDocs = new int[] { randomIntBetween(1, 100), 1, 2, randomIntBetween(1, 100) };
        createIndex(dims);
        int totalDocs = 0;
        for (int i = 0; i < numDocs.length; i++) {
            indexDocs(numDocs[i], dims, i * 100);
            totalDocs += numDocs[i];
        }
        refresh();
        assertSearch(randomFloatVector(dims), totalDocs);
    }

    public void testSearchWithoutGPU() {
        assumeTrue("cuvs not supported", GPUSupport.isSupported(false));
        final int dims = randomIntBetween(4, 128);
        final int numDocs = randomIntBetween(1, 500);
        createIndex(dims);
        ensureGreen();

        indexDocs(numDocs, dims, 0);
        refresh();

        // update settings to disable GPU usage
        Settings.Builder settingsBuilder = Settings.builder().put("index.vectors.indexing.use_gpu", false);
        assertAcked(client().admin().indices().prepareUpdateSettings("foo-index").setSettings(settingsBuilder.build()));
        ensureGreen();
        assertSearch(randomFloatVector(dims), numDocs);
    }

    private void createIndex(int dims) {
        var settings = Settings.builder().put(indexSettings());
        settings.put("index.number_of_shards", 1);
        settings.put("index.vectors.indexing.use_gpu", true);
        assertAcked(prepareCreate("foo-index").setSettings(settings.build()).setMapping(String.format(Locale.ROOT, """
                {
                  "properties": {
                    "my_vector": {
                      "type": "dense_vector",
                      "dims": %d,
                      "similarity": "l2_norm",
                      "index_options": {
                        "type": "hnsw"
                      }
                    }
                  }
                }
            """, dims)));
        ensureGreen();
    }

    private void indexDocs(int numDocs, int dims, int startDoc) {
        BulkRequestBuilder bulkRequest = client().prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            String id = String.valueOf(startDoc + i);
            bulkRequest.add(prepareIndex("foo-index").setId(id).setSource("my_vector", randomFloatVector(dims)));
        }
        BulkResponse bulkResponse = bulkRequest.get();
        assertFalse("Bulk request failed: " + bulkResponse.buildFailureMessage(), bulkResponse.hasFailures());
    }

    private void assertSearch(float[] queryVector, int totalDocs) {
        int k = Math.min(randomIntBetween(1, 20), totalDocs);
        int numCandidates = k * 10;
        assertNoFailuresAndResponse(
            prepareSearch("foo-index").setSize(k)
                .setKnnSearch(List.of(new KnnSearchBuilder("my_vector", queryVector, k, numCandidates, null, null))),
            response -> {
                assertEquals("Expected k hits to be returned", k, response.getHits().getHits().length);
            }
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
