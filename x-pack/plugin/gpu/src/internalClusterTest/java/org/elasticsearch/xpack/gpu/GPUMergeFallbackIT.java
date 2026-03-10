/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gpu.CuVSGPUSupport;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xpack.gpu.GPUPlugin.VECTORS_INDEXING_USE_GPU_NODE_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Integration test that exercises the GPU merge fallback path where vector data exceeds
 * {@link MMapDirectory}'s max chunk size and we resort to copying vector data to a temporary
 * file and mapping it as a single contiguous {@code MemorySegment}.
 * <p>
 * Uses a custom store type with a very small max chunk size so that even moderate amounts
 * of vector data trigger the file-backed fallback during merge.
 */
@LuceneTestCase.SuppressCodecs("*")
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class GPUMergeFallbackIT extends ESIntegTestCase {

    static final String SMALL_CHUNK_STORE_TYPE = "small_chunk_mmapfs";
    static final int SMALL_MAX_CHUNK_SIZE = 1024;

    public static class SmallChunkMMapStorePlugin extends Plugin implements IndexStorePlugin {
        @Override
        public Map<String, DirectoryFactory> getDirectoryFactories() {
            return Map.of(SMALL_CHUNK_STORE_TYPE, new SmallChunkDirectoryFactory());
        }
    }

    static class SmallChunkDirectoryFactory implements IndexStorePlugin.DirectoryFactory {
        @Override
        public Directory newDirectory(IndexSettings indexSettings, ShardPath shardPath) throws IOException {
            Path location = shardPath.resolveIndex();
            Files.createDirectories(location);
            return new MMapDirectory(location, SMALL_MAX_CHUNK_SIZE);
        }
    }

    public static class TestGPUPlugin extends GPUPlugin {
        public TestGPUPlugin(Settings settings) {
            super(settings);
        }

        @Override
        protected boolean isGpuIndexingFeatureAllowed() {
            return true;
        }

        @Override
        public List<ActionPlugin.ActionHandler> getActions() {
            return List.of();
        }
    }

    @BeforeClass
    public static void checkGPUSupport() {
        assumeTrue("cuvs not supported", CuVSGPUSupport.instance().isSupported());
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestGPUPlugin.class, SmallChunkMMapStorePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(VECTORS_INDEXING_USE_GPU_NODE_SETTING.getKey(), GPUPlugin.GpuMode.TRUE.name())
            .build();
    }

    public void testForceMergeWithFileFallbackHnsw() {
        doTestForceMergeWithFileFallback("hnsw");
    }

    public void testForceMergeWithFileFallbackInt8Hnsw() {
        doTestForceMergeWithFileFallback("int8_hnsw");
    }

    /**
     * Indexes vector data across multiple segments so the merged data exceeds the small max chunk
     * size, then force-merges. This exercises the path where a contiguous memory segment cannot be
     * obtained directly and we resort to copying vector data to a temporary file.
     */
    private void doTestForceMergeWithFileFallback(String type) {
        String indexName = "gpu_merge_fallback_" + type.replace("_", "");
        int dims = randomIntBetween(32, 128);
        String similarity = randomFrom("dot_product", "l2_norm", "cosine");

        Settings indexSettings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.store.type", SMALL_CHUNK_STORE_TYPE)
            .build();

        String mapping = String.format(Locale.ROOT, """
            {
              "properties": {
                "my_vector": {
                  "type": "dense_vector",
                  "dims": %d,
                  "similarity": "%s",
                  "index_options": {
                    "type": "%s",
                    "ef_construction": 200,
                    "m": 24
                  }
                }
              }
            }
            """, dims, similarity, type);

        assertAcked(prepareCreate(indexName).setSettings(indexSettings).setMapping(mapping));
        ensureGreen();

        int docsPerBatch = randomIntBetween(1000, 2000);
        int numBatches = randomIntBetween(2, 4);
        int totalDocs = 0;

        int bytesPerVector = "int8_hnsw".equals(type) ? dims + 4 : dims * Float.BYTES;
        long expectedMergedSize = (long) docsPerBatch * numBatches * bytesPerVector;
        assert expectedMergedSize > SMALL_MAX_CHUNK_SIZE
            : "merged vector data [" + expectedMergedSize + "] must exceed max chunk size [" + SMALL_MAX_CHUNK_SIZE + "]";

        for (int batch = 0; batch < numBatches; batch++) {
            BulkRequestBuilder bulkRequest = client().prepareBulk();
            for (int i = 0; i < docsPerBatch; i++) {
                String id = String.valueOf(totalDocs + i);
                float[] vector = randomFloatVector(dims, similarity);
                bulkRequest.add(prepareIndex(indexName).setId(id).setSource("my_vector", vector));
            }
            BulkResponse bulkResponse = bulkRequest.get();
            assertFalse("Bulk request failed: " + bulkResponse.buildFailureMessage(), bulkResponse.hasFailures());
            totalDocs += docsPerBatch;
            flushAndRefresh(indexName);
        }

        long segmentsBefore = indicesAdmin().prepareStats(indexName)
            .clear()
            .setSegments(true)
            .get()
            .getPrimaries()
            .getSegments()
            .getCount();
        assertThat("expected multiple segments before merge", segmentsBefore, greaterThan(1L));

        int k = 50;
        int numCandidates = k * 5;
        float[] queryVector = randomFloatVector(dims, similarity);

        Set<String> idsBeforeMerge = new HashSet<>();
        var responseBefore = prepareSearch(indexName).setSize(k)
            .setFetchSource(false)
            .setKnnSearch(List.of(new KnnSearchBuilder("my_vector", queryVector, k, numCandidates, null, null, null)))
            .get();
        try {
            SearchHit[] hitsBefore = responseBefore.getHits().getHits();
            assertEquals(k, hitsBefore.length);
            for (SearchHit hit : hitsBefore) {
                idsBeforeMerge.add(hit.getId());
            }
        } finally {
            responseBefore.decRef();
        }

        assertNoFailures(indicesAdmin().prepareForceMerge(indexName).setMaxNumSegments(1).get());

        long segmentsAfter = indicesAdmin().prepareStats(indexName).clear().setSegments(true).get().getPrimaries().getSegments().getCount();
        assertThat("expected a single segment after merge", segmentsAfter, equalTo(1L));

        var responseAfter = prepareSearch(indexName).setSize(k)
            .setFetchSource(false)
            .setKnnSearch(List.of(new KnnSearchBuilder("my_vector", queryVector, k, numCandidates, null, null, null)))
            .get();
        try {
            SearchHit[] hitsAfter = responseAfter.getHits().getHits();
            assertEquals(k, hitsAfter.length);
            assertAtLeastNOutOfKMatches(idsBeforeMerge, hitsAfter, k / 2, k);
        } finally {
            responseAfter.decRef();
        }
    }

    private static void assertAtLeastNOutOfKMatches(Set<String> idsBefore, SearchHit[] hitsAfter, int minMatches, int k) {
        int matches = 0;
        for (SearchHit hit : hitsAfter) {
            if (idsBefore.contains(hit.getId())) {
                matches++;
            }
        }
        assertTrue(
            "Expected at least " + minMatches + " out of " + k + " results to match before/after merge, but got " + matches,
            matches >= minMatches
        );
    }

    private static float[] randomFloatVector(int dims, String similarity) {
        if ("dot_product".equals(similarity)) {
            return randomUnitVector(dims);
        }
        float[] vector = new float[dims];
        for (int i = 0; i < dims; i++) {
            vector[i] = randomFloat();
        }
        return vector;
    }

    private static float[] randomUnitVector(int dims) {
        float[] vector = new float[dims];
        double sumSquares = 0.0;
        for (int i = 0; i < dims; i++) {
            vector[i] = randomFloat() * 2 - 1;
            sumSquares += vector[i] * vector[i];
        }
        float magnitude = (float) Math.sqrt(sumSquares);
        if (magnitude > 0) {
            for (int i = 0; i < dims; i++) {
                vector[i] /= magnitude;
            }
        }
        return vector;
    }
}
