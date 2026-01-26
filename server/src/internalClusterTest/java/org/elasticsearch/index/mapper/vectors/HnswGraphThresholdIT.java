/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.DenseVectorStats;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for HNSW graph threshold setting.
 * Tests that the graph is conditionally built based on the expected search cost threshold.
 *
 * The threshold represents the minimum expected search cost before building an HNSW graph becomes worthwhile.
 * - For bbq_hnsw: default threshold is 300, graph is built when vectors > 2327
 * - For hnsw/int8_hnsw: default threshold is 150, graph is built when vectors > 1045
 */
@LuceneTestCase.SuppressCodecs("*") // only use our own codecs to ensure HNSW threshold is applied
public class HnswGraphThresholdIT extends ESIntegTestCase {

    private static final String INDEX_NAME = "hnsw_threshold_test";
    private static final String VECTOR_FIELD = "vector";
    private static final int DIMENSIONS = 64;

    // Thresholds for different index types
    private static final int BBQ_HNSW_THRESHOLD = 300;
    private static final int HNSW_THRESHOLD = 150;

    // Number of vectors needed to exceed the threshold (based on search power calculation)
    private static final int BBQ_HNSW_VECTORS_FOR_GRAPH = 2327;
    private static final int HNSW_VECTORS_FOR_GRAPH = 1045;

    /**
     * Tests that with default threshold, graph is NOT built for small vector counts,
     * but IS built when vectors exceed the search power threshold.
     */
    public void testGraphThresholdWithDefaultSettings() throws Exception {
        IndexTypeConfig config = randomIndexTypeConfig();
        logger.info("Testing with index type: {}, element type: {}", config.indexType, config.elementType);

        // Create index with default threshold
        assertAcked(
            prepareCreate(INDEX_NAME).setSettings(
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            ).setMapping(createMapping(config.indexType, config.elementType))
        );
        ensureGreen(INDEX_NAME);

        // Index vectors below the threshold
        int smallDocCount = 100;
        indexVectors(smallDocCount, 0, config.elementType);
        flushAndRefresh(INDEX_NAME);

        // Verify: with small vector count, graph should NOT be built
        DenseVectorStats statsBeforeThreshold = getDenseVectorStats();
        assertThat("Should have indexed vectors", statsBeforeThreshold.getValueCount(), equalTo((long) smallDocCount));
        Long vexSizeSmall = getVexSize(statsBeforeThreshold);
        assertTrue(
            "Graph should NOT be built with " + smallDocCount + " vectors (below threshold for " + config.indexType + ")",
            vexSizeSmall == null || vexSizeSmall == 0L
        );

        // Index more vectors to exceed the threshold
        int additionalDocs = config.vectorsForGraph;
        indexVectors(additionalDocs, smallDocCount, config.elementType);
        flushAndRefresh(INDEX_NAME);

        // Force merge to ensure all vectors are in one segment
        forceMergeIndex();

        // Verify: with vectors above threshold, graph SHOULD be built
        DenseVectorStats statsAfterThreshold = getDenseVectorStats();
        int totalDocs = smallDocCount + additionalDocs;
        assertThat("Should have indexed all vectors", statsAfterThreshold.getValueCount(), equalTo((long) totalDocs));
        Long vexSizeLarge = getVexSize(statsAfterThreshold);
        assertThat(
            "Graph SHOULD be built with " + totalDocs + " vectors (above threshold for " + config.indexType + ")",
            vexSizeLarge,
            notNullValue()
        );
        assertThat("Graph size should be positive", vexSizeLarge, greaterThan(0L));
    }

    /**
     * Tests that setting threshold=0 forces graph to always be built, even with few vectors.
     */
    public void testGraphAlwaysBuiltWithThresholdZero() throws Exception {
        IndexTypeConfig config = randomIndexTypeConfig();
        logger.info("Testing threshold=0 with index type: {}, element type: {}", config.indexType, config.elementType);

        // Create index with threshold=0 (always build graph)
        assertAcked(
            prepareCreate(INDEX_NAME).setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put("index.dense_vector.hnsw_graph_threshold", 0)
            ).setMapping(createMapping(config.indexType, config.elementType))
        );
        ensureGreen(INDEX_NAME);

        // Index small number of vectors
        int smallDocCount = 10;
        indexVectors(smallDocCount, 0, config.elementType);
        flushAndRefresh(INDEX_NAME);

        // Verify: with threshold=0, graph SHOULD be built even with few vectors
        DenseVectorStats stats = getDenseVectorStats();
        assertThat("Should have indexed vectors", stats.getValueCount(), equalTo((long) smallDocCount));
        Long vexSize = getVexSize(stats);
        assertThat("Graph SHOULD be built with threshold=0", vexSize, notNullValue());
        assertThat("Graph size should be positive", vexSize, greaterThan(0L));
    }

    private record IndexTypeConfig(String indexType, String elementType, int threshold, int vectorsForGraph) {}

    private IndexTypeConfig randomIndexTypeConfig() {
        String indexType = randomFrom("hnsw", "int8_hnsw", "bbq_hnsw");
        String elementType;
        int threshold;
        int vectorsForGraph;

        switch (indexType) {
            case "bbq_hnsw" -> {
                elementType = randomFrom("float", "bfloat16");
                threshold = BBQ_HNSW_THRESHOLD;
                vectorsForGraph = BBQ_HNSW_VECTORS_FOR_GRAPH;
            }
            case "int8_hnsw" -> {
                elementType = randomFrom("float", "bfloat16");
                threshold = HNSW_THRESHOLD;
                vectorsForGraph = HNSW_VECTORS_FOR_GRAPH;
            }
            default -> { // hnsw
                elementType = randomFrom("float", "byte", "bfloat16");
                threshold = HNSW_THRESHOLD;
                vectorsForGraph = HNSW_VECTORS_FOR_GRAPH;
            }
        }
        return new IndexTypeConfig(indexType, elementType, threshold, vectorsForGraph);
    }

    private XContentBuilder createMapping(String indexType, String elementType) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(VECTOR_FIELD)
            .field("type", "dense_vector")
            .field("dims", DIMENSIONS)
            .field("index", true)
            .field("similarity", "l2_norm")
            .field("element_type", elementType)
            .startObject("index_options")
            .field("type", indexType)
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        return builder;
    }

    private void indexVectors(int count, int startId, String elementType) {
        for (int i = 0; i < count; i++) {
            Object vector = generateVector(elementType);
            client().index(new IndexRequest(INDEX_NAME).id(String.valueOf(startId + i)).source(Map.of(VECTOR_FIELD, vector))).actionGet();
        }
    }

    private Object generateVector(String elementType) {
        if ("byte".equals(elementType)) {
            List<Integer> vector = new ArrayList<>(DIMENSIONS);
            for (int j = 0; j < DIMENSIONS; j++) {
                vector.add(randomIntBetween(-128, 127));
            }
            return vector;
        } else {
            // float, bfloat16 - all use float values in the API
            List<Float> vector = new ArrayList<>(DIMENSIONS);
            for (int j = 0; j < DIMENSIONS; j++) {
                vector.add(randomFloat());
            }
            return vector;
        }
    }

    private void flushAndRefresh(String indexName) {
        indicesAdmin().prepareFlush(indexName).get();
        indicesAdmin().prepareRefresh(indexName).get();
    }

    private void forceMergeIndex() {
        indicesAdmin().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).get();
        flushAndRefresh(INDEX_NAME);
    }

    private DenseVectorStats getDenseVectorStats() {
        IndicesStatsResponse statsResponse = indicesAdmin().prepareStats(INDEX_NAME).setDenseVector(true).get();
        return statsResponse.getIndex(INDEX_NAME).getTotal().getDenseVectorStats();
    }

    /**
     * Gets the vex (HNSW graph) size for the vector field.
     * Returns null if no graph exists.
     */
    private Long getVexSize(DenseVectorStats stats) {
        if (stats.offHeapStats() == null) {
            return null;
        }
        Map<String, Long> fieldStats = stats.offHeapStats().get(VECTOR_FIELD);
        if (fieldStats == null) {
            return null;
        }
        return fieldStats.get("vex");
    }
}
