/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.lucene;

import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Verifies that a stateless indexing node builds a correct HNSW graph while scoring quantized vectors through
 * {@link IndexDirectory.ReopeningIndexInput}. The merge reads back the vectors it has just written, so a wrong memory segment, or a wrong
 * offset within one, corrupts the graph; querying afterwards with a vector taken from the merged segment is what detects that.
 */
public class ReopeningIndexInputDirectAccessIT extends AbstractStatelessPluginIntegTestCase {

    private static final String VECTOR_FIELD = "vector";
    private static final int DIMENSIONS = 128;
    private static final int DOCS_PER_SEGMENT = 50;
    private static final int SEGMENTS = 4;
    private static final int TOTAL_DOCS = SEGMENTS * DOCS_PER_SEGMENT;

    public void testQuantizedVectorsSurviveAMerge() throws Exception {
        var indexNode = startMasterAndIndexNode();

        var indexName = randomIdentifier();
        assertAcked(
            indicesAdmin().prepareCreate(indexName)
                .setSettings(indexSettings(1, 0).build())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject(VECTOR_FIELD)
                        .field("type", "dense_vector")
                        .field("dims", DIMENSIONS)
                        .field("similarity", "cosine")
                        .startObject("index_options")
                        .field("type", "int4_hnsw")
                        // without this the merge skips graph construction below ES93HnswVectorsFormat#HNSW_GRAPH_THRESHOLD vectors, and a
                        // merge that builds no graph scores no vectors
                        .field("flat_index_threshold", 0)
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
        );

        for (int segment = 0; segment < SEGMENTS; segment++) {
            var bulkRequest = client().prepareBulk();
            for (int i = 0; i < DOCS_PER_SEGMENT; i++) {
                bulkRequest.add(new IndexRequest(indexName).source(Map.of(VECTOR_FIELD, boxed(randomUnitVector()))));
            }
            assertNoFailures(bulkRequest.get());
            refresh(indexName);
        }

        var shard = indexingShard(indexNode, indexName);

        assertNoFailures(indicesAdmin().prepareForceMerge(indexName).setMaxNumSegments(1).get());
        waitForMerges();
        refresh(indexName);

        assertMergedVectorsAreSearchable(shard);
    }

    /**
     * Reads the merged graph back on the node that wrote it. Querying with a vector taken from the segment itself must return that same
     * document, which cannot happen if the merge scored the wrong bytes.
     */
    private void assertMergedVectorsAreSearchable(IndexShard shard) throws IOException {
        try (var searcher = shard.acquireSearcher("test")) {
            var leaves = searcher.getDirectoryReader().leaves();
            assertThat("expected a single force-merged segment", leaves, hasSize(1));

            var leaf = leaves.getFirst();
            var values = leaf.reader().getFloatVectorValues(VECTOR_FIELD);
            assertThat(values, notNullValue());
            assertThat(values.size(), equalTo(TOTAL_DOCS));

            for (int i = 0; i < 10; i++) {
                var ordinal = randomIntBetween(0, values.size() - 1);
                var query = values.copy().vectorValue(ordinal).clone();
                var expectedDoc = values.ordToDoc(ordinal) + leaf.docBase;

                var topDocs = searcher.search(new KnnFloatVectorQuery(VECTOR_FIELD, query, 10), 10);
                assertThat("query taken from doc " + expectedDoc + " did not find it", docIds(topDocs), hasItem(expectedDoc));
            }
        }
    }

    private static IndexShard indexingShard(String indexNode, String indexName) {
        return internalCluster().getInstance(IndicesService.class, indexNode).indexServiceSafe(resolveIndex(indexName)).getShard(0);
    }

    private static List<Integer> docIds(TopDocs topDocs) {
        var ids = new ArrayList<Integer>();
        for (var scoreDoc : topDocs.scoreDocs) {
            ids.add(scoreDoc.doc);
        }
        return ids;
    }

    private float[] randomUnitVector() {
        var vector = new float[DIMENSIONS];
        double squaredMagnitude = 0.0;
        while (squaredMagnitude == 0.0) {
            for (int i = 0; i < DIMENSIONS; i++) {
                vector[i] = randomFloatBetween(-1.0f, 1.0f, true);
                squaredMagnitude += (double) vector[i] * vector[i];
            }
        }
        var magnitude = (float) Math.sqrt(squaredMagnitude);
        for (int i = 0; i < DIMENSIONS; i++) {
            vector[i] /= magnitude;
        }
        return vector;
    }

    private static List<Float> boxed(float[] vector) {
        var boxed = new ArrayList<Float>(vector.length);
        for (var value : vector) {
            boxed.add(value);
        }
        return boxed;
    }
}
