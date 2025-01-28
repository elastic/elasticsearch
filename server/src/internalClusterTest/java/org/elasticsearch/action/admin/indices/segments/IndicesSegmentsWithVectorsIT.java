/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.segments;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

@ClusterScope(scope = ESIntegTestCase.Scope.TEST)
@LuceneTestCase.SuppressCodecs("*")
public class IndicesSegmentsWithVectorsIT extends ESIntegTestCase {

    public void testIndicesSegmentsWithVectorsDefault() {
        String indexName = "test-vectors";
        createIndex(indexName);
        ensureGreen(indexName);

        String vectorField = "embedding";
        addMapping(indexName, vectorField);
        addVectors(indexName, vectorField, 100, 100);

        IndicesSegmentResponse response = indicesAdmin().prepareSegments(indexName).get();
        assertNoFailures(response);

        IndexSegments indexSegments = response.getIndices().get(indexName);
        assertNotNull(indexSegments);
        IndexShardSegments shardSegments = indexSegments.getShards().get(0);
        assertNotNull(shardSegments);
        ShardSegments shard = shardSegments.shards()[0];
        for (Segment segment : shard.getSegments()) {
            assertThat(segment.getAttributes().keySet(), not(hasItem(endsWith("VectorsFormat"))));
            assertThat(segment.getAttributes().values(), not(hasItem("[" + vectorField + "]")));
        }
    }

    public void testIndicesSegmentsWithVectorsIncluded() {
        String indexName = "test-vectors";
        createIndex(indexName);
        ensureGreen(indexName);

        String vectorField = "embedding";
        addMapping(indexName, vectorField);
        addVectors(indexName, vectorField, 100, 100);
        IndicesSegmentResponse response = indicesAdmin().prepareSegments(indexName).includeVectorFormatInfo(true).get();
        assertNoFailures(response);

        IndexSegments indexSegments = response.getIndices().get(indexName);
        assertNotNull(indexSegments);
        IndexShardSegments shardSegments = indexSegments.getShards().get(0);
        assertNotNull(shardSegments);
        ShardSegments shard = shardSegments.shards()[0];
        for (Segment segment : shard.getSegments()) {
            assertThat(segment.getAttributes().keySet(), hasItem(endsWith("VectorsFormat")));
            assertThat(segment.getAttributes().values(), hasItem("[" + vectorField + "]"));
        }
    }

    public void testIndicesSegmentsWithVectorsNotIncluded() {
        String indexName = "test-vectors";
        createIndex(indexName);
        ensureGreen(indexName);

        String vectorField = "embedding";
        addMapping(indexName, vectorField);
        addVectors(indexName, vectorField, 100, 100);

        IndicesSegmentResponse response = indicesAdmin().prepareSegments(indexName).includeVectorFormatInfo(false).get();
        assertNoFailures(response);

        IndexSegments indexSegments = response.getIndices().get(indexName);
        assertNotNull(indexSegments);
        IndexShardSegments shardSegments = indexSegments.getShards().get(0);
        assertNotNull(shardSegments);
        ShardSegments shard = shardSegments.shards()[0];
        for (Segment segment : shard.getSegments()) {
            assertThat(segment.getAttributes().keySet(), not(hasItem(endsWith("VectorsFormat"))));
            assertThat(segment.getAttributes().values(), not(hasItem("[" + vectorField + "]")));
        }
    }

    private static void addMapping(String indexName, String vectorField) {
        PutMappingRequest request = new PutMappingRequest().indices(indexName)
            .origin(randomFrom("1", "2"))
            .source(vectorField, "type=dense_vector");
        assertAcked(indicesAdmin().putMapping(request).actionGet());
    }

    private static void addVectors(String indexName, String vectorField, int maxDims, int maxDocs) {
        int docs = between(10, maxDocs);
        int dims = between(10, maxDims);
        for (int i = 0; i < docs; i++) {
            List<Float> floats = randomList(dims, dims, ESTestCase::randomFloat);
            prepareIndex(indexName).setId("" + i).setSource(vectorField, floats).get();
        }
        indicesAdmin().prepareFlush(indexName).get();
    }

    @Monster("opens many files")
    public void testManyIndicesSegmentsWithVectorsIncluded() {
        String indexName = "test-vectors";
        createIndex(indexName);
        ensureGreen(indexName);

        int numVectorFields = randomIntBetween(100, 1000);
        for (int i = 0; i < numVectorFields; i++) {
            String vectorField = "embedding_" + i;
            addMapping(indexName, vectorField);
            addVectors(indexName, vectorField, 1024, 1000);
        }

        long start = System.currentTimeMillis();
        IndicesSegmentResponse response = indicesAdmin().prepareSegments(indexName).includeVectorFormatInfo(false).get();
        long end = System.currentTimeMillis();
        long timeMS = end - start;

        assertNoFailures(response);
        IndexSegments indexSegments = response.getIndices().get(indexName);
        assertNotNull(indexSegments);
        IndexShardSegments shardSegments = indexSegments.getShards().get(0);
        assertNotNull(shardSegments);

        ShardSegments shard = shardSegments.shards()[0];
        for (Segment segment : shard.getSegments()) {
            assertThat(segment.getAttributes().keySet(), not(hasItem(endsWith("VectorsFormat"))));
        }

        indicesAdmin().prepareClearCache(indexName).get();

        long startIncluded = System.currentTimeMillis();
        IndicesSegmentResponse responseIncluded = indicesAdmin().prepareSegments(indexName).includeVectorFormatInfo(true).get();
        long endIncluded = System.currentTimeMillis();
        long includedTimeMS = endIncluded - startIncluded;

        assertNoFailures(responseIncluded);
        IndexSegments indexSegmentsIncluded = responseIncluded.getIndices().get(indexName);
        assertNotNull(indexSegmentsIncluded);
        IndexShardSegments shardSegmentsIncluded = indexSegmentsIncluded.getShards().get(0);
        assertNotNull(shardSegmentsIncluded);

        shard = shardSegmentsIncluded.shards()[0];
        for (Segment segment : shard.getSegments()) {
            assertThat(segment.getAttributes().keySet(), hasItem(endsWith("VectorsFormat")));
        }

        assertThat(includedTimeMS, lessThan(timeMS * numVectorFields));
    }
}
