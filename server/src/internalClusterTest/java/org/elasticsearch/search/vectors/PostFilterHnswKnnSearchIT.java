/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Integration tests for kNN search with post-filtering across different
 * element types (float, byte), index types (HNSW), and field structures (flat, nested).
 */
public class PostFilterHnswKnnSearchIT extends ESIntegTestCase {

    private static final String VECTOR_FIELD = "vector";
    private static final String TAG_FIELD = "tag";
    private static final String NESTED_FIELD = "nested_field";
    private static final int DIMS = 4;

    @Override
    public Settings indexSettings() {
        return Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).build();
    }

    public void testHnswFloat() throws IOException {
        String indexName = "hnsw_float_test";
        createHnswIndex(indexName, "float");
        indexFlatDocs(indexName);
        assertPostFilterFlat(indexName, new float[] { 1, 1, 1, 20 });
    }

    public void testHnswByte() throws IOException {
        String indexName = "hnsw_byte_test";
        createHnswIndex(indexName, "byte");
        indexFlatDocs(indexName);
        assertPostFilterFlat(indexName, new float[] { 1, 1, 1, 20 });
    }

    public void testHnswFloatNested() throws IOException {
        String indexName = "hnsw_float_nested_test";
        createHnswNestedIndex(indexName, "float");
        indexNestedDocs(indexName);
        assertPostFilterNested(indexName, new float[] { 1, 1, 1, 20 });
    }

    public void testHnswByteNested() throws IOException {
        String indexName = "hnsw_byte_nested_test";
        createHnswNestedIndex(indexName, "byte");
        indexNestedDocs(indexName);
        assertPostFilterNested(indexName, new float[] { 1, 1, 1, 20 });
    }

    public void testPostFilterReportsVectorOpsInProfile() throws IOException {
        String indexName = "profile_test";
        createHnswIndex(indexName, "float");
        indexFlatDocs(indexName);

        int k = 5;
        var knnSearch = new KnnSearchBuilder(VECTOR_FIELD, new float[] { 1, 1, 1, 20 }, k, 20, null, null, null).addFilterQuery(
            QueryBuilders.termQuery(TAG_FIELD, "common")
        );

        assertResponse(
            client().prepareSearch(indexName).setKnnSearch(List.of(knnSearch)).setSize(k).setProfile(true),
            response -> {
                assertNotEquals(0, response.getHits().getHits().length);
                var profileResults = response.getProfileResults();
                assertFalse("Profile results should not be empty", profileResults.isEmpty());
                long vectorOpsSum = profileResults.values()
                    .stream()
                    .mapToLong(
                        pr -> pr.getQueryPhase()
                            .getSearchProfileDfsPhaseResult()
                            .getQueryProfileShardResult()
                            .stream()
                            .mapToLong(qpr -> qpr.getVectorOperationsCount().longValue())
                            .sum()
                    )
                    .sum();
                assertThat("Expected vector operations to be reported in profile", vectorOpsSum, greaterThan(0L));
            }
        );
    }

    private void createHnswIndex(String indexName, String elementType) throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(VECTOR_FIELD)
            .field("type", "dense_vector")
            .field("element_type", elementType)
            .field("dims", DIMS)
            .field("index", true)
            .field("similarity", "l2_norm")
            .startObject("index_options")
            .field("type", "hnsw")
            .field("m", 16)
            .field("ef_construction", 200)
            .endObject()
            .endObject()
            .startObject(TAG_FIELD)
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject();
        prepareCreate(indexName).setMapping(mapping).get();
        ensureGreen(indexName);
    }

    private void createHnswNestedIndex(String indexName, String elementType) throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(NESTED_FIELD)
            .field("type", "nested")
            .startObject("properties")
            .startObject(VECTOR_FIELD)
            .field("type", "dense_vector")
            .field("element_type", elementType)
            .field("dims", DIMS)
            .field("index", true)
            .field("similarity", "l2_norm")
            .startObject("index_options")
            .field("type", "hnsw")
            .field("m", 16)
            .field("ef_construction", 200)
            .endObject()
            .endObject()
            .startObject(TAG_FIELD)
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        prepareCreate(indexName).setMapping(mapping).get();
        ensureGreen(indexName);
    }

    /**
     * Indexes 20 flat docs: 16 "common" (ids 0-15), 4 "rare" (ids 16-19).
     * Vectors: [1, 1, 1, i]. selectivity(common) = 0.8 > 0.7 → post-filter.
     * For byte element type the mapping handles float-to-byte conversion.
     */
    private void indexFlatDocs(String indexName) {
        for (int i = 0; i < 20; i++) {
            String tag = i < 16 ? "common" : "rare";
            prepareIndex(indexName).setId(Integer.toString(i))
                .setSource(VECTOR_FIELD, new float[] { 1, 1, 1, i }, TAG_FIELD, tag)
                .get();
        }
        forceMerge(true);
        refresh(indexName);
    }

    /**
     * Indexes 10 parent docs, each with 2 nested children.
     * Parents 0-7: children tagged "common". Parents 8-9: children tagged "rare".
     * Total: 16 common children, 4 rare children → selectivity(common) = 0.8 > 0.7.
     */
    private void indexNestedDocs(String indexName) {
        for (int parentId = 0; parentId < 10; parentId++) {
            String tag = parentId < 8 ? "common" : "rare";
            int childIdx0 = parentId * 2;
            int childIdx1 = parentId * 2 + 1;
            prepareIndex(indexName).setId("parent_" + parentId)
                .setSource(
                    NESTED_FIELD,
                    List.of(
                        Map.of(VECTOR_FIELD, new float[] { 1, 1, 1, childIdx0 }, TAG_FIELD, tag),
                        Map.of(VECTOR_FIELD, new float[] { 1, 1, 1, childIdx1 }, TAG_FIELD, tag)
                    )
                )
                .get();
        }
        forceMerge(true);
        refresh(indexName);
    }

    /**
     * Query vector is nearest to "rare" docs, filter requires "common".
     * Verifies post-filtering returns only matching docs in correct order.
     */
    private void assertPostFilterFlat(String indexName, float[] queryVector) {
        int k = 5;
        var knnSearch = new KnnSearchBuilder(VECTOR_FIELD, queryVector, k, 20, null, null, null).addFilterQuery(
            QueryBuilders.termQuery(TAG_FIELD, "common")
        );

        assertResponse(client().prepareSearch(indexName).setKnnSearch(List.of(knnSearch)).setSize(k), response -> {
            assertEquals(k, response.getHits().getHits().length);
            for (SearchHit hit : response.getHits().getHits()) {
                assertEquals("common", hit.getSourceAsMap().get(TAG_FIELD));
            }
            // Closest common docs to [1,1,1,20]: 15, 14, 13, 12, 11
            assertEquals("15", response.getHits().getHits()[0].getId());
            assertEquals("14", response.getHits().getHits()[1].getId());
            assertEquals("13", response.getHits().getHits()[2].getId());
            assertEquals("12", response.getHits().getHits()[3].getId());
            assertEquals("11", response.getHits().getHits()[4].getId());
        });
    }

    /**
     * Nested variant: query vector is nearest to "rare" children, filter requires "common".
     * Verifies post-filtering returns parent docs whose matching children are closest.
     */
    private void assertPostFilterNested(String indexName, float[] queryVector) {
        int k = 3;
        String nestedVectorField = NESTED_FIELD + "." + VECTOR_FIELD;
        String nestedTagField = NESTED_FIELD + "." + TAG_FIELD;
        var knnSearch = new KnnSearchBuilder(nestedVectorField, queryVector, k, 20, null, null, null).addFilterQuery(
            QueryBuilders.termQuery(nestedTagField, "common")
        );

        assertResponse(client().prepareSearch(indexName).setKnnSearch(List.of(knnSearch)).setSize(k), response -> {
            assertTrue("Expected at least 1 result", response.getHits().getHits().length > 0);
            // Closest common children to [1,1,1,20] are child_15 (parent_7), child_14 (parent_7),
            // child_13 (parent_6), etc. After parent dedup: parent_7, parent_6, parent_5.
            assertEquals("parent_7", response.getHits().getHits()[0].getId());
            assertEquals("parent_6", response.getHits().getHits()[1].getId());
            assertEquals("parent_5", response.getHits().getHits()[2].getId());
        });
    }
}
