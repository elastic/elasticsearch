/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
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
 *
 * <p>Post-filtering is dormant by default (the {@code index.dense_vector.post_filter_selectivity_threshold}
 * setting defaults to {@link PostFilterKnnQuery#DEFAULT_POST_FILTERING_THRESHOLD} == 1.0, so only no-op
 * filters would ever qualify). Each index here lowers the threshold to {@code 0.7} at creation time so the
 * ~0.8-selectivity filters used below take the post-filter path. The setting carries
 * {@link org.elasticsearch.common.settings.Setting.Property#PrivateIndex}, so it cannot be set
 * explicitly through the index-creation API unless {@link #forbidPrivateIndexSettings()} is
 * overridden to {@code false}, which this suite does.
 */
public class PostFilterHnswKnnSearchIT extends ESIntegTestCase {

    private static final String VECTOR_FIELD = "vector";
    private static final String TAG_FIELD = "tag";
    private static final String NESTED_FIELD = "nested_field";
    private static final int DIMS = 4;
    private static final float POST_FILTER_THRESHOLD = 0.7f;

    @Override
    public Settings indexSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(DenseVectorFieldMapper.POST_FILTER_SELECTIVITY_THRESHOLD.getKey(), POST_FILTER_THRESHOLD)
            .build();
    }

    @Override
    protected boolean forbidPrivateIndexSettings() {
        // The post-filter selectivity threshold is a PrivateIndex setting; allow it to be set
        // explicitly at index-creation time so these tests can lower it from the dormant default.
        return false;
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

    /**
     * Deterministic tag layout: first 80 docs are "pass", last 20 are "fail".
     * Query is nearest to "fail" docs. Post-filtering may not find k results via retry alone,
     * verifying correctness of the fallback to the original pre-filtered query.
     */
    public void testHnswFloatFallback() throws IOException {
        String indexName = "hnsw_float_fallback_test";
        createHnswIndex(indexName, "float");
        indexFlatDocsDeterministic(indexName);
        assertPostFilterFallback(indexName, new float[] { 1, 1, 1, 100 });
    }

    /**
     * Index sorted by price ascending, vectors correlate with price ([1,1,1,price]).
     * Query nearest to expensive docs, range filter requires price &lt; 80.
     * Simulates a production scenario where index sorting causes kNN proximity to align with filter boundaries.
     */
    public void testHnswFloatSortedIndex() throws IOException {
        String indexName = "hnsw_float_sorted_test";
        createSortedHnswIndex(indexName, "float");
        indexSortedDocs(indexName);
        assertPostFilterSorted(indexName, new float[] { 1, 1, 1, 100 });
    }

    public void testPostFilterReportsVectorOpsInProfile() throws IOException {
        String indexName = "profile_test";
        createHnswIndex(indexName, "float");
        indexFlatDocs(indexName);

        int k = 5;
        var knnSearch = new KnnSearchBuilder(VECTOR_FIELD, new float[] { 1, 1, 1, 20 }, k, 20, null, null, null).addFilterQuery(
            QueryBuilders.termQuery(TAG_FIELD, "common")
        );

        assertResponse(client().prepareSearch(indexName).setKnnSearch(List.of(knnSearch)).setSize(k).setProfile(true), response -> {
            assertNotEquals(0, response.getHits().getHits().length);
            assertProfileReportsVectorOps(response);
        });
    }

    /**
     * Asserts the profile reports a non-zero {@code vector_operations_count}. Exercises the
     * post-filter accounting path that accumulates round-0 + retry (and, on fallback, the inner
     * query's) vector operations into {@code PostFilterKnnQuery#totalVectorOps}.
     */
    private static void assertProfileReportsVectorOps(SearchResponse response) {
        var shardResults = response.getSearchProfileShardResults();
        assertFalse("Profile results should not be empty", shardResults.isEmpty());
        long vectorOpsSum = shardResults.values()
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
     * Indexes 200 flat docs with random 80/20 "common"/"rare" tag split.
     * Vectors: [1, 1, 1, i]. Expected selectivity(common) ~ 0.8 &gt; 0.7 → post-filter.
     * For byte element type the mapping handles float-to-byte conversion.
     */
    private void indexFlatDocs(String indexName) {
        for (int i = 0; i < 200; i++) {
            String tag = randomFloat() < .8f ? "common" : "rare";
            prepareIndex(indexName).setId(Integer.toString(i))
                .setSource(VECTOR_FIELD, new float[] { 1, 1, 1, randomIntBetween(-128, 127) }, TAG_FIELD, tag)
                .get();
        }
        forceMerge(true);
        refresh(indexName);
    }

    /**
     * Indexes 100 parent docs, each with 2 nested children.
     * Each parent's children get a random 80/20 "common"/"rare" tag.
     * Expected selectivity(common) ~ 0.8 &gt; 0.7 → post-filter.
     */
    private void indexNestedDocs(String indexName) {
        for (int parentId = 0; parentId < 100; parentId++) {
            String tag = randomFloat() < .8f ? "common" : "rare";
            prepareIndex(indexName).setId("parent_" + parentId)
                .setSource(
                    NESTED_FIELD,
                    List.of(
                        Map.of(VECTOR_FIELD, new float[] { 1, 1, 1, randomIntBetween(-128, 127) }, TAG_FIELD, tag),
                        Map.of(VECTOR_FIELD, new float[] { 1, 1, 1, randomIntBetween(-128, 127) }, TAG_FIELD, tag)
                    )
                )
                .get();
        }
        forceMerge(true);
        refresh(indexName);
    }

    /**
     * Query vector is nearest to high-index docs, filter requires "common".
     * With random tag assignment we cannot predict exact doc IDs, but all results must be "common".
     */
    private void assertPostFilterFlat(String indexName, float[] queryVector) {
        int k = 5;
        var knnSearch = new KnnSearchBuilder(VECTOR_FIELD, queryVector, k, 20, null, null, null).addFilterQuery(
            QueryBuilders.termQuery(TAG_FIELD, "common")
        );

        assertResponse(client().prepareSearch(indexName).setKnnSearch(List.of(knnSearch)).setSize(k), response -> {
            assertTrue("Expected at least 1 result", response.getHits().getHits().length > 0);
            for (SearchHit hit : response.getHits().getHits()) {
                assertEquals("common", hit.getSourceAsMap().get(TAG_FIELD));
            }
        });
    }

    /**
     * Nested variant: query vector is nearest to high-index children, filter requires "common".
     * With random tag assignment we cannot predict exact parent IDs, but all results must be "common" parents.
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
            for (SearchHit hit : response.getHits().getHits()) {
                assertTrue("Expected parent doc ID", hit.getId().startsWith("parent_"));
            }
        });
    }

    /**
     * Indexes 100 flat docs with deterministic tags: 0-79 = "pass", 80-99 = "fail".
     * selectivity(pass) = 0.8 &gt; 0.7 → post-filter. The query neighborhood (high-index docs) is all "fail".
     */
    private void indexFlatDocsDeterministic(String indexName) {
        for (int i = 0; i < 100; i++) {
            String tag = i < 80 ? "pass" : "fail";
            prepareIndex(indexName).setId(Integer.toString(i)).setSource(VECTOR_FIELD, new float[] { 1, 1, 1, i }, TAG_FIELD, tag).get();
        }
        forceMerge(true);
        refresh(indexName);
    }

    private void assertPostFilterFallback(String indexName, float[] queryVector) {
        int k = 5;
        var knnSearch = new KnnSearchBuilder(VECTOR_FIELD, queryVector, k, 100, null, null, null).addFilterQuery(
            QueryBuilders.termQuery(TAG_FIELD, "pass")
        );

        assertResponse(client().prepareSearch(indexName).setKnnSearch(List.of(knnSearch)).setSize(k).setProfile(true), response -> {
            assertEquals("Expected exactly k results", k, response.getHits().getHits().length);
            for (SearchHit hit : response.getHits().getHits()) {
                assertEquals("pass", hit.getSourceAsMap().get(TAG_FIELD));
            }
            // Fallback path runs the inner query after post-filter retry comes up short — its
            // vectorOps must still be reported in the profile.
            assertProfileReportsVectorOps(response);
        });
    }

    private void createSortedHnswIndex(String indexName, String elementType) throws IOException {
        Settings sortedSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(DenseVectorFieldMapper.POST_FILTER_SELECTIVITY_THRESHOLD.getKey(), POST_FILTER_THRESHOLD)
            .put("index.sort.field", "price")
            .put("index.sort.order", "asc")
            .build();
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
            .startObject("price")
            .field("type", "integer")
            .endObject()
            .endObject()
            .endObject();
        prepareCreate(indexName).setSettings(sortedSettings).setMapping(mapping).get();
        ensureGreen(indexName);
    }

    /**
     * Indexes 100 docs with price=i and vector=[1,1,1,i]. On a sorted index, segment doc IDs
     * follow price order, causing kNN proximity to align with the price-based filter boundary.
     */
    private void indexSortedDocs(String indexName) {
        for (int i = 0; i < 100; i++) {
            prepareIndex(indexName).setId(Integer.toString(i)).setSource(VECTOR_FIELD, new float[] { 1, 1, 1, i }, "price", i).get();
        }
        forceMerge(true);
        refresh(indexName);
    }

    /**
     * Query vector nearest to expensive docs (price >= 80), range filter requires price &lt; 80.
     * selectivity = 80/100 = 0.8 → would post-filter, but the index is sorted so post-filtering is
     * disabled and the pre-filter path must still return exactly k in-range hits.
     */
    private void assertPostFilterSorted(String indexName, float[] queryVector) {
        int k = 5;
        var knnSearch = new KnnSearchBuilder(VECTOR_FIELD, queryVector, k, 100, null, null, null).addFilterQuery(
            QueryBuilders.rangeQuery("price").lt(80)
        );

        assertResponse(client().prepareSearch(indexName).setKnnSearch(List.of(knnSearch)).setSize(k), response -> {
            assertEquals("Expected exactly k results", k, response.getHits().getHits().length);
            for (SearchHit hit : response.getHits().getHits()) {
                int price = (int) hit.getSourceAsMap().get("price");
                assertTrue("Expected price < 80, got " + price, price < 80);
            }
        });
    }
}
