/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
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
import static org.hamcrest.Matchers.lessThan;

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
    private static final int FLAT_DOC_COUNT = 1000;
    private static final int NESTED_PARENT_COUNT = 1000;
    private static final int FALLBACK_DOC_COUNT = 5000;
    private static final int FALLBACK_NEAR_PASS = 4;

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
     * Deterministic tag layout that drives the full post-filter sequence: only {@link #FALLBACK_NEAR_PASS}
     * ({@literal < k}) "pass" docs sit at the very top of the query neighborhood, followed by a long run of
     * "fail" docs, then "pass" again far away. Round-0 finds fewer than k matches, the retry round fires but
     * cannot cross the "fail" gap within its beam, and the search falls back to the original pre-filtered
     * query - which must still return exactly k "pass" hits.
     */
    public void testHnswFloatFallback() throws IOException {
        String indexName = "hnsw_float_fallback_test";
        createHnswIndex(indexName, "float");
        indexFlatDocsDeterministic(indexName);
        assertPostFilterFallback(indexName, new float[] { 1, 1, 1, FALLBACK_DOC_COUNT });
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
        assertThat("Expected vector operations to be reported in profile", sumVectorOps(response), greaterThan(0L));
    }

    /**
     * Guards against the kNN search silently falling back to Lucene's exhaustive (exact) branch instead
     * of traversing the HNSW graph. That branch scores every candidate, so the profiled
     * {@code vector_operations_count} equals {@code exactScanBaseline} (the number of vectors an exact scan
     * would touch); graph search visits far fewer. We therefore require the reported count to be positive
     * (the search ran) and strictly below the baseline (it was approximate).
     *
     * @param exactScanBaseline vectors an exact scan would score: the segment vector count for the
     *                          post-filter round-0 delegate (which strips the user filter), or the
     *                          filter cardinality for a plain pre-filtered search.
     */
    private static void assertApproximateSearch(SearchResponse response, long exactScanBaseline) {
        long vectorOps = sumVectorOps(response);
        assertThat("Expected vector operations to be reported in profile", vectorOps, greaterThan(0L));
        assertThat(
            "kNN fell back to exact search: visited " + vectorOps + " vectors, exact baseline is " + exactScanBaseline,
            vectorOps,
            lessThan(exactScanBaseline)
        );
    }

    private static long sumVectorOps(SearchResponse response) {
        var shardResults = response.getSearchProfileShardResults();
        assertFalse("Profile results should not be empty", shardResults.isEmpty());
        return shardResults.values()
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
     * Indexes {@link #FLAT_DOC_COUNT} flat docs with random 80/20 "common"/"rare" tag split.
     * Vectors: [1, 1, 1, i]. Expected selectivity(common) ~ 0.8 &gt; 0.7 → post-filter.
     * For byte element type the mapping handles float-to-byte conversion.
     */
    private void indexFlatDocs(String indexName) {
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < FLAT_DOC_COUNT; i++) {
            String tag = randomFloat() < .8f ? "common" : "rare";
            bulk.add(
                prepareIndex(indexName).setId(Integer.toString(i))
                    .setSource(VECTOR_FIELD, new float[] { 1, 1, 1, randomIntBetween(-128, 127) }, TAG_FIELD, tag)
            );
        }
        executeBulkAndRefresh(indexName, bulk);
    }

    private void executeBulkAndRefresh(String indexName, BulkRequestBuilder bulk) {
        BulkResponse response = bulk.get();
        assertFalse(response.buildFailureMessage(), response.hasFailures());
        refresh(indexName);
    }

    /**
     * Indexes {@link #NESTED_PARENT_COUNT} parent docs, each with 2 nested children (so
     * {@code NESTED_PARENT_COUNT * 2} child vectors). Each parent's children get a random 80/20
     * "common"/"rare" tag. Expected selectivity(common) ~ 0.8 &gt; 0.7 → post-filter.
     */
    private void indexNestedDocs(String indexName) {
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int parentId = 0; parentId < NESTED_PARENT_COUNT; parentId++) {
            String tag = randomFloat() < .8f ? "common" : "rare";
            bulk.add(
                prepareIndex(indexName).setId("parent_" + parentId)
                    .setSource(
                        NESTED_FIELD,
                        List.of(
                            Map.of(VECTOR_FIELD, new float[] { 1, 1, 1, randomIntBetween(-128, 127) }, TAG_FIELD, tag),
                            Map.of(VECTOR_FIELD, new float[] { 1, 1, 1, randomIntBetween(-128, 127) }, TAG_FIELD, tag)
                        )
                    )
            );
        }
        executeBulkAndRefresh(indexName, bulk);
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

        assertResponse(client().prepareSearch(indexName).setKnnSearch(List.of(knnSearch)).setSize(k).setProfile(true), response -> {
            assertTrue("Expected at least 1 result", response.getHits().getHits().length > 0);
            for (SearchHit hit : response.getHits().getHits()) {
                assertEquals("common", hit.getSourceAsMap().get(TAG_FIELD));
            }
            assertApproximateSearch(response, FLAT_DOC_COUNT);
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

        assertResponse(client().prepareSearch(indexName).setKnnSearch(List.of(knnSearch)).setSize(k).setProfile(true), response -> {
            assertTrue("Expected at least 1 result", response.getHits().getHits().length > 0);
            for (SearchHit hit : response.getHits().getHits()) {
                assertTrue("Expected parent doc ID", hit.getId().startsWith("parent_"));
            }
            assertApproximateSearch(response, NESTED_PARENT_COUNT * 2L);
        });
    }

    /**
     * Deterministic tag layout that forces the full post-filter → retry → fallback sequence.
     * Vectors are {@code [1,1,1,i]}, so proximity to the query {@code [1,1,1,FALLBACK_DOC_COUNT]}
     * decreases with {@code i}. Only the top {@link #FALLBACK_NEAR_PASS} docs (highest {@code i},
     * nearest the query) are "pass"; immediately below them sits a long contiguous run of "fail"
     * docs (~20% of the index), and everything further away is "pass" again.
     * <p>
     * Round-0 returns fewer than k matches (only {@link #FALLBACK_NEAR_PASS} "pass" docs are within
     * reach), so the retry round fires. The retry beam is far smaller than the "fail" gap, so it
     * fills entirely with rejected docs and never crosses into the distant "pass" region — the
     * post-filter stays short of k and the search falls back to the bare pre-filtered query, which
     * does cross the gap and must return exactly k "pass" hits. selectivity(pass) ≈ 0.8 &gt; 0.7.
     */
    private void indexFlatDocsDeterministic(String indexName) {
        int failCount = Math.round(FALLBACK_DOC_COUNT * 0.2f); // ~20% "fail" → selectivity ~0.8
        int gapEnd = FALLBACK_DOC_COUNT - FALLBACK_NEAR_PASS;   // docs in [gapEnd, N) are the near "pass" block
        int gapStart = gapEnd - failCount;                     // docs in [gapStart, gapEnd) are the "fail" gap
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < FALLBACK_DOC_COUNT; i++) {
            String tag = (i >= gapStart && i < gapEnd) ? "fail" : "pass";
            bulk.add(
                prepareIndex(indexName).setId(Integer.toString(i)).setSource(VECTOR_FIELD, new float[] { 1, 1, 1, i }, TAG_FIELD, tag)
            );
        }
        executeBulkAndRefresh(indexName, bulk);
    }

    private void assertPostFilterFallback(String indexName, float[] queryVector) {
        int k = 5;
        var knnSearch = new KnnSearchBuilder(VECTOR_FIELD, queryVector, k, 50, null, null, null).addFilterQuery(
            QueryBuilders.termQuery(TAG_FIELD, "pass")
        );

        assertResponse(client().prepareSearch(indexName).setKnnSearch(List.of(knnSearch)).setSize(k).setProfile(true), response -> {
            assertEquals("Expected exactly k results", k, response.getHits().getHits().length);
            for (SearchHit hit : response.getHits().getHits()) {
                assertEquals("pass", hit.getSourceAsMap().get(TAG_FIELD));
            }
            assertApproximateSearch(response, FALLBACK_DOC_COUNT);
        });
    }

}
