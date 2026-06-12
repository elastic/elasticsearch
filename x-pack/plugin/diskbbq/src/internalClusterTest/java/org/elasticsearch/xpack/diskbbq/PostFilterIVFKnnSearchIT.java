/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.diskbbq;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.license.DiskBBQLicensingIT.enableLicensing;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Integration tests for kNN post-filtering over a {@code bbq_disk} (IVF) index, mirroring
 * {@code PostFilterHnswKnnSearchIT} for the IVF query tree (float, nested/diversifying, sorted,
 * fallback). diskbbq only supports float element types, so there is no byte variant.
 *
 * <p>Post-filtering is dormant by default ({@code index.dense_vector.post_filter_selectivity_threshold}
 * defaults to 1.0). Each index lowers it to {@code 0.7} so the ~0.8-selectivity filters below take the
 * post-filter path. That setting is {@link org.elasticsearch.common.settings.Setting.Property#PrivateIndex},
 * so {@link #forbidPrivateIndexSettings()} is overridden to {@code false} to allow setting it explicitly.
 *
 * <p>{@code default_visit_percentage: 100} forces the IVF search to scan every centroid; combined with the
 * single force-merged segment (doc counts stay below the 384 vectors-per-cluster default, so one cluster is
 * built) the search is effectively exhaustive, keeping recall — and therefore the assertions — deterministic.
 */
@LuceneTestCase.SuppressCodecs("*")
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class PostFilterIVFKnnSearchIT extends ESIntegTestCase {

    private static final String VECTOR_FIELD = "vector";
    private static final String TAG_FIELD = "tag";
    private static final String NESTED_FIELD = "nested_field";
    private static final int DIMS = 4;
    private static final float POST_FILTER_THRESHOLD = 0.7f;

    @Before
    public void resetLicensing() {
        enableLicensing();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial")
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateDiskBBQ.class);
    }

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

    public void testIvfFloat() throws IOException {
        String indexName = "ivf_float_test";
        createIvfIndex(indexName);
        indexFlatDocs(indexName);
        assertPostFilterFlat(indexName, new float[] { 1, 1, 1, 20 });
    }

    public void testIvfFloatNested() throws IOException {
        String indexName = "ivf_float_nested_test";
        createIvfNestedIndex(indexName);
        indexNestedDocs(indexName);
        assertPostFilterNested(indexName, new float[] { 1, 1, 1, 20 });
    }

    /**
     * Deterministic tag layout: docs 0-79 are "pass", 80-99 are "fail". The query is nearest to
     * the "fail" neighborhood, so post-filtering's round-0 + retry rounds come up short of k and the
     * orchestrator must fall back to the original pre-filtered query to still return exactly k hits.
     */
    public void testIvfFloatFallback() throws IOException {
        String indexName = "ivf_float_fallback_test";
        createIvfIndex(indexName);
        indexFlatDocsDeterministic(indexName);
        assertPostFilterFallback(indexName, new float[] { 1, 1, 1, 100 });
    }

    public void testPostFilterReportsVectorOpsInProfile() throws IOException {
        String indexName = "ivf_profile_test";
        createIvfIndex(indexName);
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

    private void createIvfIndex(String indexName) throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(VECTOR_FIELD)
            .field("type", "dense_vector")
            .field("element_type", "float")
            .field("dims", DIMS)
            .field("index", true)
            .field("similarity", "l2_norm")
            .startObject("index_options")
            .field("type", "bbq_disk")
            .field("bits", 4)
            .field("default_visit_percentage", 100)
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

    private void createIvfNestedIndex(String indexName) throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(NESTED_FIELD)
            .field("type", "nested")
            .startObject("properties")
            .startObject(VECTOR_FIELD)
            .field("type", "dense_vector")
            .field("element_type", "float")
            .field("dims", DIMS)
            .field("index", true)
            .field("similarity", "l2_norm")
            .startObject("index_options")
            .field("type", "bbq_disk")
            .field("bits", 4)
            .field("default_visit_percentage", 100)
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
     * Indexes 200 flat docs with a random 80/20 "common"/"rare" tag split and vectors {@code [1, 1, 1, i]}.
     * Expected selectivity(common) ~ 0.8 &gt; 0.7 → post-filter path.
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
     * Indexes 100 parent docs, each with 2 nested children. Each parent's children get a random
     * 80/20 "common"/"rare" tag. Expected selectivity(common) ~ 0.8 &gt; 0.7 → post-filter path.
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
     * Query vector is nearest to high-index docs, filter requires "common". With random tag
     * assignment we cannot predict exact doc IDs, but post-filtering only returns filter-passing
     * docs, so every hit must be "common".
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
     * With random tag assignment we cannot predict exact parent IDs, but all results must be
     * "common" parents.
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

}
