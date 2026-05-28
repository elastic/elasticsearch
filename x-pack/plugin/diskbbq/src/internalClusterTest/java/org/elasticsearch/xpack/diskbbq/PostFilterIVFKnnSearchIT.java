/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.diskbbq;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
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

public class PostFilterIVFKnnSearchIT extends ESIntegTestCase {

    private static final String VECTOR_FIELD = "vector";
    private static final String TAG_FIELD = "tag";
    private static final String NESTED_FIELD = "nested_field";
    private static final int DIMS = 64;

    @Before
    public void resetLicensing() {
        enableLicensing();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateDiskBBQ.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial")
            .build();
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).build();
    }

    public void testIVFFloat() throws IOException {
        String indexName = "ivf_float_test";
        createIVFIndex(indexName);
        indexFlatDocs(indexName);
        assertPostFilterFlat(indexName);
    }

    /**
     * Deterministic tag layout: first 400 docs are "pass", last 100 are "fail".
     * Query is nearest to "fail" docs. Post-filtering cannot find k results (IVF collector
     * is too small and centroid-skip retry finds nothing), verifying fallback to the original query.
     */
    public void testIVFFloatFallback() throws IOException {
        String indexName = "ivf_float_fallback_test";
        createIVFIndex(indexName);
        indexFlatDocsDeterministic(indexName);
        assertPostFilterFallback(indexName);
    }

    /**
     * Index sorted by price ascending, vectors correlate with price.
     * Query nearest to expensive docs, range filter requires price &lt; 400.
     * Simulates a production scenario where index sorting causes kNN proximity to align with filter boundaries.
     */
    public void testIVFFloatSortedIndex() throws IOException {
        String indexName = "ivf_float_sorted_test";
        createSortedIVFIndex(indexName);
        indexSortedDocs(indexName);
        assertPostFilterSorted(indexName);
    }

    public void testIVFFloatNested() throws IOException {
        String indexName = "ivf_float_nested_test";
        createIVFNestedIndex(indexName);
        indexNestedDocs(indexName);
        assertPostFilterNested(indexName);
    }

    public void testPostFilterReportsVectorOpsInProfile() throws IOException {
        String indexName = "ivf_profile_test";
        createIVFIndex(indexName);
        indexFlatDocs(indexName);

        int k = 5;
        var knnSearch = new KnnSearchBuilder(VECTOR_FIELD, makeVector(500), k, 500, 100f, null, null).addFilterQuery(
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

    private void createIVFIndex(String indexName) throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(VECTOR_FIELD)
            .field("type", "dense_vector")
            .field("dims", DIMS)
            .field("index", true)
            .field("similarity", "l2_norm")
            .startObject("index_options")
            .field("type", "bbq_disk")
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

    private void createIVFNestedIndex(String indexName) throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(NESTED_FIELD)
            .field("type", "nested")
            .startObject("properties")
            .startObject(VECTOR_FIELD)
            .field("type", "dense_vector")
            .field("dims", DIMS)
            .field("index", true)
            .field("similarity", "l2_norm")
            .startObject("index_options")
            .field("type", "bbq_disk")
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
     * Creates a deterministic 64-dim vector that spreads variation across all dimensions.
     * Vectors with closer seeds are closer in L2 distance.
     */
    private static float[] makeVector(int seed) {
        float[] vec = new float[DIMS];
        for (int j = 0; j < DIMS; j++) {
            vec[j] = seed * 0.1f + j;
        }
        return vec;
    }

    /**
     * Indexes 500 flat docs with random 80/20 "common"/"rare" tag split.
     * Expected selectivity(common) ~ 0.8 > 0.7 → post-filter.
     */
    private void indexFlatDocs(String indexName) {
        for (int i = 0; i < 500; i++) {
            String tag = randomFloat() < .8f ? "common" : "rare";
            prepareIndex(indexName).setId(Integer.toString(i)).setSource(VECTOR_FIELD, makeVector(i), TAG_FIELD, tag).get();
        }
        forceMerge(true);
        refresh(indexName);
    }

    /**
     * Indexes 250 parent docs, each with 2 nested children.
     * Parents 0-199: children tagged "common". Parents 200-249: children tagged "rare".
     * Total: 400 common children, 100 rare children → selectivity(common) = 0.8.
     */
    private void indexNestedDocs(String indexName) {
        for (int parentId = 0; parentId < 250; parentId++) {
            String tag = parentId < 200 ? "common" : "rare";
            int childIdx0 = parentId * 2;
            int childIdx1 = parentId * 2 + 1;
            prepareIndex(indexName).setId("parent_" + parentId)
                .setSource(
                    NESTED_FIELD,
                    List.of(
                        Map.of(VECTOR_FIELD, makeVector(childIdx0), TAG_FIELD, tag),
                        Map.of(VECTOR_FIELD, makeVector(childIdx1), TAG_FIELD, tag)
                    )
                )
                .get();
        }
        forceMerge(true);
        refresh(indexName);
    }

    private void assertPostFilterFlat(String indexName) {
        int k = 5;
        // Query vector nearest to "rare" docs (seed >= 400)
        var knnSearch = new KnnSearchBuilder(VECTOR_FIELD, makeVector(500), k, 500, 100f, null, null).addFilterQuery(
            QueryBuilders.termQuery(TAG_FIELD, "common")
        );

        assertResponse(client().prepareSearch(indexName).setKnnSearch(List.of(knnSearch)).setSize(k), response -> {
            assertTrue("Expected at least 1 result", response.getHits().getHits().length > 0);
            for (SearchHit hit : response.getHits().getHits()) {
                assertEquals("common", hit.getSourceAsMap().get(TAG_FIELD));
            }
        });
    }

    private void assertPostFilterNested(String indexName) {
        int k = 3;
        String nestedVectorField = NESTED_FIELD + "." + VECTOR_FIELD;
        String nestedTagField = NESTED_FIELD + "." + TAG_FIELD;
        var knnSearch = new KnnSearchBuilder(nestedVectorField, makeVector(500), k, 500, 100f, null, null).addFilterQuery(
            QueryBuilders.termQuery(nestedTagField, "common")
        );

        assertResponse(client().prepareSearch(indexName).setKnnSearch(List.of(knnSearch)).setSize(k), response -> {
            assertTrue("Expected at least 1 result", response.getHits().getHits().length > 0);
            for (SearchHit hit : response.getHits().getHits()) {
                assertTrue("Expected parent doc ID", hit.getId().startsWith("parent_"));
                int parentId = Integer.parseInt(hit.getId().substring("parent_".length()));
                assertTrue("Expected common parent (id < 200), got " + parentId, parentId < 200);
            }
        });
    }

    /**
     * Indexes 500 flat docs with deterministic tags: 0-399 = "pass", 400-499 = "fail".
     * selectivity(pass) = 0.8 > 0.7 → post-filter. The query neighborhood (high-seed docs) is all "fail".
     */
    private void indexFlatDocsDeterministic(String indexName) {
        for (int i = 0; i < 500; i++) {
            String tag = i < 400 ? "pass" : "fail";
            prepareIndex(indexName).setId(Integer.toString(i)).setSource(VECTOR_FIELD, makeVector(i), TAG_FIELD, tag).get();
        }
        forceMerge(true);
        refresh(indexName);
    }

    private void assertPostFilterFallback(String indexName) {
        int k = 5;
        var knnSearch = new KnnSearchBuilder(VECTOR_FIELD, makeVector(500), k, 500, 100f, null, null).addFilterQuery(
            QueryBuilders.termQuery(TAG_FIELD, "pass")
        );

        assertResponse(client().prepareSearch(indexName).setKnnSearch(List.of(knnSearch)).setSize(k).setProfile(true), response -> {
            assertEquals("Expected exactly k results from fallback", k, response.getHits().getHits().length);
            for (SearchHit hit : response.getHits().getHits()) {
                assertEquals("pass", hit.getSourceAsMap().get(TAG_FIELD));
            }
            // Fallback path runs the inner query after post-filter retry comes up short — its
            // vectorOps must still be reported in the profile.
            assertProfileReportsVectorOps(response);
        });
    }

    private void createSortedIVFIndex(String indexName) throws IOException {
        Settings sortedSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put("index.sort.field", "price")
            .put("index.sort.order", "asc")
            .build();
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(VECTOR_FIELD)
            .field("type", "dense_vector")
            .field("dims", DIMS)
            .field("index", true)
            .field("similarity", "l2_norm")
            .startObject("index_options")
            .field("type", "bbq_disk")
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
     * Indexes 500 docs with price=i and vector=makeVector(i). On a sorted index, segment doc IDs
     * follow price order, causing kNN proximity to align with the price-based filter boundary.
     */
    private void indexSortedDocs(String indexName) {
        for (int i = 0; i < 500; i++) {
            prepareIndex(indexName).setId(Integer.toString(i)).setSource(VECTOR_FIELD, makeVector(i), "price", i).get();
        }
        forceMerge(true);
        refresh(indexName);
    }

    /**
     * Query vector nearest to expensive docs (price >= 400), range filter requires price &lt; 400.
     * selectivity = 400/500 = 0.8 → post-filter. Nearest results are all outside the filter range.
     */
    private void assertPostFilterSorted(String indexName) {
        int k = 5;
        var knnSearch = new KnnSearchBuilder(VECTOR_FIELD, makeVector(500), k, 500, 100f, null, null).addFilterQuery(
            QueryBuilders.rangeQuery("price").lt(400)
        );

        assertResponse(client().prepareSearch(indexName).setKnnSearch(List.of(knnSearch)).setSize(k), response -> {
            assertEquals("Expected exactly k results", k, response.getHits().getHits().length);
            for (SearchHit hit : response.getHits().getHits()) {
                int price = (int) hit.getSourceAsMap().get("price");
                assertTrue("Expected price < 400, got " + price, price < 400);
            }
        });
    }
}
