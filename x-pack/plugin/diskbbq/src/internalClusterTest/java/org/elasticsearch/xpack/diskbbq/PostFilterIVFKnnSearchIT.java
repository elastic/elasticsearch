/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.diskbbq;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.SliceIndexing;
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
 * {@code PostFilterHnswKnnSearchIT} for the IVF query tree (float, sliced, fallback, retry).
 * {@code bbq_disk} also accepts {@code element_type: byte} on snapshot builds, which is what
 * {@code internalClusterTest} runs on; byte coverage lives in {@code IVFPostFilterFactoryTests} at the
 * query level rather than here.
 * <p>
 * Nested (block-join) vector fields deliberately stay on the pre-filter path
 * ({@code DenseVectorFieldType#canPostFilter}), so the nested case below asserts pre-filter behaviour.
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
    // Realistic dimensionality for the exact-score assertion: at DIMS=4 quantization is effectively lossless.
    private static final int EXACT_SCORE_DIMS = 64;
    private static final int EXACT_SCORE_DOCS = 200;
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
     * Sliced (index-sorted) variant, exercising the {@link org.elasticsearch.search.vectors.IVFKnnFloatSlicedVectorQuery}
     * post-filter path that has no HNSW equivalent. Docs are routed into two slices; the search is scoped to a single
     * slice and adds a term filter, so a filtered slice-restricted post-filter query is built. Every hit must both pass
     * the filter ("common") and belong to the queried slice.
     */
    public void testIvfFloatSliced() throws IOException {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        String indexName = "ivf_float_sliced_test";
        createSlicedIvfIndex(indexName);
        indexSlicedDocs(indexName);
        assertPostFilterSliced(indexName, "s1", new float[] { 1, 1, 1, 20 });
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

    /**
     * Multi-segment variant of the flat post-filter path. Docs are indexed in two flushes without a
     * force-merge so the IVF query must stash and filter per-leaf candidates across more than one segment.
     */
    public void testIvfFloatMultiSegment() throws IOException {
        String indexName = "ivf_float_multi_segment_test";
        createIvfIndex(indexName);
        indexFlatDocsMultiSegment(indexName);
        assertPostFilterFlat(indexName, new float[] { 1, 1, 1, 20 });
    }

    /**
     * Layout that forces round-0 to come up short of {@code k} while still leaving enough far-away
     * "pass" docs for the retry round to fill the remainder — without falling back to the pre-filtered
     * inner query. Nearest neighborhood is almost all "fail" with only two "pass" docs; additional
     * "pass" docs sit far from the query so only the retry (after excluding round-0 candidates) can
     * surface them. {@code default_visit_percentage: 100} keeps the search exhaustive so the retry is
     * deterministic.
     */
    public void testIvfFloatRetryWithoutFallback() throws IOException {
        String indexName = "ivf_float_retry_test";
        createIvfIndex(indexName);
        indexFlatDocsRetrySucceeds(indexName);
        assertPostFilterRetry(indexName, new float[] { 1, 1, 1, 100 });
    }

    /**
     * Post-filtering with {@code auto_calibrate: true}. The post-filter delegate must skip exact
     * auto-rescore of the oversampled pool and still return filter-passing hits.
     */
    public void testIvfFloatAutoCalibrate() throws IOException {
        String indexName = "ivf_float_auto_calibrate_test";
        createIvfAutoCalibrateIndex(indexName);
        indexFlatDocs(indexName);
        assertPostFilterFlat(indexName, new float[] { 1, 1, 1, 20 });
    }

    /**
     * The default {@code bbq_disk} shape: {@code bits: 1}, so {@code rescore_vector} defaults to a 3x
     * oversample and an outer rescore wraps the query, and the mapping's default visit percentage, so the
     * codec's dynamic visit ratio is in play. The {@code bits: 4} indices used elsewhere in this suite leave
     * {@code rescore_vector} unset, which bypasses the oversample plumbing entirely.
     */
    public void testIvfFloatDefaultBits() throws IOException {
        String indexName = "ivf_float_default_bits_test";
        createIvfDefaultBitsIndex(indexName);
        indexFlatDocs(indexName);
        assertPostFilterFlat(indexName, new float[] { 1, 1, 1, 20 });
    }

    /**
     * With {@code auto_calibrate} the exact rescore lives inside the IVF query, and a post-filter delegate
     * skips it because its pool has not been filtered yet. Something must still apply it afterwards, or
     * post-filtered hits come back carrying approximate scores while the same query on the pre-filter path
     * returns exact ones - two score domains for one query, which corrupts the coordinator's merge.
     * <p>
     * Asserted against the arithmetic rather than by comparing two indices, because a comparison passes
     * trivially whenever both sides happen to take the same path. The expected {@code l2_norm} score of a doc
     * is {@code 1/(1 + ||doc - query||^2)} computed from the vectors this test generates, so an approximate
     * score cannot satisfy it.
     * <p>
     * {@link #EXACT_SCORE_DIMS} dimensions matter: at the 4 dimensions the rest of this suite uses, 1-bit
     * quantization error is around 1e-7 and rescoring is numerically indistinguishable from not rescoring,
     * so the assertion would hold either way and prove nothing.
     */
    public void testAutoCalibratePostFilteredScoresAreExact() throws IOException {
        String indexName = "ivf_ac_exact_scores";
        createIvfExactScoreIndex(indexName);

        // 80% "pass", spread uniformly, so round 0's pool always yields enough survivors to avoid the
        // fallback (which would rescore anyway and make the assertion vacuous).
        float[][] vectors = new float[EXACT_SCORE_DOCS][];
        for (int i = 0; i < EXACT_SCORE_DOCS; i++) {
            vectors[i] = exactScoreVector(i);
            prepareIndex(indexName).setId(Integer.toString(i))
                .setSource(VECTOR_FIELD, vectors[i], TAG_FIELD, i % 5 == 0 ? "fail" : "pass")
                .get();
        }
        refresh(indexName);

        int k = 5;
        float[] queryVector = exactScoreVector(EXACT_SCORE_DOCS / 2);
        var knnSearch = new KnnSearchBuilder(VECTOR_FIELD, queryVector, k, 100, null, null, null).addFilterQuery(
            QueryBuilders.termQuery(TAG_FIELD, "pass")
        );

        assertResponse(client().prepareSearch(indexName).setKnnSearch(List.of(knnSearch)).setSize(k), response -> {
            assertEquals("Expected exactly k results", k, response.getHits().getHits().length);
            assertScoresDescending(response);
            for (SearchHit hit : response.getHits().getHits()) {
                assertEquals("pass", hit.getSourceAsMap().get(TAG_FIELD));
                float[] docVector = vectors[Integer.parseInt(hit.getId())];
                double squaredDistance = 0;
                for (int d = 0; d < EXACT_SCORE_DIMS; d++) {
                    double delta = docVector[d] - queryVector[d];
                    squaredDistance += delta * delta;
                }
                assertEquals(
                    "doc " + hit.getId() + " must carry its exact l2_norm score, not an approximate one",
                    1.0 / (1.0 + squaredDistance),
                    hit.getScore(),
                    1e-5
                );
            }
        });
    }

    /**
     * Deterministic, well-spread vectors in {@code [-1, 1)} via a xorshift-multiply mix of the seed, so the
     * test can recompute the exact distance without storing anything, and so 1-bit quantization of a
     * {@link #EXACT_SCORE_DIMS}-dimensional vector loses real precision.
     */
    private static float[] exactScoreVector(int seed) {
        float[] vector = new float[EXACT_SCORE_DIMS];
        long h = seed * 0x9E3779B97F4A7C15L + 0x165667B19E3779F9L;
        for (int d = 0; d < EXACT_SCORE_DIMS; d++) {
            h ^= h >>> 33;
            h *= 0xFF51AFD7ED558CCDL;
            h ^= h >>> 29;
            vector[d] = ((h >>> 40) % 2000) / 1000f - 1f;
        }
        return vector;
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
        prepareCreate(indexName).setMapping(ivfMapping(false)).get();
        ensureGreen(indexName);
    }

    private void createIvfAutoCalibrateIndex(String indexName) throws IOException {
        createIvfAutoCalibrateIndex(indexName, POST_FILTER_THRESHOLD);
    }

    private void createIvfAutoCalibrateIndex(String indexName, float postFilterThreshold) throws IOException {
        Settings settings = Settings.builder()
            .put(indexSettings())
            .put(DenseVectorFieldMapper.POST_FILTER_SELECTIVITY_THRESHOLD.getKey(), postFilterThreshold)
            .put(IndexSettings.DENSE_VECTOR_EXPERIMENTAL_FEATURES_SETTING.getKey(), true)
            .build();
        prepareCreate(indexName).setSettings(settings).setMapping(ivfMapping(true)).get();
        ensureGreen(indexName);
    }

    /**
     * {@code auto_calibrate} over {@link #EXACT_SCORE_DIMS}-dimensional vectors with 1-bit quantization, so
     * the approximate scores the codec produces are materially different from the exact ones.
     */
    private void createIvfExactScoreIndex(String indexName) throws IOException {
        Settings settings = Settings.builder()
            .put(indexSettings())
            .put(DenseVectorFieldMapper.POST_FILTER_SELECTIVITY_THRESHOLD.getKey(), POST_FILTER_THRESHOLD)
            .put(IndexSettings.DENSE_VECTOR_EXPERIMENTAL_FEATURES_SETTING.getKey(), true)
            .build();
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(VECTOR_FIELD)
            .field("type", "dense_vector")
            .field("element_type", "float")
            .field("dims", EXACT_SCORE_DIMS)
            .field("index", true)
            .field("similarity", "l2_norm")
            .startObject("index_options")
            .field("type", "bbq_disk")
            .field("bits", 1)
            .field("default_visit_percentage", 100)
            .field("auto_calibrate", true)
            .endObject()
            .endObject()
            .startObject(TAG_FIELD)
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject();
        prepareCreate(indexName).setSettings(settings).setMapping(mapping).get();
        ensureGreen(indexName);
    }

    /**
     * Default {@code bits} (1), so {@code rescore_vector} defaults to 3x oversample, and the mapping's
     * default visit percentage, so the codec's dynamic visit ratio is in play. This is the configuration the
     * oversample plumbing actually ships with; the {@code bits: 4} indices above bypass it entirely.
     */
    private void createIvfDefaultBitsIndex(String indexName) throws IOException {
        prepareCreate(indexName).setMapping(ivfMapping(false, 1, null)).get();
        ensureGreen(indexName);
    }

    private static XContentBuilder ivfMapping(boolean autoCalibrate) throws IOException {
        return ivfMapping(autoCalibrate, 4, 100);
    }

    /**
     * @param bits            quantization bits. 4 leaves {@code rescore_vector} unset, so no oversample and no
     *                        outer rescore; 1 (the {@code bbq_disk} default) defaults it to 3x, which is the
     *                        shape that drives the oversample plumbing in {@code createKnnFloatQuery}.
     * @param visitPercentage {@code null} keeps the mapping default of 0, i.e. the codec's dynamic visit
     *                        ratio; 100 forces an exhaustive scan for tests that need determinism.
     */
    private static XContentBuilder ivfMapping(boolean autoCalibrate, int bits, Integer visitPercentage) throws IOException {
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
            .field("bits", bits);
        if (visitPercentage != null) {
            mapping.field("default_visit_percentage", visitPercentage);
        }
        if (autoCalibrate) {
            mapping.field("auto_calibrate", true);
        }
        return mapping.endObject().endObject().startObject(TAG_FIELD).field("type", "keyword").endObject().endObject().endObject();
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
     * Creates a sliced {@code bbq_disk} index. Slicing routes docs into index-sorted slices, and combined with a term
     * filter over a searched slice it drives the sliced IVF post-filter query. The post-filter threshold is lowered like
     * the other indices so the ~0.8-selectivity filter takes the post-filter path.
     */
    private void createSlicedIvfIndex(String indexName) throws IOException {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexSettings.SLICE_ENABLED.getKey(), true)
            .put(IndexSettings.DENSE_VECTOR_EXPERIMENTAL_FEATURES_SETTING.getKey(), true)
            .put(DenseVectorFieldMapper.POST_FILTER_SELECTIVITY_THRESHOLD.getKey(), POST_FILTER_THRESHOLD)
            .build();
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
        prepareCreate(indexName).setSettings(settings).setMapping(mapping).get();
        ensureGreen(indexName);
    }

    /**
     * Indexes 100 docs per slice ("s1", "s2") with a random 80/20 "common"/"rare" tag split and vectors
     * {@code [1, 1, 1, i]}. Doc ids are prefixed with the slice name so slice membership can be asserted.
     * Expected selectivity(common) ~ 0.8 &gt; 0.7 → post-filter path.
     */
    private void indexSlicedDocs(String indexName) {
        for (String slice : List.of("s1", "s2")) {
            for (int i = 0; i < 100; i++) {
                // Correlate the tag with the slice: "s1" is mostly "common", "s2" mostly "rare". A tag
                // distribution independent of the slice would make in-slice selectivity identical to the
                // whole-index figure, so a selectivity estimate that ignores slice scoping would look correct.
                String tag = randomFloat() < (slice.equals("s1") ? .8f : .2f) ? "common" : "rare";
                client().index(
                    new IndexRequest(indexName).id(slice + "_" + i)
                        .source(VECTOR_FIELD, new float[] { 1, 1, 1, randomIntBetween(-128, 127) }, TAG_FIELD, tag)
                        .routing(slice)
                        .setRoutingFromSlice(true)
                ).actionGet();
            }
        }
        refresh(indexName);
    }

    /**
     * Query vector is nearest to high-index docs, filter requires "common", and the search is scoped to a single slice.
     * Post-filtering only returns filter-passing docs, and slice scoping only returns docs from the queried slice, so
     * every hit must be "common" and carry the queried slice's id prefix.
     */
    private void assertPostFilterSliced(String indexName, String slice, float[] queryVector) {
        int k = 5;
        var knnSearch = new KnnSearchBuilder(VECTOR_FIELD, queryVector, k, 20, null, null, null).addFilterQuery(
            QueryBuilders.termQuery(TAG_FIELD, "common")
        );
        SearchRequestBuilder search = client().prepareSearch(indexName).setKnnSearch(List.of(knnSearch)).setSize(k);
        search.request().searchSlice(slice);

        assertResponse(search, response -> {
            assertTrue("Expected at least 1 result", response.getHits().getHits().length > 0);
            assertScoresDescending(response);
            for (SearchHit hit : response.getHits().getHits()) {
                assertEquals("common", hit.getSourceAsMap().get(TAG_FIELD));
                assertTrue("Expected hit from queried slice", hit.getId().startsWith(slice + "_"));
            }
        });
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
        // No force-merge: natural segmentation is what production looks like, and it is the only way the
        // per-leaf candidate stashing and regrouping this feature adds gets exercised at all.
        refresh(indexName);
    }

    /**
     * Indexes 100 parent docs, each with 2 nested children. Each parent's children get a random
     * 80/20 "common"/"rare" tag. Expected selectivity(common) ~ 0.8 &gt; 0.7 → post-filter path.
     */
    private void indexNestedDocs(String indexName) {
        for (int parentId = 0; parentId < 100; parentId++) {
            // Siblings are tagged independently: giving both children of a parent the same tag would make the
            // child-level filter behave like a parent-level one and hide any child/parent confusion.
            prepareIndex(indexName).setId("parent_" + parentId)
                .setSource(
                    NESTED_FIELD,
                    List.of(
                        Map.of(
                            VECTOR_FIELD,
                            new float[] { 1, 1, 1, randomIntBetween(-128, 127) },
                            TAG_FIELD,
                            randomFloat() < .8f ? "common" : "rare"
                        ),
                        Map.of(
                            VECTOR_FIELD,
                            new float[] { 1, 1, 1, randomIntBetween(-128, 127) },
                            TAG_FIELD,
                            randomFloat() < .8f ? "common" : "rare"
                        )
                    )
                )
                .get();
        }
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
            assertEquals("Expected exactly k results", k, response.getHits().getHits().length);
            assertScoresDescending(response);
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
    /**
     * Nested fields take the pre-filter path, so this asserts the filter is honoured rather than that
     * post-filtering ran. Every returned parent must have at least one "common" child, which is what the
     * child-level filter selects on; siblings are tagged independently so the filter is genuinely child-level.
     */
    @SuppressWarnings("unchecked")
    private void assertPostFilterNested(String indexName, float[] queryVector) {
        int k = 3;
        String nestedVectorField = NESTED_FIELD + "." + VECTOR_FIELD;
        String nestedTagField = NESTED_FIELD + "." + TAG_FIELD;
        var knnSearch = new KnnSearchBuilder(nestedVectorField, queryVector, k, 20, null, null, null).addFilterQuery(
            QueryBuilders.termQuery(nestedTagField, "common")
        );

        assertResponse(client().prepareSearch(indexName).setKnnSearch(List.of(knnSearch)).setSize(k), response -> {
            assertEquals("Expected exactly k parents", k, response.getHits().getHits().length);
            assertScoresDescending(response);
            for (SearchHit hit : response.getHits().getHits()) {
                assertTrue("Expected parent doc ID", hit.getId().startsWith("parent_"));
                List<Map<String, Object>> children = (List<Map<String, Object>>) hit.getSourceAsMap().get(NESTED_FIELD);
                assertNotNull("Expected nested children in _source", children);
                assertTrue(
                    "Every returned parent must have a child matching the filter, got " + children,
                    children.stream().anyMatch(child -> "common".equals(child.get(TAG_FIELD)))
                );
            }
        });
    }

    /** Scores must come back in descending order regardless of which path produced them. */
    private static void assertScoresDescending(SearchResponse response) {
        SearchHit[] hits = response.getHits().getHits();
        for (int i = 1; i < hits.length; i++) {
            assertTrue(
                "Scores must be descending, got " + hits[i - 1].getScore() + " then " + hits[i].getScore(),
                hits[i - 1].getScore() >= hits[i].getScore()
            );
        }
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
            assertScoresDescending(response);
            for (SearchHit hit : response.getHits().getHits()) {
                assertEquals("pass", hit.getSourceAsMap().get(TAG_FIELD));
            }
            // Fallback path runs the inner query after post-filter retry comes up short — its
            // vectorOps must still be reported in the profile.
            assertProfileReportsVectorOps(response);
        });
    }

    /**
     * Same 80/20 common/rare layout as {@link #indexFlatDocs}, but flushed in two batches and never
     * force-merged so the index keeps multiple segments.
     */
    private void indexFlatDocsMultiSegment(String indexName) {
        for (int i = 0; i < 100; i++) {
            String tag = randomFloat() < .8f ? "common" : "rare";
            prepareIndex(indexName).setId(Integer.toString(i))
                .setSource(VECTOR_FIELD, new float[] { 1, 1, 1, randomIntBetween(-128, 127) }, TAG_FIELD, tag)
                .get();
        }
        refresh(indexName);
        for (int i = 100; i < 200; i++) {
            String tag = randomFloat() < .8f ? "common" : "rare";
            prepareIndex(indexName).setId(Integer.toString(i))
                .setSource(VECTOR_FIELD, new float[] { 1, 1, 1, randomIntBetween(-128, 127) }, TAG_FIELD, tag)
                .get();
        }
        refresh(indexName);
    }

    /**
     * Deterministic layout for retry-without-fallback. Docs 0-79, 97, 98 are "pass" (selectivity
     * 0.82 &gt; 0.7 → post-filter); the rest are "fail". A high-index query sees only two "pass" docs
     * (97, 98) inside its round-0 neighborhood, so round-0 returns 2 &lt; k and the retry must pull the
     * remaining hits from the far "pass" docs.
     */
    private void indexFlatDocsRetrySucceeds(String indexName) {
        for (int i = 0; i < 100; i++) {
            String tag = (i <= 79 || i == 97 || i == 98) ? "pass" : "fail";
            prepareIndex(indexName).setId(Integer.toString(i)).setSource(VECTOR_FIELD, new float[] { 1, 1, 1, i }, TAG_FIELD, tag).get();
        }
        forceMerge(true);
        refresh(indexName);
    }

    private void assertPostFilterRetry(String indexName, float[] queryVector) {
        int k = 5;
        // numCands large enough that round-0's oversampled pool still sits inside the hostile
        // high-index neighborhood (only 2 pass docs), so retry must contribute the remaining hits.
        var knnSearch = new KnnSearchBuilder(VECTOR_FIELD, queryVector, k, 40, null, null, null).addFilterQuery(
            QueryBuilders.termQuery(TAG_FIELD, "pass")
        );

        assertResponse(client().prepareSearch(indexName).setKnnSearch(List.of(knnSearch)).setSize(k).setProfile(true), response -> {
            assertEquals("Expected exactly k results from round-0 + retry", k, response.getHits().getHits().length);
            assertScoresDescending(response);
            for (SearchHit hit : response.getHits().getHits()) {
                assertEquals("pass", hit.getSourceAsMap().get(TAG_FIELD));
            }
            assertProfileReportsVectorOps(response);
        });
    }

}
