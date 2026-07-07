/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.query;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.VectorIndexType;
import org.elasticsearch.index.mapper.vectors.DenseVectorScriptDocValues;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.ScriptScoreQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.vectors.DenseVectorQueryBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests for the {@code dense_vector} query against every supported {@link VectorIndexType}.
 * Each test run picks a random codec; randomized test seeds cover all types over time.
 *
 * <p>Both modes are validated against a script-score baseline that computes
 * {@link VectorSimilarityFunction#compare(float[], float[])} over the per-doc vector values:
 * <ul>
 *     <li><b>Raw path</b> ({@code quantized=false}, default): per-doc scores must equal the
 *     script-score ground truth within float-arithmetic tolerance on every codec.</li>
 *     <li><b>Codec path</b> ({@code quantized=true}): on non-quantized codecs, scores must match the
 *     script-score ground truth (no codec quantization to fall back to). On quantized codecs, scores
 *     stay within the codec's quantization budget of the ground truth.</li>
 * </ul>
 */
public class DenseVectorQueryIT extends ESIntegTestCase {

    public static final String INDEX_NAME = "dense_vector_query_it";
    public static final String VECTOR_FIELD = "vector";
    public static final String VECTOR_SCORE_SCRIPT = "vector_scoring";
    public static final String VECTOR_SCORE_SCRIPT_COSINE = "vector_scoring_cosine";
    public static final String QUERY_VECTOR_PARAM = "query_vector";

    /** Tolerance for "raw path matches the script-score ground truth" — float arithmetic noise only. */
    private static final float DELTA = 1e-6f;

    private VectorIndexType selectedIndexType;
    private ElementType selectedElementType;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(CustomScriptPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {
        private static final VectorSimilarityFunction SIMILARITY_FUNCTION = DenseVectorFieldMapper.VectorSimilarity.L2_NORM
            .vectorSimilarityFunction(IndexVersion.current(), ElementType.FLOAT);
        // The literal cosine function (normalizes both operands itself); the ground truth for a per-query
        // similarity_function=cosine override against the raw stored vectors of the l2_norm field.
        private static final VectorSimilarityFunction COSINE_FUNCTION = VectorSimilarityFunction.COSINE;

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Map.of(
                VECTOR_SCORE_SCRIPT,
                vars -> compare(SIMILARITY_FUNCTION, vars),
                VECTOR_SCORE_SCRIPT_COSINE,
                vars -> compare(COSINE_FUNCTION, vars)
            );
        }

        private static float compare(VectorSimilarityFunction function, Map<String, Object> vars) {
            Map<?, ?> doc = (Map<?, ?>) vars.get("doc");
            return function.compare(
                ((DenseVectorScriptDocValues) doc.get(VECTOR_FIELD)).getVectorValue(),
                (float[]) vars.get(QUERY_VECTOR_PARAM)
            );
        }
    }

    @Before
    public void setup() throws IOException {
        // Randomize over the float-family element types and any VectorIndexType that supports the chosen
        // type. The randomized test seed cycles element types and index types over runs. bfloat16 stores
        // reduced-precision vectors, but the dense_vector query and the script-score baseline both read the
        // same stored representation, so the raw path still matches the ground truth within tolerance.
        selectedElementType = randomFrom(ElementType.FLOAT, ElementType.BFLOAT16);
        selectedIndexType = randomFrom(
            Arrays.stream(VectorIndexType.values()).filter(t -> t.supportsElementType(selectedElementType)).toList()
        );
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(VECTOR_FIELD)
            .field("type", "dense_vector")
            .field("element_type", selectedElementType.toString())
            .field("similarity", "l2_norm")
            .startObject("index_options")
            .field("type", selectedIndexType.name().toLowerCase(Locale.ROOT))
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 5))
            .build();
        prepareCreate(INDEX_NAME).setMapping(mapping).setSettings(settings).get();
        ensureGreen(INDEX_NAME);
    }

    private record TestParams(int numDocs, int numDims, float[] queryVector) {
        public static TestParams generate() {
            int numDims = randomIntBetween(32, 256) * 2; // even, ≥64 for BBQ codecs
            int numDocs = randomIntBetween(10, 50);
            return new TestParams(numDocs, numDims, randomVector(numDims));
        }
    }

    public void testDenseVectorRawMatchesGroundTruth() {
        TestParams params = indexRandomDocs();
        DenseVectorQueryBuilder query = new DenseVectorQueryBuilder(VECTOR_FIELD, params.queryVector(), null, false);
        runAndCompare(INDEX_NAME, query, params, VECTOR_SCORE_SCRIPT, DELTA);
    }

    public void testDenseVectorQuantizedScoring() {
        TestParams params = indexRandomDocs();
        DenseVectorQueryBuilder query = new DenseVectorQueryBuilder(VECTOR_FIELD, params.queryVector(), null, true);
        // On non-quantized codecs there is no quantized representation to fall back to; the codec-bound
        // scorer reads the same raw values, so scores must match ground truth exactly. On quantized
        // codecs the codec scorer uses the quantized representation, so scores stay within a per-codec
        // budget of the ground truth.
        double tolerance = selectedIndexType.isQuantized() ? quantizedTolerance(selectedIndexType) : DELTA;
        runAndCompare(INDEX_NAME, query, params, VECTOR_SCORE_SCRIPT, tolerance);
    }

    /**
     * A per-query {@code similarity_function} override that differs from the field's mapped similarity
     * ({@code l2_norm}) must score the raw stored vectors with the literal chosen metric. The override path
     * uses the literal {@code COSINE} function — which normalizes both operands itself — rather than the
     * {@code NORMALIZE_COSINE} storage optimization (which maps {@code COSINE} to {@code DOT_PRODUCT} and
     * assumes unit-normalized vectors), so a cosine override yields true cosine regardless of how the field
     * was stored. The raw path always reads raw vectors, so scores must match the cosine ground truth within
     * float tolerance on every codec, quantized or not.
     */
    public void testDenseVectorSimilarityOverrideMatchesGroundTruth() {
        TestParams params = indexRandomDocs();
        DenseVectorQueryBuilder query = new DenseVectorQueryBuilder(
            VECTOR_FIELD,
            params.queryVector(),
            DenseVectorFieldMapper.VectorSimilarity.COSINE,
            false
        );
        runAndCompare(INDEX_NAME, query, params, VECTOR_SCORE_SCRIPT_COSINE, DELTA);
    }

    /**
     * A non-indexed (index:false) field stores its vectors as binary doc values rather than KNN vector
     * values, so it is scored through a separate doc-values path. It has no configured similarity, so the
     * query supplies one via similarity_function. The script-score baseline reads the same binary doc values,
     * so the raw scores must match the ground truth within float tolerance for both float and bfloat16.
     */
    public void testDenseVectorOnNonIndexedField() throws IOException {
        String index = "dense_vector_query_it_unindexed";
        ElementType elementType = randomFrom(ElementType.FLOAT, ElementType.BFLOAT16);
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(VECTOR_FIELD)
            .field("type", "dense_vector")
            .field("element_type", elementType.toString())
            .field("index", false)
            .endObject()
            .endObject()
            .endObject();
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 5))
            .build();
        prepareCreate(index).setMapping(mapping).setSettings(settings).get();
        ensureGreen(index);

        TestParams params = TestParams.generate();
        IndexRequestBuilder[] docs = new IndexRequestBuilder[params.numDocs()];
        for (int i = 0; i < params.numDocs(); i++) {
            docs[i] = prepareIndex(index).setId(String.valueOf(i)).setSource(VECTOR_FIELD, randomVector(params.numDims()));
        }
        indexRandom(true, docs);

        // index:false has no configured similarity, so a similarity_function is required; use l2_norm to match
        // the script-score baseline.
        DenseVectorQueryBuilder query = new DenseVectorQueryBuilder(
            VECTOR_FIELD,
            params.queryVector(),
            DenseVectorFieldMapper.VectorSimilarity.L2_NORM,
            false
        );
        runAndCompare(index, query, params, VECTOR_SCORE_SCRIPT, DELTA);
    }

    /**
     * An l2_norm override on a cosine-normalized field must score against the original vectors, not
     * the unit-normalized ones stored in the KNN index. Script doc-values read originals via
     * {@code DenormalizedCosineFloatVectorValues}, so they are the correct ground truth.
     */
    public void testSimilarityOverrideOnCosineNormalizedField() throws IOException {
        String index = "dense_vector_query_it_cosine_override";
        int numDims = randomIntBetween(32, 128) * 2;
        int numDocs = randomIntBetween(10, 50);

        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(VECTOR_FIELD)
            .field("type", "dense_vector")
            .field("element_type", "float")
            .field("similarity", "cosine")
            .startObject("index_options")
            .field("type", selectedIndexType.name().toLowerCase(Locale.ROOT))
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 5))
            .build();
        prepareCreate(index).setMapping(mapping).setSettings(settings).get();
        ensureGreen(index);

        // Scale vectors above unit length so euclidean on unit-normalized != euclidean on originals.
        float[] queryVector = randomVector(numDims);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            float scale = 2.0f + randomFloat() * 8.0f;
            float[] v = randomVector(numDims);
            for (int d = 0; d < numDims; d++) {
                v[d] *= scale;
            }
            docs[i] = prepareIndex(index).setId(String.valueOf(i)).setSource(VECTOR_FIELD, v);
        }
        indexRandom(true, docs);

        DenseVectorQueryBuilder query = new DenseVectorQueryBuilder(
            VECTOR_FIELD,
            queryVector,
            DenseVectorFieldMapper.VectorSimilarity.L2_NORM,
            false
        );
        runAndCompare(index, query, new TestParams(numDocs, numDims, queryVector), VECTOR_SCORE_SCRIPT, DELTA);
    }

    /**
     * A non-indexed (index:false) float field stores the per-doc magnitude in its binary doc values.
     * COSINE scoring uses that stored magnitude to avoid recomputing it per doc. Scores must match
     * the COSINE script-score ground truth within float tolerance.
     */
    public void testCosineOnNonIndexedField() throws IOException {
        String index = "dense_vector_query_it_unindexed_cosine";
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(VECTOR_FIELD)
            .field("type", "dense_vector")
            .field("element_type", "float")
            .field("index", false)
            .endObject()
            .endObject()
            .endObject();
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 5))
            .build();
        prepareCreate(index).setMapping(mapping).setSettings(settings).get();
        ensureGreen(index);

        TestParams params = TestParams.generate();
        IndexRequestBuilder[] docs = new IndexRequestBuilder[params.numDocs()];
        for (int i = 0; i < params.numDocs(); i++) {
            docs[i] = prepareIndex(index).setId(String.valueOf(i)).setSource(VECTOR_FIELD, randomVector(params.numDims()));
        }
        indexRandom(true, docs);

        DenseVectorQueryBuilder query = new DenseVectorQueryBuilder(
            VECTOR_FIELD,
            params.queryVector(),
            DenseVectorFieldMapper.VectorSimilarity.COSINE,
            false
        );
        runAndCompare(index, query, params, VECTOR_SCORE_SCRIPT_COSINE, DELTA);
    }

    /** Loose per-codec budget for codec-path vs raw script-score distance. */
    private static double quantizedTolerance(VectorIndexType type) {
        return switch (type) {
            case INT8_HNSW, INT8_FLAT -> 0.5;
            case INT4_HNSW, INT4_FLAT -> 1.5;
            case BBQ_HNSW, BBQ_FLAT, BBQ_DISK -> 5.0;
            // Non-quantized types should never reach this branch; treat as a tight bound if they do.
            case HNSW, FLAT -> DELTA;
        };
    }

    private TestParams indexRandomDocs() {
        TestParams params = TestParams.generate();
        IndexRequestBuilder[] docs = new IndexRequestBuilder[params.numDocs()];
        for (int i = 0; i < params.numDocs(); i++) {
            docs[i] = prepareIndex(INDEX_NAME).setId(String.valueOf(i)).setSource(VECTOR_FIELD, randomVector(params.numDims()));
        }
        indexRandom(true, docs);
        return params;
    }

    /**
     * Run the {@code dense_vector} query, then compare each hit's score with a script-score baseline.
     *
     * @param tolerance per-doc score delta tolerance.
     */
    private void runAndCompare(String index, DenseVectorQueryBuilder query, TestParams params, String baselineScript, double tolerance) {
        SearchRequestBuilder search = prepareSearch(index).setQuery(query).setSize(params.numDocs()).setTrackTotalHits(randomBoolean());
        assertNoFailuresAndResponse(
            search,
            denseResponse -> compareWithExactSearch(index, denseResponse, params.queryVector(), params.numDocs(), baselineScript, tolerance)
        );
    }

    private static void compareWithExactSearch(
        String index,
        SearchResponse denseResponse,
        float[] queryVector,
        int docCount,
        String baselineScript,
        double tolerance
    ) {
        // Guard against a vacuous pass: the per-hit comparison below loops over the dense hits, so it would
        // silently succeed if the dense query matched nothing. Every doc has the field, so all are scored.
        assertThat("dense_vector query returned fewer hits than indexed docs", denseResponse.getHits().getHits().length, equalTo(docCount));
        Script script = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, baselineScript, Map.of(QUERY_VECTOR_PARAM, queryVector));
        ScriptScoreQueryBuilder scriptScoreQueryBuilder = QueryBuilders.scriptScoreQuery(new MatchAllQueryBuilder(), script);
        assertNoFailuresAndResponse(prepareSearch(index).setQuery(scriptScoreQueryBuilder).setSize(docCount), exactResponse -> {
            assertHitCount(exactResponse, docCount);

            int i = 0;
            SearchHit[] exactHits = exactResponse.getHits().getHits();
            for (SearchHit denseHit : denseResponse.getHits().getHits()) {
                while (i < exactHits.length && exactHits[i].getId().equals(denseHit.getId()) == false) {
                    i++;
                }
                if (i >= exactHits.length) {
                    fail("dense_vector hit not found in exact search");
                }
                assertThat(
                    "dense_vector score for doc " + denseHit.getId() + " is not within tolerance of script-score ground truth",
                    (double) denseHit.getScore(),
                    closeTo(exactHits[i].getScore(), tolerance)
                );
            }
        });
    }

    private static float[] randomVector(int numDimensions) {
        float[] vector = new float[numDimensions];
        for (int j = 0; j < numDimensions; j++) {
            vector[j] = randomFloatBetween(0, 1, true);
        }
        return vector;
    }
}
