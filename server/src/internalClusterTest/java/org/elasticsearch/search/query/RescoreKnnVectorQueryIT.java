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
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.retriever.KnnRetrieverBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.search.vectors.RescoreVectorBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.hamcrest.Matchers.equalTo;

public class RescoreKnnVectorQueryIT extends ESIntegTestCase {

    public static final String INDEX_NAME = "test";
    public static final String VECTOR_FIELD = "vector";
    public static final String VECTOR_SCORE_SCRIPT = "vector_scoring";
    public static final String QUERY_VECTOR_PARAM = "query_vector";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(CustomScriptPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {
        private static final VectorSimilarityFunction SIMILARITY_FUNCTION = DenseVectorFieldMapper.VectorSimilarity.L2_NORM
            .vectorSimilarityFunction(IndexVersion.current(), DenseVectorFieldMapper.ElementType.FLOAT);

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Map.of(VECTOR_SCORE_SCRIPT, vars -> {
                Map<?, ?> doc = (Map<?, ?>) vars.get("doc");
                return SIMILARITY_FUNCTION.compare(
                    ((DenseVectorScriptDocValues) doc.get(VECTOR_FIELD)).getVectorValue(),
                    (float[]) vars.get(QUERY_VECTOR_PARAM)
                );
            });
        }
    }

    @Before
    public void setup() throws IOException {
        String type = randomFrom(
            Arrays.stream(VectorIndexType.values())
                .filter(VectorIndexType::isQuantized)
                .map(t -> t.name().toLowerCase(Locale.ROOT))
                .collect(Collectors.toCollection(ArrayList::new))
        );
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(VECTOR_FIELD)
            .field("type", "dense_vector")
            .field("similarity", "l2_norm")
            .startObject("index_options")
            .field("type", type)
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

    private record TestParams(
        int numDocs,
        int numDims,
        float[] queryVector,
        int k,
        int numCands,
        RescoreVectorBuilder rescoreVectorBuilder
    ) {
        public static TestParams generate() {
            int numDims = randomIntBetween(32, 512) * 2; // Ensure even dimensions
            int numDocs = randomIntBetween(10, 100);
            int k = randomIntBetween(1, numDocs - 5);
            return new TestParams(
                numDocs,
                numDims,
                randomVector(numDims),
                k,
                (int) (k * randomFloatBetween(1.0f, 10.0f, true)),
                new RescoreVectorBuilder(randomFloatBetween(1.0f, 100f, true))
            );
        }
    }

    public void testKnnSearchRescore() {
        BiFunction<TestParams, SearchRequestBuilder, SearchRequestBuilder> knnSearchGenerator = (testParams, requestBuilder) -> {
            KnnSearchBuilder knnSearch = new KnnSearchBuilder(
                VECTOR_FIELD,
                testParams.queryVector,
                testParams.k,
                testParams.numCands,
                testParams.rescoreVectorBuilder,
                null
            );
            return requestBuilder.setKnnSearch(List.of(knnSearch));
        };
        testKnnRescore(knnSearchGenerator);
    }

    public void testKnnQueryRescore() {
        BiFunction<TestParams, SearchRequestBuilder, SearchRequestBuilder> knnQueryGenerator = (testParams, requestBuilder) -> {
            KnnVectorQueryBuilder knnQuery = new KnnVectorQueryBuilder(
                VECTOR_FIELD,
                testParams.queryVector,
                testParams.k,
                testParams.numCands,
                testParams.rescoreVectorBuilder,
                null
            );
            return requestBuilder.setQuery(knnQuery);
        };
        testKnnRescore(knnQueryGenerator);
    }

    public void testKnnRetriever() {
        BiFunction<TestParams, SearchRequestBuilder, SearchRequestBuilder> knnQueryGenerator = (testParams, requestBuilder) -> {
            KnnRetrieverBuilder knnRetriever = new KnnRetrieverBuilder(
                VECTOR_FIELD,
                testParams.queryVector,
                null,
                testParams.k,
                testParams.numCands,
                testParams.rescoreVectorBuilder,
                null
            );
            return requestBuilder.setSource(new SearchSourceBuilder().retriever(knnRetriever));
        };
        testKnnRescore(knnQueryGenerator);
    }

    private void testKnnRescore(BiFunction<TestParams, SearchRequestBuilder, SearchRequestBuilder> searchRequestGenerator) {
        TestParams testParams = TestParams.generate();

        int numDocs = testParams.numDocs;
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];

        for (int i = 0; i < numDocs; i++) {
            docs[i] = prepareIndex(INDEX_NAME).setId("" + i).setSource(VECTOR_FIELD, randomVector(testParams.numDims));
        }
        indexRandom(true, docs);

        float[] queryVector = testParams.queryVector;
        float oversample = randomFloatBetween(1.0f, 100f, true);
        RescoreVectorBuilder rescoreVectorBuilder = new RescoreVectorBuilder(oversample);

        SearchRequestBuilder requestBuilder = searchRequestGenerator.apply(
            testParams,
            prepareSearch(INDEX_NAME).setSize(numDocs).setTrackTotalHits(randomBoolean())
        );

        assertNoFailuresAndResponse(requestBuilder, knnResponse -> { compareWithExactSearch(knnResponse, queryVector, numDocs); });
    }

    private static void compareWithExactSearch(SearchResponse knnResponse, float[] queryVector, int docCount) {
        // Do an exact query and compare
        Script script = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            VECTOR_SCORE_SCRIPT,
            Map.of(QUERY_VECTOR_PARAM, queryVector)
        );
        ScriptScoreQueryBuilder scriptScoreQueryBuilder = QueryBuilders.scriptScoreQuery(new MatchAllQueryBuilder(), script);
        assertNoFailuresAndResponse(prepareSearch(INDEX_NAME).setQuery(scriptScoreQueryBuilder).setSize(docCount), exactResponse -> {
            assertHitCount(exactResponse, docCount);

            int i = 0;
            SearchHit[] exactHits = exactResponse.getHits().getHits();
            for (SearchHit knnHit : knnResponse.getHits().getHits()) {
                while (i < exactHits.length && exactHits[i].getId().equals(knnHit.getId()) == false) {
                    i++;
                }
                if (i >= exactHits.length) {
                    fail("Knn doc not found in exact search");
                }
                assertThat("Real score is not the same as rescored score", knnHit.getScore(), equalTo(exactHits[i].getScore()));
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
