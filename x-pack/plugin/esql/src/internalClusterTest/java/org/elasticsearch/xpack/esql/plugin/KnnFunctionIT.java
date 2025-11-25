/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.internal.IndicesAdminClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.parser.ParserUtils;
import org.elasticsearch.xpack.esql.parser.QueryParam;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.index.IndexMode.LOOKUP;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.DenseVectorFieldTypeIT.ALL_DENSE_VECTOR_INDEX_TYPES;
import static org.elasticsearch.xpack.esql.DenseVectorFieldTypeIT.NON_QUANTIZED_DENSE_VECTOR_INDEX_TYPES;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class KnnFunctionIT extends AbstractEsqlIntegTestCase {

    private final Map<Integer, List<Number>> indexedVectors = new HashMap<>();
    private int numDocs;
    private int numDims;

    private final DenseVectorFieldMapper.ElementType elementType;
    private final String indexType;

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        List<Object[]> params = new ArrayList<>();
        for (String indexType : ALL_DENSE_VECTOR_INDEX_TYPES) {
            params.add(new Object[] { DenseVectorFieldMapper.ElementType.FLOAT, indexType });
        }
        for (String indexType : NON_QUANTIZED_DENSE_VECTOR_INDEX_TYPES) {
            params.add(new Object[] { DenseVectorFieldMapper.ElementType.BYTE, indexType });
            params.add(new Object[] { DenseVectorFieldMapper.ElementType.BIT, indexType });
        }

        // Remove flat index types, as knn does not do a top k for flat
        params.removeIf(param -> param[1] != null && ((String) param[1]).contains("flat"));
        return params;
    }

    public KnnFunctionIT(@Name("elementType") DenseVectorFieldMapper.ElementType elementType, @Name("indexType") String indexType) {
        this.elementType = elementType;
        this.indexType = indexType;
    }

    public void testKnnDefaults() {
        float[] queryVector = new float[numDims];
        Arrays.fill(queryVector, 0.0f);

        var query = String.format(Locale.ROOT, """
            FROM test METADATA _score
            | WHERE knn(vector, %s)
            | KEEP id, _score, vector
            | SORT _score DESC
            | LIMIT 10
            """, Arrays.toString(queryVector));

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score", "vector"));
            assertColumnTypes(resp.columns(), List.of("integer", "double", "dense_vector"));

            List<List<Object>> valuesList = EsqlTestUtils.getValuesList(resp);
            assertEquals(Math.min(indexedVectors.size(), 10), valuesList.size());
            double previousScore = Float.MAX_VALUE;
            for (List<Object> row : valuesList) {
                // Vectors should be in score order
                double currentScore = (Double) row.get(1);
                assertThat(currentScore, lessThanOrEqualTo(previousScore));
                previousScore = currentScore;
                @SuppressWarnings("unchecked")
                // Vectors should be the same
                List<Number> actualVector = (List<Number>) row.get(2);
                List<Number> expectedVector = indexedVectors.get(row.get(0));
                for (int j = 0; j < actualVector.size(); j++) {
                    float expected = expectedVector.get(j).floatValue();
                    float actual = actualVector.get(j).floatValue();
                    assertEquals(expected, actual, 0f);
                }
                var score = (Double) row.get(1);
                assertNotNull(score);
                assertTrue(score > 0.0);
            }
        }
    }

    public void testKnnKOverridesLimit() {
        float[] queryVector = new float[numDims];
        Arrays.fill(queryVector, 0.0f);

        var query = String.format(Locale.ROOT, """
            FROM test METADATA _score
            | WHERE knn(vector, %s, {"k": 5, "min_candidates": 20})
            | KEEP id, _score, vector
            | SORT _score DESC
            | LIMIT 10
            """, Arrays.toString(queryVector));

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score", "vector"));
            assertColumnTypes(resp.columns(), List.of("integer", "double", "dense_vector"));

            List<List<Object>> valuesList = EsqlTestUtils.getValuesList(resp);
            assertEquals(5, valuesList.size());
        }
    }

    public void testDenseVectorQueryParams() {
        float[] queryVector = new float[numDims];
        Arrays.fill(queryVector, 0);
        EsqlQueryRequest queryRequest = new EsqlQueryRequest();
        QueryParams queryParams = new QueryParams(
            List.of(new QueryParam("queryVector", Arrays.asList(queryVector), DataType.INTEGER, ParserUtils.ParamClassification.VALUE))
        );

        queryRequest.params(queryParams);

        var query = String.format(Locale.ROOT, """
            FROM test METADATA _score
            | WHERE knn(vector, %s) OR id > 100
            | KEEP id, _score, vector
            | SORT _score DESC
            | LIMIT 5
            """, Arrays.toString(queryVector));

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score", "vector"));
            assertColumnTypes(resp.columns(), List.of("integer", "double", "dense_vector"));

            List<List<Object>> valuesList = EsqlTestUtils.getValuesList(resp);
            assertEquals(5, valuesList.size());
        }
    }

    public void testKnnNonPushedDown() {
        float[] queryVector = new float[numDims];
        Arrays.fill(queryVector, 0.0f);

        var query = String.format(Locale.ROOT, """
            FROM test METADATA _score
            | WHERE knn(vector, %s) OR id > 100
            | KEEP id, _score, vector
            | SORT _score DESC
            | LIMIT 5
            """, Arrays.toString(queryVector));

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score", "vector"));
            assertColumnTypes(resp.columns(), List.of("integer", "double", "dense_vector"));

            List<List<Object>> valuesList = EsqlTestUtils.getValuesList(resp);
            assertEquals(5, valuesList.size());
        }
    }

    public void testKnnWithPrefilters() {
        float[] queryVector = new float[numDims];
        Arrays.fill(queryVector, 0.0f);

        // We retrieve 5 from knn, but must be prefiltered with id > 5 or no result will be returned as it would be post-filtered
        var query = String.format(Locale.ROOT, """
            FROM test METADATA _score
            | WHERE knn(vector, %s) AND id > 5 AND id <= 10
            | KEEP id, _score, vector
            | SORT _score DESC
            | LIMIT 5
            """, Arrays.toString(queryVector));

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score", "vector"));
            assertColumnTypes(resp.columns(), List.of("integer", "double", "dense_vector"));

            List<List<Object>> valuesList = EsqlTestUtils.getValuesList(resp);
            // K = 5, 1 more for every id > 10
            assertEquals(5, valuesList.size());
        }
    }

    public void testKnnWithLookupJoin() {
        float[] queryVector = new float[numDims];
        Arrays.fill(queryVector, 0.0f);

        var query = String.format(Locale.ROOT, """
            FROM test
            | LOOKUP JOIN test_lookup ON id
            | WHERE KNN(lookup_vector, %s) OR id > 100
            | LIMIT 5
            """, Arrays.toString(queryVector));

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(
            error.getMessage(),
            containsString(
                "line 3:13: [KNN] function cannot operate on [lookup_vector], supplied by an index [test_lookup] in non-STANDARD "
                    + "mode [lookup]"
            )
        );
    }

    @Before
    public void setup() throws IOException {
        var indexName = "test";
        var client = client().admin().indices();
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("id")
            .field("type", "integer")
            .endObject()
            .startObject("vector")
            .field("type", "dense_vector")
            .field(
                "similarity",
                // Let's not use others to avoid vector normalization
                randomFrom("l2_norm", "max_inner_product")
            )
            .startObject("index_options")
            .field("type", indexType)
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1);

        var createRequest = client.prepareCreate(indexName).setMapping(mapping).setSettings(settingsBuilder.build());
        assertAcked(createRequest);

        numDocs = randomIntBetween(20, 35);
        numDims = 64 + randomIntBetween(1, 10) * 2;
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            List<Number> vector = new ArrayList<>(numDims);
            for (int j = 0; j < numDims; j++) {
                switch (elementType) {
                    case FLOAT -> vector.add(randomFloatBetween(0F, 1F, true));
                    case BYTE, BIT -> vector.add((byte) (randomFloatBetween(0F, 1F, true) * 127.0f));
                    default -> throw new IllegalArgumentException("Unexpected element type: " + elementType);
                }
            }
            docs[i] = prepareIndex("test").setId(String.valueOf(i)).setSource("id", String.valueOf(i), "vector", vector);
            indexedVectors.put(i, vector);
        }

        indexRandom(true, docs);

        createAndPopulateLookupIndex(client, "test_lookup");
    }

    private void createAndPopulateLookupIndex(IndicesAdminClient client, String lookupIndexName) throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("id")
            .field("type", "integer")
            .endObject()
            .startObject("lookup_vector")
            .field("type", "dense_vector")
            .field("similarity", "l2_norm")
            .endObject()
            .endObject()
            .endObject();

        Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexSettings.MODE.getKey(), LOOKUP.getName());

        var createRequest = client.prepareCreate(lookupIndexName).setMapping(mapping).setSettings(settingsBuilder.build());
        assertAcked(createRequest);
    }
}
