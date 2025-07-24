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
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.index.IndexMode.LOOKUP;
import static org.elasticsearch.index.IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING;
import static org.elasticsearch.index.mapper.SourceFieldMapper.Mode.SYNTHETIC;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.DenseVectorFieldTypeIT.ALL_DENSE_VECTOR_INDEX_TYPES;
import static org.elasticsearch.xpack.esql.DenseVectorFieldTypeIT.NON_QUANTIZED_DENSE_VECTOR_INDEX_TYPES;
import static org.hamcrest.CoreMatchers.containsString;

public class KnnFunctionIT extends AbstractEsqlIntegTestCase {

    private final Map<Integer, List<Number>> indexedVectors = new HashMap<>();
    private int numDocs;
    private int numDims;

    private final DenseVectorFieldMapper.ElementType elementType;
    private final String indexType;
    private final boolean synthetic;
    private final boolean index;


    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        List<Object[]> params = new ArrayList<>();
        // Indexed field types
        for (String indexType : ALL_DENSE_VECTOR_INDEX_TYPES) {
            params.add(new Object[] {DenseVectorFieldMapper.ElementType.FLOAT, indexType, true, false });
        }
//        params.add(new Object[] {DenseVectorFieldMapper.ElementType.BYTE, "flat", true, false });
        for (String indexType : NON_QUANTIZED_DENSE_VECTOR_INDEX_TYPES) {
            params.add(new Object[] {DenseVectorFieldMapper.ElementType.BYTE, indexType, true, false });
        }
        for (DenseVectorFieldMapper.ElementType elementType : List.of(
            DenseVectorFieldMapper.ElementType.BYTE,
            DenseVectorFieldMapper.ElementType.FLOAT
        )) {
            // No indexing
            params.add(new Object[]{elementType, null, false, false});
            // No indexing, synthetic source
            params.add(new Object[]{elementType, null, false, true});
        }

        // Remove flat index types, as knn does not do a top k for flat
        params.removeIf(param -> param[1] != null && ((String) param[1]).contains("flat"));
        return params;
    }

    public KnnFunctionIT(
        @Name("elementType") DenseVectorFieldMapper.ElementType elementType,
        @Name("indexType") String indexType,
        @Name("index") boolean index,
        @Name("synthetic") boolean synthetic
    ) {
        this.elementType = elementType;
        this.indexType = indexType;
        this.index = index;
        this.synthetic = synthetic;
    }

    public void testKnnDefaults() {
        float[] queryVector = new float[numDims];
        Arrays.fill(queryVector, 0.0f);

        var query = String.format(Locale.ROOT, """
            FROM test METADATA _score
            | WHERE knn(vector, %s, 10)
            | KEEP id, floats, _score, vector
            | SORT _score DESC
            """, Arrays.toString(queryVector));

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "floats", "_score", "vector"));
            assertColumnTypes(resp.columns(), List.of("integer", "double", "double", "dense_vector"));

            List<List<Object>> valuesList = EsqlTestUtils.getValuesList(resp);
            assertEquals(Math.min(indexedVectors.size(), 10), valuesList.size());
            for (int i = 0; i < valuesList.size(); i++) {
                List<Object> row = valuesList.get(i);
                // Vectors should be in order of ID, as they're less similar than the query vector as the ID increases
                assertEquals(i, row.getFirst());
                @SuppressWarnings("unchecked")
                // Vectors should be the same
                List<Number> floats = (List<Number>) row.get(1);
                for (int j = 0; j < floats.size(); j++) {
                    assertEquals(floats.get(j).floatValue(), indexedVectors.get(i).get(j).floatValue(), 0f);
                }
                var score = (Double) row.get(2);
                assertNotNull(score);
                assertTrue(score > 0.0);
            }
        }
    }

    public void testKnnOptions() {
        float[] queryVector = new float[numDims];
        Arrays.fill(queryVector, 0.0f);

        var query = String.format(Locale.ROOT, """
            FROM test METADATA _score
            | WHERE knn(vector, %s, 5)
            | KEEP id, floats, _score, vector
            | SORT _score DESC
            """, Arrays.toString(queryVector));

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "floats", "_score", "vector"));
            assertColumnTypes(resp.columns(), List.of("integer", "double", "double", "dense_vector"));

            List<List<Object>> valuesList = EsqlTestUtils.getValuesList(resp);
            assertEquals(5, valuesList.size());
        }
    }

    public void testKnnNonPushedDown() {
        float[] queryVector = new float[numDims];
        Arrays.fill(queryVector, 0.0f);

        // TODO we need to decide what to do when / if user uses k for limit, as no more than k results will be returned from knn query
        var query = String.format(Locale.ROOT, """
            FROM test METADATA _score
            | WHERE knn(vector, %s, 5) OR (id > 10 AND id <= 15)
            | KEEP id, floats, _score, vector
            | SORT _score DESC
            """, Arrays.toString(queryVector));

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "floats", "_score", "vector"));
            assertColumnTypes(resp.columns(), List.of("integer", "double", "double", "dense_vector"));

            List<List<Object>> valuesList = EsqlTestUtils.getValuesList(resp);
            // K = 5, and 5 from the disjunction
            assertEquals(10, valuesList.size());
        }
    }

    public void testKnnWithPrefilters() {
        float[] queryVector = new float[numDims];
        Arrays.fill(queryVector, 1.0f);

        // We retrieve 5 from knn, but must be prefiltered with id > 5 or no result will be returned as it would be post-filtered
        var query = String.format(Locale.ROOT, """
            FROM test METADATA _score
            | WHERE knn(vector, %s, 5) AND id > 5 AND id <= 10
            | KEEP id, floats, _score, vector
            | SORT _score DESC
            | LIMIT 5
            """, Arrays.toString(queryVector));

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "floats", "_score", "vector"));
            assertColumnTypes(resp.columns(), List.of("integer", "double", "double", "dense_vector"));

            List<List<Object>> valuesList = EsqlTestUtils.getValuesList(resp);
            // K = 5, 1 more for every id > 10
            assertEquals(5, valuesList.size());
        }
    }

    public void testKnnWithLookupJoin() {
        float[] queryVector = new float[numDims];
        Arrays.fill(queryVector, 1.0f);

        var query = String.format(Locale.ROOT, """
            FROM test
            | LOOKUP JOIN test_lookup ON id
            | WHERE KNN(lookup_vector, %s, 5) OR (id > 5 AND id <= 10)
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
        assumeTrue("Needs KNN support", EsqlCapabilities.Cap.KNN_FUNCTION_V3.isEnabled());

        var indexName = "test";
        var client = client().admin().indices();
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("id")
            .field("type", "integer")
            .endObject()
            .startObject("floats")
            .field("type", "float")
            .endObject()
            .startObject("vector")
            .field("type", "dense_vector");
        if (index) {
            mapping.field(
                "similarity",
                // Let's not use others to avoid vector normalization
                randomFrom("l2_norm", "max_inner_product")
            );
        }
        if (indexType != null) {
            mapping.startObject("index_options").field("type", indexType).endObject();
        }
        mapping.endObject().endObject().endObject();

        Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1);
        if (synthetic) {
            settingsBuilder.put(INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SYNTHETIC);
        }

        var createRequest = client.prepareCreate(indexName).setMapping(mapping).setSettings(settingsBuilder.build());
        assertAcked(createRequest);

        numDocs = randomIntBetween(20, 35);
        numDims = 64 + randomIntBetween(1, 10) * 2;
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        byte value = 0;
        for (int i = 0; i < numDocs; i++) {
            List<Number> vector = new ArrayList<>(numDims);
            for (int j = 0; j < numDims; j++) {
                vector.add(value);
            }
            value++;
            docs[i] = prepareIndex("test").setId("" + i).setSource("id", String.valueOf(i), "floats", vector, "vector", vector);
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
