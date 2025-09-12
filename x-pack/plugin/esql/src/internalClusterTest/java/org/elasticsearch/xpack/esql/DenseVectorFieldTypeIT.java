/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.script.field.vectors.DenseVector;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
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
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.index.IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING;
import static org.elasticsearch.index.mapper.SourceFieldMapper.Mode.SYNTHETIC;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class DenseVectorFieldTypeIT extends AbstractEsqlIntegTestCase {

    public static final Set<String> ALL_DENSE_VECTOR_INDEX_TYPES = Arrays.stream(DenseVectorFieldMapper.VectorIndexType.values())
        .filter(DenseVectorFieldMapper.VectorIndexType::isEnabled)
        .map(v -> v.getName().toLowerCase(Locale.ROOT))
        .collect(Collectors.toSet());

    public static final Set<String> NON_QUANTIZED_DENSE_VECTOR_INDEX_TYPES = Arrays.stream(DenseVectorFieldMapper.VectorIndexType.values())
        .filter(t -> t.isEnabled() && t.isQuantized() == false)
        .map(v -> v.getName().toLowerCase(Locale.ROOT))
        .collect(Collectors.toSet());

    public static final float DELTA = 1e-7F;

    private final ElementType elementType;
    private final DenseVectorFieldMapper.VectorSimilarity similarity;
    private final boolean synthetic;
    private final boolean index;

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        List<Object[]> params = new ArrayList<>();

        for (ElementType elementType : List.of(ElementType.BYTE, ElementType.FLOAT)) {
            // Test all similarities
            for (DenseVectorFieldMapper.VectorSimilarity similarity : DenseVectorFieldMapper.VectorSimilarity.values()) {
                params.add(new Object[] { elementType, similarity, true, false });
            }

            // No indexing
            params.add(new Object[] { elementType, null, false, false });
            // No indexing, synthetic source
            params.add(new Object[] { elementType, null, false, true });
        }

        return params;
    }

    public DenseVectorFieldTypeIT(
        @Name("elementType") ElementType elementType,
        @Name("similarity") DenseVectorFieldMapper.VectorSimilarity similarity,
        @Name("index") boolean index,
        @Name("synthetic") boolean synthetic
    ) {
        this.elementType = elementType;
        this.similarity = similarity;
        this.index = index;
        this.synthetic = synthetic;
    }

    private final Map<Integer, List<Number>> indexedVectors = new HashMap<>();

    public void testRetrieveFieldType() {
        var query = """
            FROM test
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "vector"));
            assertColumnTypes(resp.columns(), List.of("integer", "dense_vector"));
        }
    }

    @SuppressWarnings("unchecked")
    public void testRetrieveTopNDenseVectorFieldData() {
        var query = """
                FROM test
                | KEEP id, vector
                | SORT id ASC
            """;

        try (var resp = run(query)) {
            List<List<Object>> valuesList = EsqlTestUtils.getValuesList(resp);
            indexedVectors.forEach((id, expectedVector) -> {
                var values = valuesList.get(id);
                assertEquals(id, values.get(0));
                List<Number> actualVector = (List<Number>) values.get(1);
                if (expectedVector == null) {
                    assertNull(actualVector);
                } else {
                    assertNotNull(actualVector);
                    assertEquals(expectedVector.size(), actualVector.size());
                    for (int i = 0; i < expectedVector.size(); i++) {
                        assertEquals(expectedVector.get(i).floatValue(), actualVector.get(i).floatValue(), DELTA);
                    }
                }
            });
        }
    }

    @SuppressWarnings("unchecked")
    public void testRetrieveDenseVectorFieldData() {
        var query = """
            FROM test
            | KEEP id, vector
            """;

        try (var resp = run(query)) {
            List<List<Object>> valuesList = EsqlTestUtils.getValuesList(resp);
            assertEquals(valuesList.size(), indexedVectors.size());
            // print all values for debugging
            valuesList.forEach(value -> {
                assertEquals(2, value.size());
                Integer id = (Integer) value.get(0);
                List<Number> expectedVector = indexedVectors.get(id);
                List<Number> actualVector = (List<Number>) value.get(1);
                if (expectedVector == null) {
                    assertNull(actualVector);
                } else {
                    assertNotNull(actualVector);
                    assertEquals(expectedVector.size(), actualVector.size());
                    for (int i = 0; i < actualVector.size(); i++) {
                        assertEquals(
                            "Actual: " + actualVector + "; expected: " + expectedVector,
                            expectedVector.get(i).floatValue(),
                            actualVector.get(i).floatValue(),
                            DELTA
                        );
                    }
                }
            });
        }
    }

    public void testNonIndexedDenseVectorField() throws IOException {
        createIndexWithDenseVector("no_dense_vectors");

        int numDocs = randomIntBetween(10, 100);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = prepareIndex("no_dense_vectors").setId("" + i).setSource("id", String.valueOf(i));
        }

        indexRandom(true, docs);

        var query = """
            FROM no_dense_vectors
            | KEEP id, vector
            """;

        try (var resp = run(query)) {
            List<List<Object>> valuesList = EsqlTestUtils.getValuesList(resp);
            assertEquals(numDocs, valuesList.size());
            valuesList.forEach(value -> {
                assertEquals(2, value.size());
                Integer id = (Integer) value.get(0);
                assertNotNull(id);
                Object vector = value.get(1);
                assertNull(vector);
            });
        }
    }

    @Before
    public void setup() throws IOException {
        assumeTrue("Dense vector type is disabled", EsqlCapabilities.Cap.DENSE_VECTOR_FIELD_TYPE.isEnabled());

        createIndexWithDenseVector("test");

        int numDims = randomIntBetween(32, 64) * 2; // min 64, even number
        int numDocs = randomIntBetween(10, 100);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            List<Number> vector = new ArrayList<>(numDims);
            if (rarely()) {
                docs[i] = prepareIndex("test").setId("" + i).setSource("id", String.valueOf(i));
                indexedVectors.put(i, null);
            } else {
                for (int j = 0; j < numDims; j++) {
                    switch (elementType) {
                        case FLOAT -> vector.add(randomFloatBetween(0F, 1F, true));
                        case BYTE -> vector.add((byte) (randomFloatBetween(0F, 1F, true) * 127.0f));
                        default -> throw new IllegalArgumentException("Unexpected element type: " + elementType);
                    }
                }
                if ((elementType == ElementType.FLOAT) && (similarity == DenseVectorFieldMapper.VectorSimilarity.DOT_PRODUCT || rarely())) {
                    // Normalize the vector
                    float magnitude = DenseVector.getMagnitude(vector);
                    vector.replaceAll(number -> number.floatValue() / magnitude);
                }
                docs[i] = prepareIndex("test").setId("" + i).setSource("id", String.valueOf(i), "vector", vector);
                indexedVectors.put(i, vector);
            }
        }

        indexRandom(true, docs);
    }

    private void createIndexWithDenseVector(String indexName) throws IOException {
        var client = client().admin().indices();
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("id")
            .field("type", "integer")
            .endObject()
            .startObject("vector")
            .field("type", "dense_vector")
            .field("element_type", elementType.toString().toLowerCase(Locale.ROOT))
            .field("index", index);
        if (index) {
            mapping.field("similarity", similarity.name().toLowerCase(Locale.ROOT));
            String indexType = elementType == ElementType.FLOAT
                ? randomFrom(ALL_DENSE_VECTOR_INDEX_TYPES)
                : randomFrom(NON_QUANTIZED_DENSE_VECTOR_INDEX_TYPES);
            mapping.startObject("index_options").field("type", indexType).endObject();
        }
        mapping.endObject().endObject().endObject();
        Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 5));
        if (synthetic) {
            settingsBuilder.put(INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SYNTHETIC);
        }

        var createRequest = client.prepareCreate(indexName)
            .setSettings(Settings.builder().put("index.number_of_shards", 1))
            .setMapping(mapping)
            .setSettings(settingsBuilder.build());
        assertAcked(createRequest);
    }
}
