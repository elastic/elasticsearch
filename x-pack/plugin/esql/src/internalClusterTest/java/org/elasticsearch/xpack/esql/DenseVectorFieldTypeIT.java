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
import org.elasticsearch.index.codec.vectors.BFloat16;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.script.field.vectors.DenseVector;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.index.IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING;
import static org.elasticsearch.index.IndexSettings.INDEX_MAPPING_EXCLUDE_SOURCE_VECTORS_SETTING;
import static org.elasticsearch.index.mapper.SourceFieldMapper.Mode.SYNTHETIC;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.L2_NORM_VECTOR_SIMILARITY_FUNCTION;
import static org.hamcrest.Matchers.hasKey;

public class DenseVectorFieldTypeIT extends AbstractEsqlIntegTestCase {

    private enum VectorSourceOptions {
        DEFAULT,
        SYNTHETIC,
        INCLUDE_SOURCE_VECTORS
    }

    public static final Set<String> ALL_DENSE_VECTOR_INDEX_TYPES = Arrays.stream(DenseVectorFieldMapper.VectorIndexType.values())
        .map(v -> v.getName().toLowerCase(Locale.ROOT))
        .collect(Collectors.toSet());

    public static final Set<String> NON_QUANTIZED_DENSE_VECTOR_INDEX_TYPES = Arrays.stream(DenseVectorFieldMapper.VectorIndexType.values())
        .filter(t -> t.isQuantized() == false)
        .map(v -> v.getName().toLowerCase(Locale.ROOT))
        .collect(Collectors.toSet());

    public static final float DELTA = 1e-7F;

    private final ElementType elementType;
    private final DenseVectorFieldMapper.VectorSimilarity similarity;
    private final VectorSourceOptions sourceOptions;
    private final boolean index;

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        List<Object[]> params = new ArrayList<>();
        for (ElementType elementType : List.of(ElementType.BYTE, ElementType.FLOAT, ElementType.BIT)) {
            // Test all similarities
            for (DenseVectorFieldMapper.VectorSimilarity similarity : DenseVectorFieldMapper.VectorSimilarity.values()) {
                if (elementType == ElementType.BIT && similarity != DenseVectorFieldMapper.VectorSimilarity.L2_NORM) {
                    continue;
                }
                params.add(new Object[] { elementType, similarity, true, VectorSourceOptions.DEFAULT });
                params.add(new Object[] { elementType, similarity, true, VectorSourceOptions.SYNTHETIC });
                params.add(new Object[] { elementType, similarity, true, VectorSourceOptions.INCLUDE_SOURCE_VECTORS });
            }

            params.add(new Object[] { elementType, null, false, VectorSourceOptions.DEFAULT });
            params.add(new Object[] { elementType, null, false, VectorSourceOptions.SYNTHETIC });
            params.add(new Object[] { elementType, null, false, VectorSourceOptions.INCLUDE_SOURCE_VECTORS });
        }

        return params;
    }

    public DenseVectorFieldTypeIT(
        @Name("elementType") ElementType elementType,
        @Name("similarity") DenseVectorFieldMapper.VectorSimilarity similarity,
        @Name("index") boolean index,
        @Name("sourceOptions") VectorSourceOptions sourceOptions
    ) {
        this.elementType = elementType;
        this.similarity = similarity;
        this.index = index;
        this.sourceOptions = sourceOptions;
    }

    private final Map<Integer, List<Number>> indexedVectors = new HashMap<>();

    public void testRetrieveFieldType() {
        assumeTrue("Need L2_NORM available for dense_vector retrieval", L2_NORM_VECTOR_SIMILARITY_FUNCTION.isEnabled());

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
        assumeTrue("Need L2_NORM available for dense_vector retrieval", L2_NORM_VECTOR_SIMILARITY_FUNCTION.isEnabled());

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
        assumeTrue("Need L2_NORM available for dense_vector retrieval", L2_NORM_VECTOR_SIMILARITY_FUNCTION.isEnabled());

        var query = """
            FROM test
            | KEEP id, vector
            """;

        try (var resp = run(query)) {
            List<List<Object>> valuesList = EsqlTestUtils.getValuesList(resp);
            assertEquals(valuesList.size(), indexedVectors.size());
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

    @SuppressWarnings("unchecked")
    public void testDenseVectorsIncludedInSource() {
        var query = """
            FROM test METADATA _source
            | KEEP _source
            """;

        try (var resp = run(query)) {
            List<List<Object>> valuesList = EsqlTestUtils.getValuesList(resp);
            assertEquals(valuesList.size(), indexedVectors.size());
            valuesList.forEach(value -> {
                assertEquals(1, value.size());
                Map<String, Object> source = (Map<String, Object>) value.get(0);
                assertThat(source, hasKey("id"));
                assertNotNull(source.get("id"));
                Integer id = Integer.valueOf(source.get("id").toString());
                // Vectors should be in _source if they are included in the index settings, and the vector is not null
                if (sourceOptions == VectorSourceOptions.INCLUDE_SOURCE_VECTORS && indexedVectors.get(id) != null) {
                    assertThat(source, hasKey("vector"));
                    assertNotNull(source.get("vector"));
                } else {
                    assertThat(source, Matchers.aMapWithSize(1));
                }
            });
        }
    }

    public void testNonIndexedDenseVectorField() throws IOException {
        createIndexWithDenseVector("no_dense_vectors", 64);

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
        int numDims = randomIntBetween(8, 16) * 8; // min 64, even number
        createIndexWithDenseVector("test", numDims);
        if (elementType == ElementType.BIT) {
            numDims = numDims / 8;
        }
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
                        case BYTE, BIT -> vector.add((byte) randomIntBetween(-128, 127));
                        default -> throw new IllegalArgumentException("Unexpected element type: " + elementType);
                    }
                }
                if ((elementType == ElementType.FLOAT) && (similarity == DenseVectorFieldMapper.VectorSimilarity.DOT_PRODUCT || rarely())) {
                    // Normalize the vector
                    float magnitude = DenseVector.getMagnitude(vector);
                    vector.replaceAll(number -> number.floatValue() / magnitude);
                }
                Object vectorToIndex;
                if (randomBoolean()) {
                    vectorToIndex = vector;
                } else {
                    // Test array input
                    vectorToIndex = switch (elementType) {
                        case FLOAT -> {
                            float[] array = new float[numDims];
                            for (int k = 0; k < numDims; k++) {
                                array[k] = vector.get(k).floatValue();
                            }
                            final ByteBuffer buffer = ByteBuffer.allocate(Float.BYTES * numDims);
                            buffer.asFloatBuffer().put(array);
                            yield Base64.getEncoder().encodeToString(buffer.array());
                        }
                        case BFLOAT16 -> {
                            float[] array = new float[numDims];
                            for (int k = 0; k < numDims; k++) {
                                array[k] = vector.get(k).floatValue();
                            }
                            final ByteBuffer buffer = ByteBuffer.allocate(BFloat16.BYTES * numDims);
                            BFloat16.floatToBFloat16(array, buffer.asShortBuffer());
                            yield Base64.getEncoder().encodeToString(buffer.array());
                        }
                        case BYTE, BIT -> {
                            byte[] array = new byte[numDims];
                            for (int k = 0; k < numDims; k++) {
                                array[k] = vector.get(k).byteValue();
                            }
                            yield randomBoolean() ? Base64.getEncoder().encodeToString(array) : HexFormat.of().formatHex(array);
                        }
                    };
                }
                docs[i] = prepareIndex("test").setId("" + i).setSource("id", String.valueOf(i), "vector", vectorToIndex);
                indexedVectors.put(i, vector);
            }
        }

        indexRandom(true, docs);
    }

    private void createIndexWithDenseVector(String indexName, int dims) throws IOException {
        var client = client().admin().indices();
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("id")
            .field("type", "integer")
            .endObject()
            .startObject("vector")
            .field("type", "dense_vector")
            .field("dims", dims)
            .field("element_type", elementType.toString().toLowerCase(Locale.ROOT))
            .field("index", index);
        if (index) {
            mapping.field("similarity", similarity.name().toLowerCase(Locale.ROOT));
            String indexType;
            if (elementType == ElementType.FLOAT) {
                indexType = randomFrom(ALL_DENSE_VECTOR_INDEX_TYPES);
            } else {
                indexType = randomFrom(NON_QUANTIZED_DENSE_VECTOR_INDEX_TYPES);
            }
            mapping.startObject("index_options").field("type", indexType).endObject();
        }
        mapping.endObject().endObject().endObject();
        Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 5));
        switch (sourceOptions) {
            // ensure vectors are actually in _source
            case INCLUDE_SOURCE_VECTORS -> settingsBuilder.put(INDEX_MAPPING_EXCLUDE_SOURCE_VECTORS_SETTING.getKey(), false);
            // ensure synthetic source is on
            case SYNTHETIC -> settingsBuilder.put(INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SYNTHETIC);
            // default, which is vectors outside of source and synthetic off
            case DEFAULT -> {
            }
        }

        var createRequest = client.prepareCreate(indexName)
            .setSettings(Settings.builder().put("index.number_of_shards", 1))
            .setMapping(mapping)
            .setSettings(settingsBuilder.build());
        assertAcked(createRequest);
    }
}
