/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.codec.vectors.BFloat16;
import org.elasticsearch.index.codec.vectors.VectorTestUtils;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.VectorFormat;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.VectorSimilarity;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.Before;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.index.IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING;
import static org.elasticsearch.index.IndexSettings.INDEX_MAPPING_EXCLUDE_SOURCE_VECTORS_SETTING;
import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapperTestUtils.getSupportedSimilarities;
import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapperTestUtils.randomCompatibleDimensions;
import static org.elasticsearch.index.query.QueryBuilders.idsQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Integration tests for the dense_vector fields API, covering all element types, all supported ingest value
 * formats (array, base64, hex), and both {@code format: "array"} and {@code format: "binary"} fetch formats.
 * Parameterized on synthetic source and {@code index.mapping.exclude_source_vectors}.
 */
public class DenseVectorFieldsApiTests extends ESSingleNodeTestCase {

    private static final String INDEX = "test";
    private static final int NESTED_ENTRIES = 3;

    /**
     * Bounds the bfloat16 round-trip error for random floats in [-1, 1). The bfloat16 format has 7 mantissa
     * bits. For values in [-1, 1) the ULP is at most 2^-8. When cosine normalization stacks with a second
     * quantization step (e.g. BASE64_BFLOAT16 ingest on a cosine field), errors can reach 2 ULP = 2^-7.
     */
    private static final float BFLOAT16_DELTA = 0x1p-7f;
    /** Delta for the cosine normalize/denormalize round-trip on float vectors. */
    private static final float FLOAT_COSINE_DELTA = 1e-5f;

    private record VectorSpec(ElementType elementType, int dims, SimilarityMeasure similarity, float[] floats, byte[] bytes) {
        @Override
        public float[] floats() {
            return switch (elementType) {
                case FLOAT, BFLOAT16 -> floats;
                case BYTE, BIT -> byteComponentsAsFloats(bytes);
            };
        }

        @Override
        public byte[] bytes() {
            assert elementType == ElementType.BYTE || elementType == ElementType.BIT;
            return bytes;
        }

        /** Returns true for BYTE and BIT element types whose binary output is one raw byte per component. */
        boolean isByteEncoded() {
            return switch (elementType) {
                case BYTE, BIT -> true;
                case FLOAT, BFLOAT16 -> false;
            };
        }

        /** Name of the indexed field for this element type. */
        String indexedField() {
            return elementType + "_indexed";
        }

        /** Name of the doc-values field for this element type. */
        String docValuesField() {
            return elementType + "_dv";
        }

        /**
         * Comparison delta, derived from the element type, similarity, and ingest format.
         */
        float delta(IngestFormat ingestFormat) {
            return switch (elementType) {
                case BFLOAT16 -> BFLOAT16_DELTA;
                case FLOAT -> {
                    // bfloat16-encoded base64 introduces the same amount of error as using the bfloat16 element type directly
                    if (ingestFormat == IngestFormat.BASE64_BFLOAT16) {
                        yield BFLOAT16_DELTA;
                    }
                    yield similarity.vectorSimilarity() == VectorSimilarity.COSINE ? FLOAT_COSINE_DELTA : 0.0f;
                }
                case BYTE, BIT -> 0.0f;
            };
        }
    }

    /** A value format accepted at index time for a dense_vector field. */
    private enum IngestFormat {
        ARRAY,
        HEX,
        BASE64_BYTES,
        BASE64_FLOAT32,
        BASE64_BFLOAT16;

        /** Returns true when this format can be used to ingest a vector of the given element type. */
        boolean supportedBy(ElementType elementType) {
            return switch (this) {
                case ARRAY -> true;
                case HEX, BASE64_BYTES -> elementType == ElementType.BYTE || elementType == ElementType.BIT;
                case BASE64_FLOAT32, BASE64_BFLOAT16 -> elementType == ElementType.FLOAT || elementType == ElementType.BFLOAT16;
            };
        }

        /** Returns the source value for this ingest format, built from the given spec. */
        Object sourceValue(VectorSpec spec) {
            return switch (this) {
                case ARRAY -> switch (spec.elementType()) {
                    case FLOAT, BFLOAT16 -> spec.floats();
                    case BYTE, BIT -> {
                        List<Integer> list = new ArrayList<>(spec.bytes().length);
                        for (byte b : spec.bytes()) {
                            list.add((int) b);
                        }
                        yield list;
                    }
                };
                case HEX -> HexFormat.of().formatHex(spec.bytes());
                case BASE64_BYTES -> Base64.getEncoder().encodeToString(spec.bytes());
                case BASE64_FLOAT32 -> encodeFloat32Base64(spec.floats());
                case BASE64_BFLOAT16 -> encodeBFloat16Base64(spec.floats());
            };
        }

        /** Doc id for a document ingested with this format for the given spec. */
        String docId(VectorSpec spec) {
            return spec.elementType() + "-" + name().toLowerCase(Locale.ROOT);
        }
    }

    private final boolean syntheticSource;
    private final boolean excludeSourceVectors;

    private List<VectorSpec> specs;
    private Map<VectorSpec, List<VectorSpec>> nestedSpecs;
    private Map<VectorSpec, List<VectorSpec>> innerNestedSpecs;

    public DenseVectorFieldsApiTests(
        @Name("syntheticSource") boolean syntheticSource,
        @Name("excludeSourceVectors") boolean excludeSourceVectors
    ) {
        this.syntheticSource = syntheticSource;
        this.excludeSourceVectors = excludeSourceVectors;
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return List.of(
            new Object[] { false, false },
            new Object[] { false, true },
            new Object[] { true, false },
            new Object[] { true, true }
        );
    }

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    @Before
    public void setupTestIndex() throws Exception {
        int floatDims = randomCompatibleDimensions(ElementType.FLOAT, 1, 32);
        int bfloat16Dims = randomCompatibleDimensions(ElementType.BFLOAT16, 1, 32);
        int byteDims = randomCompatibleDimensions(ElementType.BYTE, 1, 32);
        // bit requires dims multiple of 8; randomCompatibleDimensions handles this
        int bitDims = randomCompatibleDimensions(ElementType.BIT, 8, 64);

        specs = List.of(
            new VectorSpec(
                ElementType.FLOAT,
                floatDims,
                randomSimilarity(ElementType.FLOAT),
                VectorTestUtils.randomFloatVector(floatDims),
                null
            ),
            new VectorSpec(
                ElementType.BFLOAT16,
                bfloat16Dims,
                randomSimilarity(ElementType.BFLOAT16),
                VectorTestUtils.randomFloatVector(bfloat16Dims),
                null
            ),
            new VectorSpec(
                ElementType.BYTE,
                byteDims,
                randomSimilarity(ElementType.BYTE),
                null,
                VectorTestUtils.randomByteVector(ElementType.BYTE.vectorLength(byteDims))
            ),
            new VectorSpec(
                ElementType.BIT,
                bitDims,
                randomSimilarity(ElementType.BIT),
                null,
                VectorTestUtils.randomByteVector(ElementType.BIT.vectorLength(bitDims))
            )
        );

        nestedSpecs = new HashMap<>();
        innerNestedSpecs = new HashMap<>();
        for (VectorSpec spec : specs) {
            List<VectorSpec> nested = new ArrayList<>(NESTED_ENTRIES);
            for (int i = 0; i < NESTED_ENTRIES; i++) {
                VectorSpec outerSpec = variantOf(spec);
                nested.add(outerSpec);
                List<VectorSpec> inner = new ArrayList<>(NESTED_ENTRIES);
                for (int j = 0; j < NESTED_ENTRIES; j++) {
                    inner.add(variantOf(spec));
                }
                innerNestedSpecs.put(outerSpec, inner);
            }
            nestedSpecs.put(spec, nested);
        }

        createTestIndex();
        indexDocuments();
        indicesAdmin().prepareRefresh(INDEX).get();
    }

    /**
     * Test all cases in one test to reduce the number of times the index is created.
     */
    public void testFetch() {
        fetchArrayFormatTestCase();
        fetchBinaryFormatTestCase();
        fetchNestedArrayFormatTestCase();
        fetchNestedBinaryFormatTestCase();
    }

    /**
     * Verifies that the fields API with no format (defaulting to {@code "array"}) or with {@code format: "array"}
     * always returns the same list of float components, regardless of which format was used to ingest the vector.
     */
    public void fetchArrayFormatTestCase() {
        for (VectorFormat vectorFormat : new VectorFormat[] { null, VectorFormat.ARRAY }) {
            forEachSpecFormatAndField((spec, ingest, field) -> assertField(spec, ingest, field, vectorFormat));
        }
    }

    /**
     * Verifies that the fields API with {@code format: "binary"} always returns the canonical base64-encoded
     * form of the vector (raw bytes for byte/bit fields, big-endian float32 for float/bfloat16 fields),
     * regardless of which format was used to ingest the vector.
     */
    public void fetchBinaryFormatTestCase() {
        forEachSpecFormatAndField((spec, ingest, field) -> assertField(spec, ingest, field, VectorFormat.BINARY));
    }

    /**
     * Verifies that the fields API returns the correct float components for each entry of a {@code nested}
     * array and for each entry of the doubly-nested {@code nested.inner} array, fetching both levels in a
     * single request, regardless of which format was used to ingest the vectors.
     */
    public void fetchNestedArrayFormatTestCase() {
        for (VectorFormat vectorFormat : new VectorFormat[] { null, VectorFormat.ARRAY }) {
            forEachSpecFormatAndField((spec, ingest, field) -> assertNestedField(spec, ingest, field, vectorFormat));
        }
    }

    /**
     * Verifies that the fields API returns the correct binary-encoded vector for each entry of a {@code nested}
     * array and for each entry of the doubly-nested {@code nested.inner} array, fetching both levels in a
     * single request, regardless of which format was used to ingest the vectors.
     */
    public void fetchNestedBinaryFormatTestCase() {
        forEachSpecFormatAndField((spec, ingest, field) -> assertNestedField(spec, ingest, field, VectorFormat.BINARY));
    }

    /**
     * Fetches {@code field} for the document holding {@code spec}'s vector in {@code ingestFormat} and
     * validates the result against the expected components and delta derived from {@code spec}.
     */
    private void assertField(VectorSpec spec, IngestFormat ingestFormat, String field, VectorFormat vectorFormat) {
        String docId = ingestFormat.docId(spec);
        String fetchFormat = vectorFormat == null ? null : vectorFormat.toString();
        String label = docId + "/" + field + "/" + (fetchFormat == null ? "<default>" : fetchFormat);

        assertNoFailuresAndResponse(
            client().prepareSearch(INDEX).setQuery(idsQuery().addIds(docId)).addFetchField(new FieldAndFormat(field, fetchFormat)),
            response -> {
                assertEquals(label, 1, response.getHits().getHits().length);
                List<Object> values = response.getHits().getAt(0).field(field).getValues();
                assertVectorValues(label, spec, ingestFormat, values, vectorFormat);
            }
        );
    }

    /**
     * Fetches both {@code "nested." + field} and {@code "nested.inner." + field} in a single search for the
     * document holding {@code spec}'s vector in {@code ingestFormat}, and validates each of the
     * {@link #NESTED_ENTRIES} outer entries and each of the {@link #NESTED_ENTRIES} inner entries within them
     * against the per-entry variant specs stored in {@link #nestedSpecs} and {@link #innerNestedSpecs}.
     */
    @SuppressWarnings("unchecked")
    private void assertNestedField(VectorSpec spec, IngestFormat ingestFormat, String field, VectorFormat vectorFormat) {
        String docId = ingestFormat.docId(spec);
        String fetchFormat = vectorFormat == null ? null : vectorFormat.toString();
        String label = "nested/" + docId + "/" + field + "/" + (fetchFormat == null ? "<default>" : fetchFormat);

        assertNoFailuresAndResponse(
            client().prepareSearch(INDEX)
                .setQuery(idsQuery().addIds(docId))
                .addFetchField(new FieldAndFormat("nested." + field, fetchFormat))
                .addFetchField(new FieldAndFormat("nested.inner." + field, fetchFormat)),
            response -> {
                assertEquals(label, 1, response.getHits().getHits().length);
                var nestedDocField = response.getHits().getAt(0).field("nested");
                assertNotNull(label + " nested field must be present", nestedDocField);
                List<Object> outerEntries = nestedDocField.getValues();
                assertEquals(label + " outer entry count", NESTED_ENTRIES, outerEntries.size());

                List<VectorSpec> outerSpecs = nestedSpecs.get(spec);
                for (int i = 0; i < NESTED_ENTRIES; i++) {
                    VectorSpec outerSpec = outerSpecs.get(i);
                    Map<String, Object> outerEntry = (Map<String, Object>) outerEntries.get(i);

                    List<Object> outerValues = (List<Object>) outerEntry.get(field);
                    assertNotNull(label + " entry[" + i + "] must have field " + field, outerValues);
                    assertVectorValues(label + " entry[" + i + "]", outerSpec, ingestFormat, outerValues, vectorFormat);

                    List<Object> innerEntries = (List<Object>) outerEntry.get("inner");
                    assertNotNull(label + " entry[" + i + "] must have 'inner'", innerEntries);
                    assertEquals(label + " entry[" + i + "] inner entry count", NESTED_ENTRIES, innerEntries.size());

                    List<VectorSpec> innerSpecs = innerNestedSpecs.get(outerSpec);
                    for (int j = 0; j < NESTED_ENTRIES; j++) {
                        Map<String, Object> innerEntry = (Map<String, Object>) innerEntries.get(j);
                        List<Object> innerValues = (List<Object>) innerEntry.get(field);
                        assertNotNull(label + " entry[" + i + "].inner[" + j + "] must have field " + field, innerValues);
                        assertVectorValues(
                            label + " entry[" + i + "].inner[" + j + "]",
                            innerSpecs.get(j),
                            ingestFormat,
                            innerValues,
                            vectorFormat
                        );
                    }
                }
            }
        );
    }

    /** Validates a fetched vector value list against the expected spec, ingest format, and fetch format. */
    private void assertVectorValues(
        String label,
        VectorSpec spec,
        IngestFormat ingestFormat,
        List<Object> values,
        VectorFormat vectorFormat
    ) {
        if (VectorFormat.BINARY.equals(vectorFormat)) {
            assertEquals(label + " binary format returns a single base64 value", 1, values.size());

            Object value = values.getFirst();
            assertThat(value, instanceOf(String.class));
            byte[] decoded = Base64.getDecoder().decode((String) value);
            if (spec.isByteEncoded()) {
                // lossless: one raw byte per component, pins the exact encoding
                assertArrayEquals(label, spec.bytes(), decoded);
            } else {
                // potentially lossy: decode the big-endian float32 payload and compare with delta
                assertVectorComponents(label, spec.floats(), decodeFloat32(decoded), spec.delta(ingestFormat), Float.class);
            }
        } else {
            assertVectorComponents(
                label,
                spec.floats(),
                values,
                spec.delta(ingestFormat),
                spec.isByteEncoded() ? Integer.class : Float.class
            );
        }
    }

    private static void assertVectorComponents(
        String label,
        float[] expected,
        List<Object> actual,
        float delta,
        Class<? extends Number> expectedType
    ) {
        assertEquals(label + " component count", expected.length, actual.size());
        for (int i = 0; i < expected.length; i++) {
            Object actualValue = actual.get(i);
            assertThat(actualValue, instanceOf(expectedType));
            assertEquals(label + " component[" + i + "]", expected[i], ((Number) actualValue).floatValue(), delta);
        }
    }

    /**
     * Iterates over every combination of spec, supported ingest format, and field (indexed + doc-values),
     * calling {@code assertion} for each.
     */
    private void forEachSpecFormatAndField(TriConsumer<VectorSpec, IngestFormat, String> assertion) {
        for (VectorSpec spec : specs) {
            for (IngestFormat ingestFormat : IngestFormat.values()) {
                if (ingestFormat.supportedBy(spec.elementType()) == false) {
                    continue;
                }
                for (String field : new String[] { spec.indexedField(), spec.docValuesField() }) {
                    assertion.apply(spec, ingestFormat, field);
                }
            }
        }
    }

    private void createTestIndex() throws IOException {
        XContentBuilder mapping = buildMapping();
        Settings.Builder settingsBuilder = Settings.builder()
            .put(INDEX_MAPPING_EXCLUDE_SOURCE_VECTORS_SETTING.getKey(), excludeSourceVectors);
        if (syntheticSource) {
            settingsBuilder.put(INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.SYNTHETIC.toString());
        }
        createIndex(INDEX, settingsBuilder.build(), mapping);
    }

    private XContentBuilder buildMapping() throws IOException {
        XContentBuilder b = XContentFactory.jsonBuilder().startObject().startObject("properties");

        // Root-level fields
        for (VectorSpec spec : specs) {
            addVectorField(b, spec.indexedField(), spec.elementType(), spec.dims(), true, spec.similarity().toString());
            addVectorField(b, spec.docValuesField(), spec.elementType(), spec.dims(), false, null);
        }

        // nested object: mirrors every root field, plus a doubly-nested inner object
        b.startObject("nested").field("type", "nested").startObject("properties");
        for (VectorSpec spec : specs) {
            addVectorField(b, spec.indexedField(), spec.elementType(), spec.dims(), true, spec.similarity().toString());
            addVectorField(b, spec.docValuesField(), spec.elementType(), spec.dims(), false, null);
        }
        b.startObject("inner").field("type", "nested").startObject("properties");
        for (VectorSpec spec : specs) {
            addVectorField(b, spec.indexedField(), spec.elementType(), spec.dims(), true, spec.similarity().toString());
            addVectorField(b, spec.docValuesField(), spec.elementType(), spec.dims(), false, null);
        }
        b.endObject().endObject(); // inner.properties, inner
        b.endObject().endObject(); // nested.properties, nested

        return b.endObject().endObject();
    }

    private static void addVectorField(
        XContentBuilder b,
        String name,
        ElementType elementType,
        int dims,
        boolean indexed,
        String similarity
    ) throws IOException {
        b.startObject(name)
            .field("type", "dense_vector")
            .field("dims", dims)
            .field("element_type", elementType.toString())
            .field("index", indexed);
        if (indexed && similarity != null) {
            b.field("similarity", similarity);
        }
        b.endObject();
    }

    private void indexDocuments() {
        for (VectorSpec spec : specs) {
            for (IngestFormat ingestFormat : IngestFormat.values()) {
                if (ingestFormat.supportedBy(spec.elementType()) == false) {
                    continue;
                }
                Object value = ingestFormat.sourceValue(spec);

                // Build the nested array: NESTED_ENTRIES outer entries, each with NESTED_ENTRIES inner entries
                List<Map<String, Object>> nestedArray = new ArrayList<>(NESTED_ENTRIES);
                for (VectorSpec outerSpec : nestedSpecs.get(spec)) {
                    Object outerValue = ingestFormat.sourceValue(outerSpec);

                    List<Map<String, Object>> innerArray = new ArrayList<>(NESTED_ENTRIES);
                    for (VectorSpec innerSpec : innerNestedSpecs.get(outerSpec)) {
                        Object innerValue = ingestFormat.sourceValue(innerSpec);
                        Map<String, Object> innerEntry = new HashMap<>();
                        innerEntry.put(innerSpec.indexedField(), innerValue);
                        innerEntry.put(innerSpec.docValuesField(), innerValue);
                        innerArray.add(innerEntry);
                    }

                    Map<String, Object> outerEntry = new HashMap<>();
                    outerEntry.put(outerSpec.indexedField(), outerValue);
                    outerEntry.put(outerSpec.docValuesField(), outerValue);
                    outerEntry.put("inner", innerArray);
                    nestedArray.add(outerEntry);
                }

                Map<String, Object> source = new HashMap<>();
                source.put(spec.indexedField(), value);
                source.put(spec.docValuesField(), value);
                source.put("nested", nestedArray);

                index(ingestFormat.docId(spec), source);
            }
        }
    }

    private void index(String id, Map<String, Object> source) {
        prepareIndex(INDEX).setId(id).setSource(source).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get(TEST_REQUEST_TIMEOUT);
    }

    /** Returns a new {@link VectorSpec} with the same element type, dims, and similarity as {@code spec}, but fresh random vector data. */
    private VectorSpec variantOf(VectorSpec spec) {
        return switch (spec.elementType()) {
            case FLOAT, BFLOAT16 -> new VectorSpec(
                spec.elementType(),
                spec.dims(),
                spec.similarity(),
                VectorTestUtils.randomFloatVector(spec.dims()),
                null
            );
            case BYTE -> new VectorSpec(
                spec.elementType(),
                spec.dims(),
                spec.similarity(),
                null,
                VectorTestUtils.randomByteVector(ElementType.BYTE.vectorLength(spec.dims()))
            );
            case BIT -> new VectorSpec(
                spec.elementType(),
                spec.dims(),
                spec.similarity(),
                null,
                VectorTestUtils.randomByteVector(ElementType.BIT.vectorLength(spec.dims()))
            );
        };
    }

    /** Encodes {@code floats} as big-endian float32 bytes, then base64. */
    private static String encodeFloat32Base64(float[] floats) {
        ByteBuffer buf = ByteBuffer.allocate(floats.length * Float.BYTES).order(ByteOrder.BIG_ENDIAN);
        buf.asFloatBuffer().put(floats);
        return Base64.getEncoder().encodeToString(buf.array());
    }

    /** Encodes {@code floats} as big-endian bfloat16 bytes, then base64. */
    private static String encodeBFloat16Base64(float[] floats) {
        byte[] bytes = new byte[floats.length * BFloat16.BYTES];
        BFloat16.floatToBFloat16(floats, 0, bytes, 0, floats.length, ByteOrder.BIG_ENDIAN);
        return Base64.getEncoder().encodeToString(bytes);
    }

    /**
     * Converts raw bytes (from a byte or bit vector) to a float list matching the array format returned by
     * the fields API.
     */
    private static float[] byteComponentsAsFloats(byte[] bytes) {
        float[] floats = new float[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            floats[i] = bytes[i];
        }
        return floats;
    }

    /** Decodes big-endian IEEE-754 float32 bytes to a list of boxed floats. */
    private static List<Object> decodeFloat32(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        int count = bytes.length / Float.BYTES;
        List<Object> values = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            values.add(buf.getFloat());
        }
        return values;
    }

    /**
     * Picks a random similarity compatible with {@code elementType}, excluding {@link SimilarityMeasure#DOT_PRODUCT}
     * which requires unit-length vectors.
     */
    private SimilarityMeasure randomSimilarity(ElementType elementType) {
        List<SimilarityMeasure> valid = getSupportedSimilarities(elementType).stream()
            .filter(s -> s != SimilarityMeasure.DOT_PRODUCT)
            .toList();
        return randomFrom(valid);
    }
}
