/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.AbstractColumnarMapperCompatibilityTestCase;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.elasticsearch.index.codec.vectors.VectorTestUtils.randomFloatVector;
import static org.elasticsearch.index.codec.vectors.VectorTestUtils.randomNormalizedFloatVector;

/**
 * Verifies that the columnar batch path of {@code dense_vector} produces the same Lucene fields as the row path, and that the
 * configurations and documents it does not handle are refused so that the batch falls back to the row path.
 */
public class DenseVectorFieldMapperColumnarCompatibilityTests extends AbstractColumnarMapperCompatibilityTestCase {

    private static final String FIELD = "v";
    private static final int DIMS = 8;

    private static Settings vectordbColumnarSettings() {
        return Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.VECTORDB_COLUMNAR.getName()).build();
    }

    /** A {@code dense_vector} mapping; a {@code null} element type or similarity leaves that parameter to its default. */
    private static XContentBuilder vectorMapping(String elementType, String similarity) throws IOException {
        return mapping(b -> {
            b.startObject(FIELD).field("type", "dense_vector").field("dims", DIMS);
            if (similarity != null) {
                b.field("similarity", similarity);
            }
            if (elementType != null) {
                b.field("element_type", elementType);
            }
            b.endObject();
        });
    }

    /** A random non-zero vector, normalized to unit length when {@code unit} is set, as {@code dot_product} requires. */
    private static float[] randomVector(int dims, boolean unit) {
        return unit ? randomNormalizedFloatVector(dims) : randomFloatVector(dims);
    }

    /**
     * A vector as a JSON string holding its base64 encoding: big-endian float32 elements, or, when {@code bfloat16} is set,
     * big-endian bfloat16 elements (the upper 16 bits of each float), the two encodings a float or bfloat16 field accepts.
     */
    private static String base64VectorJson(float[] vector, boolean bfloat16) {
        final ByteBuffer encoded = ByteBuffer.allocate(vector.length * (bfloat16 ? Short.BYTES : Float.BYTES));
        for (float value : vector) {
            if (bfloat16) {
                encoded.putShort((short) (Float.floatToIntBits(value) >>> 16));
            } else {
                encoded.putFloat(value);
            }
        }
        return "\"" + Base64.getEncoder().encodeToString(encoded.array()) + "\"";
    }

    /** A random vector, normalized to unit length when {@code unit} is set, rendered as a JSON array of floats. */
    private static String randomVectorJson(int dims, boolean unit) {
        final float[] vector = randomVector(dims, unit);
        final StringBuilder json = new StringBuilder("[");
        for (int i = 0; i < dims; i++) {
            if (i > 0) {
                json.append(',');
            }
            json.append(vector[i]);
        }
        return json.append(']').toString();
    }

    private static String vectorDoc(String vectorJson) {
        return "{\"" + FIELD + "\":" + vectorJson + "}";
    }

    private static Batch randomBatch(String name, boolean unit) {
        final int docCount = between(1, 20);
        final List<Doc> docs = new ArrayList<>(docCount);
        for (int i = 0; i < docCount; i++) {
            final String source = switch (between(0, 5)) {
                case 0 -> "{}";
                case 1 -> vectorDoc("null");
                default -> vectorDoc(randomVectorJson(DIMS, unit));
            };
            docs.add(doc("d" + i, i, source));
        }
        return batch(name, 1L, docs.toArray(Doc[]::new));
    }

    public void testFloatDotProduct() throws IOException {
        assertColumnarMatchesXContent(
            vectorMapping("float", "dot_product"),
            vectordbColumnarSettings(),
            batch(
                "float dot_product",
                1L,
                doc("d1", 1L, vectorDoc(randomVectorJson(DIMS, true))),
                doc("d2", 2L, vectorDoc(randomVectorJson(DIMS, true)))
            ),
            randomBatch("random float dot_product", true)
        );
    }

    /** {@code bfloat16} is the {@code vectordb_columnar} default element type; it indexes the same float vector as {@code float}. */
    public void testDefaultBFloat16DotProduct() throws IOException {
        assertColumnarMatchesXContent(
            vectorMapping(null, "dot_product"),
            vectordbColumnarSettings(),
            randomBatch("random bfloat16 dot_product", true)
        );
    }

    public void testL2Norm() throws IOException {
        assertColumnarMatchesXContent(
            vectorMapping(randomFrom("float", "bfloat16"), "l2_norm"),
            vectordbColumnarSettings(),
            randomBatch("random l2_norm", false)
        );
    }

    public void testMaxInnerProduct() throws IOException {
        assertColumnarMatchesXContent(
            vectorMapping(randomFrom("float", "bfloat16"), "max_inner_product"),
            vectordbColumnarSettings(),
            randomBatch("random max_inner_product", false)
        );
    }

    public void testAbsentAndNullDocs() throws IOException {
        assertColumnarMatchesXContent(
            vectorMapping("float", "l2_norm"),
            vectordbColumnarSettings(),
            batch(
                "absent and null",
                1L,
                doc("d1", 1L, "{}"),
                doc("d2", 2L, vectorDoc(randomVectorJson(DIMS, false))),
                doc("d3", 3L, vectorDoc("null")),
                doc("d4", 4L, vectorDoc(randomVectorJson(DIMS, false)))
            )
        );
    }

    /**
     * Normalized {@code cosine} indexes a non-unit vector normalized and keeps its length in the {@code ._magnitude} doc values field,
     * while a unit vector is indexed as-is with no magnitude, so a batch mixing both exercises the sparse magnitude column.
     */
    public void testCosineMixedUnitAndNonUnitVectors() throws IOException {
        assertColumnarMatchesXContent(
            vectorMapping(randomFrom("float", "bfloat16"), "cosine"),
            vectordbColumnarSettings(),
            batch(
                "cosine unit and non-unit",
                1L,
                doc("d1", 1L, vectorDoc(randomVectorJson(DIMS, false))),
                doc("d2", 2L, vectorDoc(randomVectorJson(DIMS, true))),
                doc("d3", 3L, "{}"),
                doc("d4", 4L, vectorDoc("[1,2,3,4,5,6,7,8]"))
            ),
            batch("cosine all unit", 1L, doc("d1", 1L, vectorDoc(randomVectorJson(DIMS, true)))),
            randomBatch("random cosine", randomBoolean())
        );
    }

    /** Without an explicit similarity, a {@code vectordb_columnar} vector is a {@code bfloat16} field with {@code cosine}. */
    public void testDefaultMapping() throws IOException {
        assertColumnarMatchesXContent(
            vectorMapping(null, null),
            vectordbColumnarSettings(),
            randomBatch("random default mapping", false),
            randomBatch("random default mapping, unit vectors", true)
        );
    }

    /** Whole-number elements reach the batch path as longs, and elements mixing whole and fractional numbers as a union. */
    public void testIntegerAndMixedElements() throws IOException {
        assertColumnarMatchesXContent(
            vectorMapping("float", "l2_norm"),
            vectordbColumnarSettings(),
            batch("integer elements", 1L, doc("d1", 1L, vectorDoc("[1,2,3,4,5,6,7,8]")), doc("d2", 2L, vectorDoc("[0,0,0,0,0,0,0,1]"))),
            batch("mixed elements", 1L, doc("d1", 1L, vectorDoc("[1,0.5,3,-0.25,5,6.75,7,8]")))
        );
    }

    public void testSupportedConfigurations() throws IOException {
        for (String elementType : new String[] { "float", "bfloat16" }) {
            for (String similarity : new String[] { "l2_norm", "dot_product", "max_inner_product", "cosine" }) {
                assertTrue(elementType + "/" + similarity, supportsColumnarParse(vectorMapping(elementType, similarity)));
            }
        }
        assertTrue("default mapping", supportsColumnarParse(vectorMapping(null, null)));
    }

    public void testUnsupportedConfigurations() throws IOException {
        // byte and bit vectors are indexed as KnnByteVectorField
        assertFalse(supportsColumnarParse(vectorMapping("byte", "l2_norm")));
        assertFalse(
            supportsColumnarParse(
                mapping(b -> b.startObject(FIELD).field("type", "dense_vector").field("dims", 64).field("element_type", "bit").endObject())
            )
        );
        // unindexed vectors are binary doc values
        assertFalse(
            supportsColumnarParse(
                mapping(b -> b.startObject(FIELD).field("type", "dense_vector").field("dims", DIMS).field("index", false).endObject())
            )
        );
        // the dimensions still have to be learned from the first document
        assertFalse(supportsColumnarParse(mapping(b -> b.startObject(FIELD).field("type", "dense_vector").endObject())));
    }

    public void testWrongDimensionsFallsBack() throws IOException {
        final MapperService mapperService = createMapperService(vectordbColumnarSettings(), vectorMapping("float", "l2_norm"));
        expectThrows(
            UnsupportedOperationException.class,
            () -> mapColumnarLeaf(mapperService, FIELD, vectorDoc(randomVectorJson(DIMS - 1, false)))
        );
        expectThrows(
            UnsupportedOperationException.class,
            () -> mapColumnarLeaf(mapperService, FIELD, vectorDoc(randomVectorJson(DIMS + 1, false)))
        );
    }

    /** The row path rejects a zero vector for {@code cosine}, so the batch path must refuse it too. */
    public void testZeroCosineVectorFallsBack() throws IOException {
        final MapperService mapperService = createMapperService(vectordbColumnarSettings(), vectorMapping("float", "cosine"));
        expectThrows(IllegalArgumentException.class, () -> mapColumnarLeaf(mapperService, FIELD, vectorDoc("[0,0,0,0,0,0,0,0]")));
    }

    /**
     * Base64-encoded vectors, in the float32 and the bfloat16 encoding, index the same vectors as the row path, alone and in a batch
     * that mixes them with arrays, absent and {@code null} vectors, which makes the source column a union.
     */
    public void testBase64Vectors() throws IOException {
        final String similarity = randomFrom("l2_norm", "max_inner_product", "cosine");
        final List<Doc> mixed = new ArrayList<>();
        final int docCount = between(1, 20);
        for (int i = 0; i < docCount; i++) {
            final String source = switch (between(0, 4)) {
                case 0 -> "{}";
                case 1 -> vectorDoc("null");
                case 2 -> vectorDoc(randomVectorJson(DIMS, randomBoolean()));
                default -> vectorDoc(base64VectorJson(randomVector(DIMS, randomBoolean()), randomBoolean()));
            };
            mixed.add(doc("d" + i, i, source));
        }
        assertColumnarMatchesXContent(
            vectorMapping(randomFrom("float", "bfloat16"), similarity),
            vectordbColumnarSettings(),
            batch(
                "base64 float32 and bfloat16",
                1L,
                doc("d1", 1L, vectorDoc(base64VectorJson(randomVector(DIMS, false), false))),
                doc("d2", 2L, vectorDoc(base64VectorJson(randomVector(DIMS, false), true)))
            ),
            batch("base64 mixed with arrays", 1L, mixed.toArray(Doc[]::new))
        );
    }

    /** {@code dot_product} needs unit vectors; the float32 encoding keeps a unit vector unit length. */
    public void testBase64DotProduct() throws IOException {
        assertColumnarMatchesXContent(
            vectorMapping(randomFrom("float", "bfloat16"), "dot_product"),
            vectordbColumnarSettings(),
            batch(
                "base64 dot_product",
                1L,
                doc("d1", 1L, vectorDoc(base64VectorJson(randomVector(DIMS, true), false))),
                doc("d2", 2L, vectorDoc(randomVectorJson(DIMS, true)))
            )
        );
    }

    /** A string that is not a base64 vector of the mapped dimensions is left for the row path to reject. */
    public void testInvalidBase64VectorFallsBack() throws IOException {
        final MapperService mapperService = createMapperService(vectordbColumnarSettings(), vectorMapping("float", "l2_norm"));
        final String wrongDims = base64VectorJson(randomVector(DIMS - 1, false), randomBoolean());
        expectThrows(UnsupportedOperationException.class, () -> mapColumnarLeaf(mapperService, FIELD, vectorDoc(wrongDims)));
        expectThrows(UnsupportedOperationException.class, () -> mapColumnarLeaf(mapperService, FIELD, vectorDoc("\"not a vector!\"")));
    }

    public void testNonNumericElementFallsBack() throws IOException {
        final MapperService mapperService = createMapperService(vectordbColumnarSettings(), vectorMapping("float", "l2_norm"));
        expectThrows(
            UnsupportedOperationException.class,
            () -> mapColumnarLeaf(mapperService, FIELD, vectorDoc("[\"a\",\"b\",\"c\",\"d\",\"e\",\"f\",\"g\",\"h\"]"))
        );
    }

    /** A vector the row path rejects is rejected by the batch path as well, so that the row path can report the error. */
    public void testNonUnitDotProductVectorFallsBack() throws IOException {
        final MapperService mapperService = createMapperService(vectordbColumnarSettings(), vectorMapping("float", "dot_product"));
        expectThrows(IllegalArgumentException.class, () -> mapColumnarLeaf(mapperService, FIELD, vectorDoc("[1,2,3,4,5,6,7,8]")));
    }

    private boolean supportsColumnarParse(XContentBuilder mapping) throws IOException {
        final MapperService mapperService = createMapperService(vectordbColumnarSettings(), mapping);
        final FieldMapper mapper = (FieldMapper) mapperService.mappingLookup().getMapper(FIELD);
        return mapper.supportsColumnarParse(mapperService.getIndexSettings());
    }
}
