/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.vectors;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene95.Lucene95HnswVectorsFormat;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.codec.PerFieldMapperCodec;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.DenseVectorFieldType;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.VectorSimilarity;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.lookup.SourceProvider;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.apache.lucene.codecs.lucene95.Lucene95HnswVectorsFormat.DEFAULT_BEAM_WIDTH;
import static org.apache.lucene.codecs.lucene95.Lucene95HnswVectorsFormat.DEFAULT_MAX_CONN;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DenseVectorFieldMapperTests extends MapperTestCase {

    private final ElementType elementType;
    private final boolean indexed;
    private final boolean indexOptionsSet;

    public DenseVectorFieldMapperTests() {
        this.elementType = randomFrom(ElementType.BYTE, ElementType.FLOAT);
        this.indexed = randomBoolean();
        this.indexOptionsSet = this.indexed && randomBoolean();
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "dense_vector").field("dims", 4);
        if (elementType != ElementType.FLOAT) {
            b.field("element_type", elementType.toString());
        }
        if (indexed) {
            b.field("index", true).field("similarity", "dot_product");
            if (indexOptionsSet) {
                b.startObject("index_options");
                b.field("type", "hnsw");
                b.field("m", 5);
                b.field("ef_construction", 50);
                b.endObject();
            }
        }
    }

    @Override
    protected Object getSampleValueForDocument() {
        return elementType == ElementType.BYTE ? List.of((byte) 1, (byte) 1, (byte) 1, (byte) 1) : List.of(0.5, 0.5, 0.5, 0.5);
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck(
            "dims",
            fieldMapping(b -> b.field("type", "dense_vector").field("dims", 4)),
            fieldMapping(b -> b.field("type", "dense_vector").field("dims", 5))
        );
        checker.registerConflictCheck(
            "similarity",
            fieldMapping(b -> b.field("type", "dense_vector").field("dims", 4).field("index", true).field("similarity", "dot_product")),
            fieldMapping(b -> b.field("type", "dense_vector").field("dims", 4).field("index", true).field("similarity", "l2_norm"))
        );
        checker.registerConflictCheck(
            "index",
            fieldMapping(b -> b.field("type", "dense_vector").field("dims", 4).field("index", true).field("similarity", "dot_product")),
            fieldMapping(b -> b.field("type", "dense_vector").field("dims", 4).field("index", false))
        );
        checker.registerConflictCheck(
            "element_type",
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", 4)
                    .field("index", true)
                    .field("similarity", "dot_product")
                    .field("element_type", "byte")
            ),
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", 4)
                    .field("index", true)
                    .field("similarity", "dot_product")
                    .field("element_type", "float")
            )
        );
        checker.registerConflictCheck(
            "index_options",
            fieldMapping(b -> b.field("type", "dense_vector").field("dims", 4).field("index", true).field("similarity", "dot_product")),
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", 4)
                    .field("index", true)
                    .field("similarity", "dot_product")
                    .startObject("index_options")
                    .field("type", "hnsw")
                    .field("m", 5)
                    .field("ef_construction", 80)
                    .endObject()
            )
        );
    }

    @Override
    protected boolean supportsStoredFields() {
        return false;
    }

    @Override
    protected boolean supportsIgnoreMalformed() {
        return false;
    }

    @Override
    protected void assertSearchable(MappedFieldType fieldType) {
        assertThat(fieldType, instanceOf(DenseVectorFieldType.class));
        assertEquals(fieldType.isIndexed(), indexed);
        assertEquals(fieldType.isSearchable(), indexed);
    }

    protected void assertExistsQuery(MappedFieldType fieldType, Query query, LuceneDocument fields) {
        assertThat(query, instanceOf(FieldExistsQuery.class));
        FieldExistsQuery existsQuery = (FieldExistsQuery) query;
        assertEquals("field", existsQuery.getField());
        assertNoFieldNamesField(fields);
    }

    // We override this because dense vectors are the only field type that are not aggregatable but
    // that do provide fielddata. TODO: resolve this inconsistency!
    @Override
    public void testAggregatableConsistency() {}

    public void testDims() {
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
                b.field("type", "dense_vector");
                b.field("dims", 0);
            })));
            assertThat(
                e.getMessage(),
                equalTo(
                    "Failed to parse mapping: " + "The number of dimensions for field [field] should be in the range [1, 2048] but was [0]"
                )
            );
        }
        // test max limit for non-indexed vectors
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
                b.field("type", "dense_vector");
                b.field("dims", 3000);
            })));
            assertThat(
                e.getMessage(),
                equalTo(
                    "Failed to parse mapping: "
                        + "The number of dimensions for field [field] should be in the range [1, 2048] but was [3000]"
                )
            );
        }
        // test max limit for indexed vectors
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
                b.field("type", "dense_vector");
                b.field("index", "true");
                b.field("dims", 3000);
            })));
            assertThat(
                e.getMessage(),
                equalTo(
                    "Failed to parse mapping: "
                        + "The number of dimensions for field [field] should be in the range [1, 2048] but was [3000]"
                )
            );
        }
        {
            Exception e = expectThrows(
                MapperParsingException.class,
                () -> createMapperService(fieldMapping(b -> b.field("type", "dense_vector")))
            );
            assertThat(e.getMessage(), equalTo("Failed to parse mapping: Missing required parameter [dims] for field [field]"));
        }
    }

    public void testDefaults() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "dense_vector").field("dims", 3)));

        float[] validVector = { -12.1f, 100.7f, -4 };
        double dotProduct = 0.0f;
        for (float value : validVector) {
            dotProduct += value * value;
        }
        float expectedMagnitude = (float) Math.sqrt(dotProduct);
        ParsedDocument doc1 = mapper.parse(source(b -> b.array("field", validVector)));

        List<IndexableField> fields = doc1.rootDoc().getFields("field");
        assertEquals(1, fields.size());
        assertThat(fields.get(0), instanceOf(BinaryDocValuesField.class));
        // assert that after decoding the indexed value is equal to expected
        BytesRef vectorBR = fields.get(0).binaryValue();
        float[] decodedValues = decodeDenseVector(IndexVersion.CURRENT, vectorBR);
        float decodedMagnitude = VectorEncoderDecoder.decodeMagnitude(IndexVersion.CURRENT, vectorBR);
        assertEquals(expectedMagnitude, decodedMagnitude, 0.001f);
        assertArrayEquals("Decoded dense vector values is not equal to the indexed one.", validVector, decodedValues, 0.001f);
    }

    public void testIndexedVector() throws Exception {
        VectorSimilarity similarity = RandomPicks.randomFrom(random(), VectorSimilarity.values());
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "dense_vector").field("dims", 3).field("index", true).field("similarity", similarity))
        );

        float[] vector = { -0.5f, 0.5f, 0.7071f };
        ParsedDocument doc1 = mapper.parse(source(b -> b.array("field", vector)));

        List<IndexableField> fields = doc1.rootDoc().getFields("field");
        assertEquals(1, fields.size());
        assertThat(fields.get(0), instanceOf(XKnnFloatVectorField.class));

        XKnnFloatVectorField vectorField = (XKnnFloatVectorField) fields.get(0);
        assertArrayEquals("Parsed vector is not equal to original.", vector, vectorField.vectorValue(), 0.001f);
        assertEquals(similarity.function, vectorField.fieldType().vectorSimilarityFunction());
    }

    public void testIndexedByteVector() throws Exception {
        VectorSimilarity similarity = RandomPicks.randomFrom(random(), VectorSimilarity.values());
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", 3)
                    .field("index", true)
                    .field("similarity", similarity)
                    .field("element_type", "byte")
            )
        );

        byte[] vector = { (byte) -1, (byte) 1, (byte) 127 };
        ParsedDocument doc1 = mapper.parse(source(b -> b.array("field", vector)));

        List<IndexableField> fields = doc1.rootDoc().getFields("field");
        assertEquals(1, fields.size());
        assertThat(fields.get(0), instanceOf(XKnnByteVectorField.class));

        XKnnByteVectorField vectorField = (XKnnByteVectorField) fields.get(0);
        vectorField.vectorValue();
        assertArrayEquals(
            "Parsed vector is not equal to original.",
            new byte[] { (byte) -1, (byte) 1, (byte) 127 },
            vectorField.vectorValue()
        );
        assertEquals(similarity.function, vectorField.fieldType().vectorSimilarityFunction());
    }

    public void testDotProductWithInvalidNorm() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(
                b -> b.field("type", "dense_vector").field("dims", 3).field("index", true).field("similarity", VectorSimilarity.DOT_PRODUCT)
            )
        );
        float[] vector = { -12.1f, 2.7f, -4 };
        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.array("field", vector)))
        );
        assertNotNull(e.getCause());
        assertThat(
            e.getCause().getMessage(),
            containsString(
                "The [dot_product] similarity can only be used with unit-length vectors. Preview of invalid vector: [-12.1, 2.7, -4.0]"
            )
        );

        DocumentMapper mapperWithLargerDim = createDocumentMapper(
            fieldMapping(
                b -> b.field("type", "dense_vector").field("dims", 6).field("index", true).field("similarity", VectorSimilarity.DOT_PRODUCT)
            )
        );
        float[] largerVector = { -12.1f, 2.7f, -4, 1.05f, 10.0f, 29.9f };
        e = expectThrows(DocumentParsingException.class, () -> mapperWithLargerDim.parse(source(b -> b.array("field", largerVector))));
        assertNotNull(e.getCause());
        assertThat(
            e.getCause().getMessage(),
            containsString(
                "The [dot_product] similarity can only be used with unit-length vectors. "
                    + "Preview of invalid vector: [-12.1, 2.7, -4.0, 1.05, 10.0, ...]"
            )
        );
    }

    public void testCosineWithZeroVector() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(
                b -> b.field("type", "dense_vector").field("dims", 3).field("index", true).field("similarity", VectorSimilarity.COSINE)
            )
        );
        float[] vector = { -0.0f, 0.0f, 0.0f };
        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.array("field", vector)))
        );
        assertNotNull(e.getCause());
        assertThat(
            e.getCause().getMessage(),
            containsString(
                "The [cosine] similarity does not support vectors with zero magnitude. Preview of invalid vector: [-0.0, 0.0, 0.0]"
            )
        );
    }

    public void testCosineWithZeroByteVector() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", 3)
                    .field("index", true)
                    .field("similarity", VectorSimilarity.COSINE)
                    .field("element_type", "byte")
            )
        );
        float[] vector = { -0.0f, 0.0f, 0.0f };
        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.array("field", vector)))
        );
        assertNotNull(e.getCause());
        assertThat(
            e.getCause().getMessage(),
            containsString("The [cosine] similarity does not support vectors with zero magnitude. Preview of invalid vector: [0, 0, 0]")
        );
    }

    public void testInvalidParameters() {
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(fieldMapping(b -> b.field("type", "dense_vector").field("dims", 3).field("index", true)))
        );
        assertThat(e.getMessage(), containsString("Field [index] requires field [similarity] to be configured"));

        e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(fieldMapping(b -> b.field("type", "dense_vector").field("dims", 3).field("similarity", "l2_norm")))
        );
        assertThat(e.getMessage(), containsString("Field [similarity] requires field [index] to be configured"));

        e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(
                fieldMapping(
                    b -> b.field("type", "dense_vector")
                        .field("dims", 3)
                        .startObject("index_options")
                        .field("type", "hnsw")
                        .field("m", 5)
                        .field("ef_construction", 100)
                        .endObject()
                )
            )
        );
        assertThat(e.getMessage(), containsString("Field [index_options] requires field [index] to be configured"));

        e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(
                fieldMapping(
                    b -> b.field("type", "dense_vector")
                        .field("dims", 3)
                        .field("similarity", "l2_norm")
                        .field("index", true)
                        .startObject("index_options")
                        .endObject()
                )
            )
        );
        assertThat(e.getMessage(), containsString("[index_options] requires field [type] to be configured"));

        e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(
                fieldMapping(
                    b -> b.field("type", "dense_vector")
                        .field("dims", 3)
                        .field("similarity", "l2_norm")
                        .field("index", true)
                        .startObject("index_options")
                        .field("type", "hnsw")
                        .field("ef_construction", 100)
                        .endObject()
                )
            )
        );
        assertThat(e.getMessage(), containsString("[index_options] of type [hnsw] requires field [m] to be configured"));

        e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(
                fieldMapping(
                    b -> b.field("type", "dense_vector")
                        .field("dims", 3)
                        .field("similarity", "l2_norm")
                        .field("index", true)
                        .startObject("index_options")
                        .field("type", "hnsw")
                        .field("m", 5)
                        .endObject()
                )
            )
        );
        assertThat(e.getMessage(), containsString("[index_options] of type [hnsw] requires field [ef_construction] to be configured"));

        e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(fieldMapping(b -> b.field("type", "dense_vector").field("dims", 3).field("element_type", "bytes")))
        );
        assertThat(e.getMessage(), containsString("invalid element_type [bytes]; available types are "));
    }

    public void testAddDocumentsToIndexBefore_V_7_5_0() throws Exception {
        IndexVersion indexVersion = IndexVersion.V_7_4_0;
        DocumentMapper mapper = createDocumentMapper(indexVersion, fieldMapping(b -> b.field("type", "dense_vector").field("dims", 3)));

        float[] validVector = { -12.1f, 100.7f, -4 };
        ParsedDocument doc1 = mapper.parse(source(b -> b.array("field", validVector)));
        List<IndexableField> fields = doc1.rootDoc().getFields("field");
        assertEquals(1, fields.size());
        assertThat(fields.get(0), instanceOf(BinaryDocValuesField.class));
        // assert that after decoding the indexed value is equal to expected
        BytesRef vectorBR = fields.get(0).binaryValue();
        float[] decodedValues = decodeDenseVector(indexVersion, vectorBR);
        assertArrayEquals("Decoded dense vector values is not equal to the indexed one.", validVector, decodedValues, 0.001f);
    }

    private static float[] decodeDenseVector(IndexVersion indexVersion, BytesRef encodedVector) {
        int dimCount = VectorEncoderDecoder.denseVectorLength(indexVersion, encodedVector);
        float[] vector = new float[dimCount];
        VectorEncoderDecoder.decodeDenseVector(indexVersion, encodedVector, vector);
        return vector;
    }

    public void testDocumentsWithIncorrectDims() throws Exception {
        for (boolean index : Arrays.asList(false, true)) {
            int dims = 3;
            XContentBuilder fieldMapping = fieldMapping(b -> {
                b.field("type", "dense_vector");
                b.field("dims", dims);
                b.field("index", index);
                if (index) {
                    b.field("similarity", "dot_product");
                }
            });

            DocumentMapper mapper = createDocumentMapper(fieldMapping);

            // test that error is thrown when a document has number of dims more than defined in the mapping
            float[] invalidVector = new float[dims + 1];
            DocumentParsingException e = expectThrows(
                DocumentParsingException.class,
                () -> mapper.parse(source(b -> b.array("field", invalidVector)))
            );
            assertThat(e.getCause().getMessage(), containsString("has more dimensions than defined in the mapping [3]"));

            // test that error is thrown when a document has number of dims less than defined in the mapping
            float[] invalidVector2 = new float[dims - 1];
            DocumentParsingException e2 = expectThrows(
                DocumentParsingException.class,
                () -> mapper.parse(source(b -> b.array("field", invalidVector2)))
            );
            assertThat(
                e2.getCause().getMessage(),
                containsString("has a different number of dimensions [2] than defined in the mapping [3]")
            );
        }
    }

    /**
     * Test that max dimensions limit for float dense_vector field
     * is 2048 as defined by {@link XKnnFloatVectorField}
     */
    public void testMaxDimsFloatVector() throws IOException {
        final int dims = 2048;
        VectorSimilarity similarity = VectorSimilarity.COSINE;
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "dense_vector").field("dims", dims).field("index", true).field("similarity", similarity))
        );

        float[] vector = new float[dims];
        for (int i = 0; i < dims; i++) {
            vector[i] = randomFloat();
        }
        ParsedDocument doc1 = mapper.parse(source(b -> b.array("field", vector)));
        List<IndexableField> fields = doc1.rootDoc().getFields("field");

        assertEquals(1, fields.size());
        assertThat(fields.get(0), instanceOf(XKnnFloatVectorField.class));
        XKnnFloatVectorField vectorField = (XKnnFloatVectorField) fields.get(0);
        assertEquals(dims, vectorField.fieldType().vectorDimension());
        assertEquals(VectorEncoding.FLOAT32, vectorField.fieldType().vectorEncoding());
        assertEquals(similarity.function, vectorField.fieldType().vectorSimilarityFunction());
        assertArrayEquals("Parsed vector is not equal to original.", vector, vectorField.vectorValue(), 0.001f);
    }

    /**
     * Test that max dimensions limit for byte dense_vector field
     * is 2048 as defined by {@link XKnnByteVectorField}
     */
    public void testMaxDimsByteVector() throws IOException {
        final int dims = 2048;
        VectorSimilarity similarity = VectorSimilarity.COSINE;
        ;
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .field("similarity", similarity)
                    .field("element_type", "byte")
            )
        );

        byte[] vector = new byte[dims];
        for (int i = 0; i < dims; i++) {
            vector[i] = randomByte();
        }
        ParsedDocument doc1 = mapper.parse(source(b -> b.array("field", vector)));
        List<IndexableField> fields = doc1.rootDoc().getFields("field");

        assertEquals(1, fields.size());
        assertThat(fields.get(0), instanceOf(XKnnByteVectorField.class));
        XKnnByteVectorField vectorField = (XKnnByteVectorField) fields.get(0);
        assertEquals(dims, vectorField.fieldType().vectorDimension());
        assertEquals(VectorEncoding.BYTE, vectorField.fieldType().vectorEncoding());
        assertEquals(similarity.function, vectorField.fieldType().vectorSimilarityFunction());
        assertArrayEquals("Parsed vector is not equal to original.", vector, vectorField.vectorValue());
    }

    @Override
    protected void assertFetchMany(MapperService mapperService, String field, Object value, String format, int count) throws IOException {
        assumeFalse("Dense vectors currently don't support multiple values in the same field", false);
    }

    /**
     * Dense vectors don't support doc values or string representation (for doc value parser/fetching).
     * We may eventually support that, but until then, we only verify that the parsing and fields fetching matches the provided value object
     */
    @Override
    protected void assertFetch(MapperService mapperService, String field, Object value, String format) throws IOException {
        MappedFieldType ft = mapperService.fieldType(field);
        MappedFieldType.FielddataOperation fdt = MappedFieldType.FielddataOperation.SEARCH;
        SourceToParse source = source(b -> b.field(ft.name(), value));
        SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
        when(searchExecutionContext.isSourceEnabled()).thenReturn(true);
        when(searchExecutionContext.sourcePath(field)).thenReturn(Set.of(field));
        when(searchExecutionContext.getForField(ft, fdt)).thenAnswer(inv -> fieldDataLookup(mapperService).apply(ft, () -> {
            throw new UnsupportedOperationException();
        }, fdt));
        ValueFetcher nativeFetcher = ft.valueFetcher(searchExecutionContext, format);
        ParsedDocument doc = mapperService.documentMapper().parse(source);
        withLuceneIndex(mapperService, iw -> iw.addDocuments(doc.docs()), ir -> {
            Source s = SourceProvider.fromStoredFields().getSource(ir.leaves().get(0), 0);
            nativeFetcher.setNextReader(ir.leaves().get(0));
            List<Object> fromNative = nativeFetcher.fetchValues(s, 0, new ArrayList<>());
            DenseVectorFieldType denseVectorFieldType = (DenseVectorFieldType) ft;
            switch (denseVectorFieldType.getElementType()) {
                case BYTE -> {
                    assumeFalse("byte element type testing not currently added", false);
                }
                case FLOAT -> {
                    float[] fetchedFloats = new float[denseVectorFieldType.getVectorDimensions()];
                    int i = 0;
                    for (var f : fromNative) {
                        assert f instanceof Number;
                        fetchedFloats[i++] = ((Number) f).floatValue();
                    }
                    assertThat("fetching " + value, fetchedFloats, equalTo(value));
                }
            }
        });
    }

    @Override
    // TODO: add `byte` element_type tests
    protected void randomFetchTestFieldConfig(XContentBuilder b) throws IOException {
        b.field("type", "dense_vector").field("dims", randomIntBetween(2, 2048)).field("element_type", "float");
        if (randomBoolean()) {
            b.field("index", true).field("similarity", randomFrom(VectorSimilarity.values()).toString());
        }
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        DenseVectorFieldType vectorFieldType = (DenseVectorFieldType) ft;
        return switch (vectorFieldType.getElementType()) {
            case BYTE -> randomByteArrayOfLength(vectorFieldType.getVectorDimensions());
            case FLOAT -> {
                float[] floats = new float[vectorFieldType.getVectorDimensions()];
                float magnitude = 0;
                for (int i = 0; i < floats.length; i++) {
                    float f = randomFloat();
                    floats[i] = f;
                    magnitude += f * f;
                }
                magnitude = (float) Math.sqrt(magnitude);
                if (VectorSimilarity.DOT_PRODUCT.equals(vectorFieldType.getSimilarity())) {
                    for (int i = 0; i < floats.length; i++) {
                        floats[i] /= magnitude;
                    }
                }
                yield floats;
            }
        };
    }

    @Override
    protected boolean allowsNullValues() {
        return true;
    }

    public void testCannotBeUsedInMultifields() {
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "keyword");
            b.startObject("fields");
            b.startObject("vectors");
            minimalMapping(b);
            b.endObject();
            b.endObject();
        })));
        assertThat(e.getMessage(), containsString("Field [vectors] of type [dense_vector] can't be used in multifields"));
    }

    public void testNestedVectorsCannotBeIndexed() {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> createMapperService(
                fieldMapping(
                    b -> b.field("type", "nested")
                        .startObject("properties")
                        .startObject("vector")
                        .field("type", "dense_vector")
                        .field("dims", 4)
                        .field("index", true)
                        .field("similarity", "dot_product")
                        .endObject()
                        .endObject()
                )
            )
        );
        assertThat(e.getMessage(), containsString("[dense_vector] fields cannot be indexed if they're within [nested] mappings"));
    }

    public void testByteVectorIndexBoundaries() throws IOException {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("element_type", "byte")
                    .field("dims", 3)
                    .field("index", true)
                    .field("similarity", VectorSimilarity.COSINE)
            )
        );

        Exception e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.array("field", new float[] { 128, 0, 0 })))
        );
        assertThat(
            e.getCause().getMessage(),
            containsString("element_type [byte] vectors only support integers between [-128, 127] but found [128] at dim [0];")
        );

        e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> b.array("field", new float[] { 18.2f, 0, 0 }))));
        assertThat(
            e.getCause().getMessage(),
            containsString("element_type [byte] vectors only support non-decimal values but found decimal value [18.2] at dim [0];")
        );

        e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.array("field", new float[] { 0.0f, 0.0f, -129.0f })))
        );
        assertThat(
            e.getCause().getMessage(),
            containsString("element_type [byte] vectors only support integers between [-128, 127] but found [-129] at dim [2];")
        );
    }

    public void testByteVectorQueryBoundaries() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "dense_vector");
            b.field("element_type", "byte");
            b.field("dims", 3);
            b.field("index", true);
            b.field("similarity", "dot_product");
            b.startObject("index_options");
            b.field("type", "hnsw");
            b.field("m", 3);
            b.field("ef_construction", 10);
            b.endObject();
        }));

        DenseVectorFieldType denseVectorFieldType = (DenseVectorFieldType) mapperService.fieldType("field");

        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> denseVectorFieldType.createKnnQuery(new float[] { 128, 0, 0 }, 3, null, null)
        );
        assertThat(
            e.getMessage(),
            containsString("element_type [byte] vectors only support integers between [-128, 127] but found [128.0] at dim [0];")
        );

        e = expectThrows(
            IllegalArgumentException.class,
            () -> denseVectorFieldType.createKnnQuery(new float[] { 0.0f, 0f, -129.0f }, 3, null, null)
        );
        assertThat(
            e.getMessage(),
            containsString("element_type [byte] vectors only support integers between [-128, 127] but found [-129.0] at dim [2];")
        );

        e = expectThrows(
            IllegalArgumentException.class,
            () -> denseVectorFieldType.createKnnQuery(new float[] { 0.0f, 0.5f, 0.0f }, 3, null, null)
        );
        assertThat(
            e.getMessage(),
            containsString("element_type [byte] vectors only support non-decimal values but found decimal value [0.5] at dim [1];")
        );

        e = expectThrows(
            IllegalArgumentException.class,
            () -> denseVectorFieldType.createKnnQuery(new float[] { 0, 0.0f, -0.25f }, 3, null, null)
        );
        assertThat(
            e.getMessage(),
            containsString("element_type [byte] vectors only support non-decimal values but found decimal value [-0.25] at dim [2];")
        );

        e = expectThrows(
            IllegalArgumentException.class,
            () -> denseVectorFieldType.createKnnQuery(new float[] { Float.NaN, 0f, 0.0f }, 3, null, null)
        );
        assertThat(e.getMessage(), containsString("element_type [byte] vectors do not support NaN values but found [NaN] at dim [0];"));

        e = expectThrows(
            IllegalArgumentException.class,
            () -> denseVectorFieldType.createKnnQuery(new float[] { Float.POSITIVE_INFINITY, 0f, 0.0f }, 3, null, null)
        );
        assertThat(
            e.getMessage(),
            containsString("element_type [byte] vectors do not support infinite values but found [Infinity] at dim [0];")
        );

        e = expectThrows(
            IllegalArgumentException.class,
            () -> denseVectorFieldType.createKnnQuery(new float[] { 0, Float.NEGATIVE_INFINITY, 0.0f }, 3, null, null)
        );
        assertThat(
            e.getMessage(),
            containsString("element_type [byte] vectors do not support infinite values but found [-Infinity] at dim [1];")
        );
    }

    public void testFloatVectorQueryBoundaries() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "dense_vector");
            b.field("element_type", "float");
            b.field("dims", 3);
            b.field("index", true);
            b.field("similarity", "dot_product");
            b.startObject("index_options");
            b.field("type", "hnsw");
            b.field("m", 3);
            b.field("ef_construction", 10);
            b.endObject();
        }));

        DenseVectorFieldType denseVectorFieldType = (DenseVectorFieldType) mapperService.fieldType("field");

        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> denseVectorFieldType.createKnnQuery(new float[] { Float.NaN, 0f, 0.0f }, 3, null, null)
        );
        assertThat(e.getMessage(), containsString("element_type [float] vectors do not support NaN values but found [NaN] at dim [0];"));

        e = expectThrows(
            IllegalArgumentException.class,
            () -> denseVectorFieldType.createKnnQuery(new float[] { Float.POSITIVE_INFINITY, 0f, 0.0f }, 3, null, null)
        );
        assertThat(
            e.getMessage(),
            containsString("element_type [float] vectors do not support infinite values but found [Infinity] at dim [0];")
        );

        e = expectThrows(
            IllegalArgumentException.class,
            () -> denseVectorFieldType.createKnnQuery(new float[] { 0, Float.NEGATIVE_INFINITY, 0.0f }, 3, null, null)
        );
        assertThat(
            e.getMessage(),
            containsString("element_type [float] vectors do not support infinite values but found [-Infinity] at dim [1];")
        );
    }

    public void testKnnVectorsFormat() throws IOException {
        final int m = randomIntBetween(1, DEFAULT_MAX_CONN + 10);
        final int efConstruction = randomIntBetween(1, DEFAULT_BEAM_WIDTH + 10);
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "dense_vector");
            b.field("dims", 4);
            b.field("index", true);
            b.field("similarity", "dot_product");
            b.startObject("index_options");
            b.field("type", "hnsw");
            b.field("m", m);
            b.field("ef_construction", efConstruction);
            b.endObject();
        }));
        CodecService codecService = new CodecService(mapperService, BigArrays.NON_RECYCLING_INSTANCE);
        Codec codec = codecService.codec("default");
        assertThat(codec, instanceOf(PerFieldMapperCodec.class));
        KnnVectorsFormat knnVectorsFormat = ((PerFieldMapperCodec) codec).getKnnVectorsFormatForField("field");
        assertThat(knnVectorsFormat, instanceOf(Lucene95HnswVectorsFormat.class));
        String expectedString = "Lucene95HnswVectorsFormat(name=Lucene95HnswVectorsFormat, maxConn="
            + m
            + ", beamWidth="
            + efConstruction
            + ")";
        assertEquals(expectedString, knnVectorsFormat.toString());
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        return new DenseVectorSyntheticSourceSupport();
    }

    @Override
    protected boolean supportsEmptyInputArray() {
        return false;
    }

    private static class DenseVectorSyntheticSourceSupport implements SyntheticSourceSupport {
        private final int dims = between(5, 1000);
        private final ElementType elementType = randomFrom(ElementType.BYTE, ElementType.FLOAT);
        private final boolean indexed = randomBoolean();
        private final boolean indexOptionsSet = indexed && randomBoolean();

        @Override
        public SyntheticSourceExample example(int maxValues) throws IOException {
            Object value = elementType == ElementType.BYTE
                ? randomList(dims, dims, ESTestCase::randomByte)
                : randomList(dims, dims, ESTestCase::randomFloat);
            return new SyntheticSourceExample(value, value, this::mapping);
        }

        private void mapping(XContentBuilder b) throws IOException {
            b.field("type", "dense_vector");
            b.field("dims", dims);
            if (elementType == ElementType.BYTE || randomBoolean()) {
                b.field("element_type", elementType.toString());
            }
            if (indexed) {
                b.field("index", true);
                b.field("similarity", "l2_norm");
                if (indexOptionsSet) {
                    b.startObject("index_options");
                    b.field("type", "hnsw");
                    b.field("m", 5);
                    b.field("ef_construction", 50);
                    b.endObject();
                }
            }
        }

        @Override
        public List<SyntheticSourceInvalidExample> invalidExample() {
            return List.of();
        }
    }
}
