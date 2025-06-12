/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors.mapper;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexVersion;
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
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.lookup.SourceProvider;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.rank.vectors.LocalStateRankVectors;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RankVectorsFieldMapperTests extends MapperTestCase {

    private final ElementType elementType;
    private final int dims;

    public RankVectorsFieldMapperTests() {
        this.elementType = randomFrom(ElementType.BYTE, ElementType.FLOAT, ElementType.BIT);
        this.dims = ElementType.BIT == elementType ? 4 * Byte.SIZE : 4;
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return Collections.singletonList(new LocalStateRankVectors(SETTINGS));
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        indexMapping(b, IndexVersion.current());
    }

    @Override
    protected void minimalMapping(XContentBuilder b, IndexVersion indexVersion) throws IOException {
        indexMapping(b, indexVersion);
    }

    private void indexMapping(XContentBuilder b, IndexVersion indexVersion) throws IOException {
        b.field("type", "rank_vectors").field("dims", dims);
        if (elementType != ElementType.FLOAT) {
            b.field("element_type", elementType.toString());
        }
    }

    @Override
    protected Object getSampleValueForDocument() {
        int numVectors = randomIntBetween(1, 16);
        return Stream.generate(
            () -> elementType == ElementType.FLOAT ? List.of(0.5, 0.5, 0.5, 0.5) : List.of((byte) 1, (byte) 1, (byte) 1, (byte) 1)
        ).limit(numVectors).toList();
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck(
            "dims",
            fieldMapping(b -> b.field("type", "rank_vectors").field("dims", dims)),
            fieldMapping(b -> b.field("type", "rank_vectors").field("dims", dims + 8))
        );
        checker.registerConflictCheck(
            "element_type",
            fieldMapping(b -> b.field("type", "rank_vectors").field("dims", dims).field("element_type", "byte")),
            fieldMapping(b -> b.field("type", "rank_vectors").field("dims", dims).field("element_type", "float"))
        );
        checker.registerConflictCheck(
            "element_type",
            fieldMapping(b -> b.field("type", "rank_vectors").field("dims", dims).field("element_type", "float")),
            fieldMapping(b -> b.field("type", "rank_vectors").field("dims", dims * 8).field("element_type", "bit"))
        );
        checker.registerConflictCheck(
            "element_type",
            fieldMapping(b -> b.field("type", "rank_vectors").field("dims", dims).field("element_type", "byte")),
            fieldMapping(b -> b.field("type", "rank_vectors").field("dims", dims * 8).field("element_type", "bit"))
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
        assertThat(fieldType, instanceOf(RankVectorsFieldMapper.RankVectorsFieldType.class));
        assertFalse(fieldType.isIndexed());
        assertFalse(fieldType.isSearchable());
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
                b.field("type", "rank_vectors");
                b.field("dims", 0);
            })));
            assertThat(
                e.getMessage(),
                equalTo("Failed to parse mapping: " + "The number of dimensions should be in the range [1, 4096] but was [0]")
            );
        }
        // test max limit for non-indexed vectors
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
                b.field("type", "rank_vectors");
                b.field("dims", 5000);
            })));
            assertThat(
                e.getMessage(),
                equalTo("Failed to parse mapping: " + "The number of dimensions should be in the range [1, 4096] but was [5000]")
            );
        }
    }

    public void testMergeDims() throws IOException {
        XContentBuilder mapping = mapping(b -> {
            b.startObject("field");
            b.field("type", "rank_vectors");
            b.endObject();
        });
        MapperService mapperService = createMapperService(mapping);

        mapping = mapping(b -> {
            b.startObject("field");
            b.field("type", "rank_vectors").field("dims", dims);
            b.endObject();
        });
        merge(mapperService, mapping);
        assertEquals(
            XContentHelper.convertToMap(BytesReference.bytes(mapping), false, mapping.contentType()).v2(),
            XContentHelper.convertToMap(mapperService.documentMapper().mappingSource().uncompressed(), false, mapping.contentType()).v2()
        );
    }

    public void testLargeDimsBit() throws IOException {
        createMapperService(fieldMapping(b -> {
            b.field("type", "rank_vectors");
            b.field("dims", 1024 * Byte.SIZE);
            b.field("element_type", ElementType.BIT.toString());
        }));
    }

    public void testNonIndexedVector() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "rank_vectors").field("dims", 3)));

        float[][] validVectors = { { -12.1f, 100.7f, -4 }, { 42f, .05f, -1f } };
        double[] dotProduct = new double[2];
        int vecId = 0;
        for (float[] vector : validVectors) {
            for (float value : vector) {
                dotProduct[vecId] += value * value;
            }
            vecId++;
        }
        ParsedDocument doc1 = mapper.parse(source(b -> {
            b.startArray("field");
            for (float[] vector : validVectors) {
                b.startArray();
                for (float value : vector) {
                    b.value(value);
                }
                b.endArray();
            }
            b.endArray();
        }));

        List<IndexableField> fields = doc1.rootDoc().getFields("field");
        assertEquals(1, fields.size());
        assertThat(fields.get(0), instanceOf(BinaryDocValuesField.class));
        // assert that after decoding the indexed value is equal to expected
        BytesRef vectorBR = fields.get(0).binaryValue();
        assertEquals(ElementType.FLOAT.getNumBytes(validVectors[0].length) * validVectors.length, vectorBR.length);
        float[][] decodedValues = new float[validVectors.length][];
        for (int i = 0; i < validVectors.length; i++) {
            decodedValues[i] = new float[validVectors[i].length];
            FloatBuffer fb = ByteBuffer.wrap(vectorBR.bytes, i * Float.BYTES * validVectors[i].length, Float.BYTES * validVectors[i].length)
                .order(ByteOrder.LITTLE_ENDIAN)
                .asFloatBuffer();
            fb.get(decodedValues[i]);
        }
        List<IndexableField> magFields = doc1.rootDoc().getFields("field" + RankVectorsFieldMapper.VECTOR_MAGNITUDES_SUFFIX);
        assertEquals(1, magFields.size());
        assertThat(magFields.get(0), instanceOf(BinaryDocValuesField.class));
        BytesRef magBR = magFields.get(0).binaryValue();
        assertEquals(Float.BYTES * validVectors.length, magBR.length);
        FloatBuffer fb = ByteBuffer.wrap(magBR.bytes, magBR.offset, magBR.length).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer();
        for (int i = 0; i < validVectors.length; i++) {
            assertEquals((float) Math.sqrt(dotProduct[i]), fb.get(), 0.001f);
        }
        for (int i = 0; i < validVectors.length; i++) {
            assertArrayEquals("Decoded dense vector values is not equal to the indexed one.", validVectors[i], decodedValues[i], 0.001f);
        }
    }

    public void testPoorlyIndexedVector() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "rank_vectors").field("dims", 3)));

        float[][] validVectors = { { -12.1f, 100.7f, -4 }, { 42f, .05f, -1f } };
        double[] dotProduct = new double[2];
        int vecId = 0;
        for (float[] vector : validVectors) {
            for (float value : vector) {
                dotProduct[vecId] += value * value;
            }
            vecId++;
        }
        expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> {
            b.startArray("field");
            b.startArray(); // double nested array should fail
            for (float[] vector : validVectors) {
                b.startArray();
                for (float value : vector) {
                    b.value(value);
                }
                b.endArray();
            }
            b.endArray();
            b.endArray();
        })));
    }

    public void testInvalidParameters() {

        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(fieldMapping(b -> b.field("type", "rank_vectors").field("dims", 3).field("element_type", "foo")))
        );
        assertThat(e.getMessage(), containsString("invalid element_type [foo]; available types are "));
        e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(fieldMapping(b -> b.field("type", "rank_vectors").field("dims", 3).startObject("foo").endObject()))
        );
        assertThat(
            e.getMessage(),
            containsString("Failed to parse mapping: unknown parameter [foo] on mapper [field] of type [rank_vectors]")
        );
    }

    public void testDocumentsWithIncorrectDims() throws Exception {
        int dims = 3;
        XContentBuilder fieldMapping = fieldMapping(b -> {
            b.field("type", "rank_vectors");
            b.field("dims", dims);
        });

        DocumentMapper mapper = createDocumentMapper(fieldMapping);

        // test that error is thrown when a document has number of dims more than defined in the mapping
        float[][] invalidVector = new float[4][dims + 1];
        DocumentParsingException e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> {
            b.startArray("field");
            for (float[] vector : invalidVector) {
                b.startArray();
                for (float value : vector) {
                    b.value(value);
                }
                b.endArray();
            }
            b.endArray();
        })));
        assertThat(e.getCause().getMessage(), containsString("has more dimensions than defined in the mapping [3]"));

        // test that error is thrown when a document has number of dims less than defined in the mapping
        float[][] invalidVector2 = new float[4][dims - 1];
        DocumentParsingException e2 = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> {
            b.startArray("field");
            for (float[] vector : invalidVector2) {
                b.startArray();
                for (float value : vector) {
                    b.value(value);
                }
                b.endArray();
            }
            b.endArray();
        })));
        assertThat(e2.getCause().getMessage(), containsString("has a different number of dimensions [2] than defined in the mapping [3]"));
        // test that error is thrown when some of the vectors have correct number of dims, but others do not
        DocumentParsingException e3 = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> {
            b.startArray("field");
            for (float[] vector : new float[4][dims]) {
                b.startArray();
                for (float value : vector) {
                    b.value(value);
                }
                b.endArray();
            }
            for (float[] vector : invalidVector2) {
                b.startArray();
                for (float value : vector) {
                    b.value(value);
                }
                b.endArray();
            }
            b.endArray();
        })));
        assertThat(e3.getCause().getMessage(), containsString("has a different number of dimensions [2] than defined in the mapping [3]"));
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
            Source s = SourceProvider.fromLookup(mapperService.mappingLookup(), null, mapperService.getMapperMetrics().sourceFieldMetrics())
                .getSource(ir.leaves().get(0), 0);
            nativeFetcher.setNextReader(ir.leaves().get(0));
            List<Object> fromNative = nativeFetcher.fetchValues(s, 0, new ArrayList<>());
            RankVectorsFieldMapper.RankVectorsFieldType denseVectorFieldType = (RankVectorsFieldMapper.RankVectorsFieldType) ft;
            switch (denseVectorFieldType.getElementType()) {
                case BYTE -> assumeFalse("byte element type testing not currently added", false);
                case FLOAT -> {
                    float[][] fetchedFloats = new float[fromNative.size()][];
                    for (int i = 0; i < fromNative.size(); i++) {
                        fetchedFloats[i] = (float[]) fromNative.get(i);
                    }
                    assertThat("fetching " + value, fetchedFloats, equalTo(value));
                }
            }
        });
    }

    @Override
    protected void randomFetchTestFieldConfig(XContentBuilder b) throws IOException {
        b.field("type", "rank_vectors").field("dims", randomIntBetween(2, 4096)).field("element_type", "float");
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        RankVectorsFieldMapper.RankVectorsFieldType vectorFieldType = (RankVectorsFieldMapper.RankVectorsFieldType) ft;
        int numVectors = randomIntBetween(1, 16);
        return switch (vectorFieldType.getElementType()) {
            case BYTE -> {
                byte[][] vectors = new byte[numVectors][vectorFieldType.getVectorDimensions()];
                for (int i = 0; i < numVectors; i++) {
                    vectors[i] = randomByteArrayOfLength(vectorFieldType.getVectorDimensions());
                }
                yield vectors;
            }
            case FLOAT -> {
                float[][] vectors = new float[numVectors][vectorFieldType.getVectorDimensions()];
                for (int i = 0; i < numVectors; i++) {
                    for (int j = 0; j < vectorFieldType.getVectorDimensions(); j++) {
                        vectors[i][j] = randomFloat();
                    }
                }
                yield vectors;
            }
            case BIT -> {
                byte[][] vectors = new byte[numVectors][vectorFieldType.getVectorDimensions() / 8];
                for (int i = 0; i < numVectors; i++) {
                    vectors[i] = randomByteArrayOfLength(vectorFieldType.getVectorDimensions() / 8);
                }
                yield vectors;
            }
        };
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
        assertThat(e.getMessage(), containsString("Field [vectors] of type [rank_vectors] can't be used in multifields"));
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
        private final int numVecs = between(1, 16);
        private final ElementType elementType = randomFrom(ElementType.BYTE, ElementType.FLOAT, ElementType.BIT);

        @Override
        public SyntheticSourceExample example(int maxValues) {
            Object value = switch (elementType) {
                case BYTE, BIT:
                    yield randomList(numVecs, numVecs, () -> randomList(dims, dims, ESTestCase::randomByte));
                case FLOAT:
                    yield randomList(numVecs, numVecs, () -> randomList(dims, dims, ESTestCase::randomFloat));
            };
            return new SyntheticSourceExample(value, value, this::mapping);
        }

        private void mapping(XContentBuilder b) throws IOException {
            b.field("type", "rank_vectors");
            if (elementType == ElementType.BYTE || elementType == ElementType.BIT || randomBoolean()) {
                b.field("element_type", elementType.toString());
            }
            b.field("dims", elementType == ElementType.BIT ? dims * Byte.SIZE : dims);
        }

        @Override
        public List<SyntheticSourceInvalidExample> invalidExample() {
            return List.of();
        }
    }

    @Override
    public void testSyntheticSourceKeepArrays() {
        // The mapper expects to parse an array of values by default, it's not compatible with array of arrays.
    }
}
