/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectors.mapper;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.vectors.Vectors;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class DenseVectorFieldMapperTests extends MapperTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new Vectors());
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "dense_vector").field("dims", 4);
    }

    @Override
    protected Object getSampleValueForDocument() {
        return List.of(1, 2, 3, 4);
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("dims",
            fieldMapping(b -> b.field("type", "dense_vector").field("dims", 4)),
            fieldMapping(b -> b.field("type", "dense_vector").field("dims", 5)));
    }

    @Override
    protected boolean supportsStoredFields() {
        return false;
    }

    public void testDims() {
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
                b.field("type", "dense_vector");
                b.field("dims", 0);
            })));
            assertThat(e.getMessage(), equalTo("Failed to parse mapping: " +
                "The number of dimensions for field [field] should be in the range [1, 2048] but was [0]"));
        }
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
                b.field("type", "dense_vector");
                b.field("dims", 3000);
            })));
            assertThat(e.getMessage(), equalTo("Failed to parse mapping: " +
                "The number of dimensions for field [field] should be in the range [1, 2048] but was [3000]"));
        }
        {
            Exception e = expectThrows(MapperParsingException.class,
                () -> createMapperService(fieldMapping(b -> b.field("type", "dense_vector"))));
            assertThat(e.getMessage(), equalTo("Failed to parse mapping: Missing required parameter [dims] for field [field]"));
        }
    }

    public void testDefaults() throws Exception {

        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "dense_vector").field("dims", 3)));

        float[] validVector = {-12.1f, 100.7f, -4};
        double dotProduct = 0.0f;
        for (float value: validVector) {
            dotProduct += value * value;
        }
        float expectedMagnitude = (float) Math.sqrt(dotProduct);
        ParsedDocument doc1 = mapper.parse(source(b -> b.array("field", validVector)));

        IndexableField[] fields = doc1.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        assertThat(fields[0], instanceOf(BinaryDocValuesField.class));
        // assert that after decoding the indexed value is equal to expected
        BytesRef vectorBR = fields[0].binaryValue();
        float[] decodedValues = decodeDenseVector(Version.CURRENT, vectorBR);
        float decodedMagnitude = VectorEncoderDecoder.decodeMagnitude(Version.CURRENT, vectorBR);
        assertEquals(expectedMagnitude, decodedMagnitude, 0.001f);
        assertArrayEquals(
            "Decoded dense vector values is not equal to the indexed one.",
            validVector,
            decodedValues,
            0.001f
        );
    }

    public void testAddDocumentsToIndexBefore_V_7_5_0() throws Exception {
        Version indexVersion = Version.V_7_4_0;
        DocumentMapper mapper
            = createDocumentMapper(indexVersion, fieldMapping(b -> b.field("type", "dense_vector").field("dims", 3)));

        float[] validVector = {-12.1f, 100.7f, -4};
        ParsedDocument doc1 = mapper.parse(source(b -> b.array("field", validVector)));
        IndexableField[] fields = doc1.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        assertThat(fields[0], instanceOf(BinaryDocValuesField.class));
        // assert that after decoding the indexed value is equal to expected
        BytesRef vectorBR = fields[0].binaryValue();
        float[] decodedValues = decodeDenseVector(indexVersion, vectorBR);
        assertArrayEquals(
            "Decoded dense vector values is not equal to the indexed one.",
            validVector,
            decodedValues,
            0.001f
        );
    }

    private static float[] decodeDenseVector(Version indexVersion, BytesRef encodedVector) {
        int dimCount = VectorEncoderDecoder.denseVectorLength(indexVersion, encodedVector);
        float[] vector = new float[dimCount];

        ByteBuffer byteBuffer = ByteBuffer.wrap(encodedVector.bytes, encodedVector.offset, encodedVector.length);
        for (int dim = 0; dim < dimCount; dim++) {
            vector[dim] = byteBuffer.getFloat();
        }
        return vector;
    }

    public void testDocumentsWithIncorrectDims() throws Exception {

        int dims = 3;
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "dense_vector").field("dims", dims)));

        // test that error is thrown when a document has number of dims more than defined in the mapping
        float[] invalidVector = new float[dims + 1];
        MapperParsingException e = expectThrows(MapperParsingException.class,
            () -> mapper.parse(source(b -> b.array("field", invalidVector))));
        assertThat(e.getCause().getMessage(), containsString("has exceeded the number of dimensions [3] defined in mapping"));

        // test that error is thrown when a document has number of dims less than defined in the mapping
        float[] invalidVector2 = new float[dims - 1];
        MapperParsingException e2 = expectThrows(MapperParsingException.class,
            () -> mapper.parse(source(b -> b.array("field", invalidVector2))));
        assertThat(e2.getCause().getMessage(), containsString("has number of dimensions [2] less than defined in the mapping [3]"));
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        assumeFalse("Test implemented in a follow up", true);
        return null;
    }

    @Override
    protected boolean allowsNullValues() {
        return false;       // TODO should this allow null values?
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
}
