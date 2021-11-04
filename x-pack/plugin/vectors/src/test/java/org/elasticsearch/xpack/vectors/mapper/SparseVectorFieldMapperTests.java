/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectors.mapper;

import org.apache.logging.log4j.Level;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.vectors.DenseVectorPlugin;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

@SuppressWarnings("deprecation")
public class SparseVectorFieldMapperTests extends MapperTestCase {

    @Override
    protected String[] getParseMinimalWarnings() {
        return new String[] { "The [sparse_vector] field type is deprecated and will be removed in 8.0." };
    }

    @Override
    protected String[] getParseMaximalWarnings() {
        return getParseMinimalWarnings();
    }

    @Override
    protected void registerParameters(ParameterChecker checker) {
        // no parameters to check
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "sparse_vector");
    }

    @Override
    protected boolean supportsStoredFields() {
        return false;
    }

    @Override
    protected Object getSampleValueForDocument() {
        return Collections.singletonMap("1", 1);
    }

    @Override
    protected Collection<Plugin> getPlugins() {
        return Collections.singletonList(new DenseVectorPlugin());
    }

    public void testDefaults() throws Exception {
        Version indexVersion = Version.CURRENT;
        int[] indexedDims = { 65535, 50, 2 };
        float[] indexedValues = { 0.5f, 1800f, -34567.11f };
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc1 = mapper.parse(source(b -> {
            b.startObject("field");
            b.field(Integer.toString(indexedDims[0]), indexedValues[0]);
            b.field(Integer.toString(indexedDims[1]), indexedValues[1]);
            b.field(Integer.toString(indexedDims[2]), indexedValues[2]);
            b.endObject();
        }));
        IndexableField[] fields = doc1.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        assertThat(fields[0], Matchers.instanceOf(BinaryDocValuesField.class));

        // assert that after decoding the indexed values are equal to expected
        int[] expectedDims = { 2, 50, 65535 }; // the same as indexed but sorted
        float[] expectedValues = { -34567.11f, 1800f, 0.5f }; // the same as indexed but sorted by their dimensions
        double dotProduct = 0.0f;
        for (float value : expectedValues) {
            dotProduct += value * value;
        }
        float expectedMagnitude = (float) Math.sqrt(dotProduct);

        // assert that after decoded magnitude, dims and values are equal to expected
        BytesRef vectorBR = fields[0].binaryValue();
        int[] decodedDims = VectorEncoderDecoder.decodeSparseVectorDims(indexVersion, vectorBR);
        assertArrayEquals("Decoded sparse vector dimensions are not equal to the indexed ones.", expectedDims, decodedDims);
        float[] decodedValues = VectorEncoderDecoder.decodeSparseVector(indexVersion, vectorBR);
        assertArrayEquals("Decoded sparse vector values are not equal to the indexed ones.", expectedValues, decodedValues, 0.001f);
        float decodedMagnitude = VectorEncoderDecoder.decodeMagnitude(indexVersion, vectorBR);
        assertEquals(expectedMagnitude, decodedMagnitude, 0.001f);

        assertWarnings(Level.WARN, SparseVectorFieldMapper.DEPRECATION_MESSAGE);
    }

    public void testAddDocumentsToIndexBefore_V_7_5_0() throws Exception {
        Version indexVersion = Version.V_7_4_0;
        DocumentMapper mapper = createDocumentMapper(indexVersion, fieldMapping(this::minimalMapping));

        int[] indexedDims = { 65535, 50, 2 };
        float[] indexedValues = { 0.5f, 1800f, -34567.11f };
        ParsedDocument doc1 = mapper.parse(source(b -> {
            b.startObject("field");
            b.field(Integer.toString(indexedDims[0]), indexedValues[0]);
            b.field(Integer.toString(indexedDims[1]), indexedValues[1]);
            b.field(Integer.toString(indexedDims[2]), indexedValues[2]);
            b.endObject();
        }));
        IndexableField[] fields = doc1.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        assertThat(fields[0], Matchers.instanceOf(BinaryDocValuesField.class));

        // assert that after decoding the indexed values are equal to expected
        int[] expectedDims = { 2, 50, 65535 }; // the same as indexed but sorted
        float[] expectedValues = { -34567.11f, 1800f, 0.5f }; // the same as indexed but sorted by their dimensions

        // assert that after decoded magnitude, dims and values are equal to expected
        BytesRef vectorBR = fields[0].binaryValue();
        int[] decodedDims = VectorEncoderDecoder.decodeSparseVectorDims(indexVersion, vectorBR);
        assertArrayEquals("Decoded sparse vector dimensions are not equal to the indexed ones.", expectedDims, decodedDims);
        float[] decodedValues = VectorEncoderDecoder.decodeSparseVector(indexVersion, vectorBR);
        assertArrayEquals("Decoded sparse vector values are not equal to the indexed ones.", expectedValues, decodedValues, 0.001f);

        assertWarnings(Level.WARN, SparseVectorFieldMapper.DEPRECATION_MESSAGE);
    }

    public void testDimensionNumberValidation() throws IOException {
        // 1. test for an error on negative dimension
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> {
            mapper.parse(source(b -> {
                b.startObject("field");
                b.field("-50", 100f);
                b.endObject();
            }));
        });
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(
            e.getCause().getMessage(),
            containsString("dimension number must be a non-negative integer value not exceeding [65535], got [-50]")
        );

        // 2. test for an error on a dimension greater than MAX_DIMS_NUMBER
        e = expectThrows(MapperParsingException.class, () -> {
            mapper.parse(source(b -> {
                b.startObject("field");
                b.field("70000", 100f);
                b.endObject();
            }));
        });
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(
            e.getCause().getMessage(),
            containsString("dimension number must be a non-negative integer value not exceeding [65535], got [70000]")
        );

        // 3. test for an error on a wrong formatted dimension
        e = expectThrows(MapperParsingException.class, () -> {
            mapper.parse(source(b -> {
                b.startObject("field");
                b.field("WrongDim123", 100f);
                b.endObject();
            }));
        });
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(
            e.getCause().getMessage(),
            containsString("dimensions should be integers represented as strings, but got [WrongDim123]")
        );

        // 4. test for an error on a wrong format for the map of dims to values
        e = expectThrows(MapperParsingException.class, () -> {
            mapper.parse(source(b -> {
                b.startObject("field");
                b.startArray("10").value(10f).value(100f).endArray();
                b.endObject();
            }));
        });
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(
            e.getCause().getMessage(),
            containsString("takes an object that maps a dimension number to a float, but got unexpected token [START_ARRAY]")
        );

        assertWarnings(Level.WARN, SparseVectorFieldMapper.DEPRECATION_MESSAGE);
    }

    public void testDimensionLimit() throws IOException {
        Map<String, Object> validVector = IntStream.range(0, SparseVectorFieldMapper.MAX_DIMS_COUNT)
            .boxed()
            .collect(Collectors.toMap(String::valueOf, Function.identity()));

        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        mapper.parse(source(b -> b.field("field", validVector)));

        Map<String, Object> invalidVector = IntStream.range(0, SparseVectorFieldMapper.MAX_DIMS_COUNT + 1)
            .boxed()
            .collect(Collectors.toMap(String::valueOf, Function.identity()));

        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(source(b -> b.field("field", invalidVector)))
        );
        assertThat(e.getDetailedMessage(), containsString("has exceeded the maximum allowed number of dimensions"));

        assertWarnings(Level.WARN, SparseVectorFieldMapper.DEPRECATION_MESSAGE);
    }

    @Override
    public void testUpdates() throws IOException {
        // no updates to test
    }

    @Override
    public void testMeta() throws IOException {
        super.testMeta();
        assertParseMinimalWarnings();
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        assumeFalse("doesn't support docvalues_fetcher", true);
        return null;
    }

    @Override
    protected boolean allowsNullValues() {
        return false;
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
        assertThat(e.getMessage(), containsString("Field [vectors] of type [sparse_vector] can't be used in multifields"));
    }

    protected Level getWarningLevel() {
        return Level.WARN;
    }
}
