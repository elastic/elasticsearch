/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugins.Plugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.containsString;

public class ScaledFloatFieldMapperTests extends MapperTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return singletonList(new MapperExtrasPlugin());
    }

    @Override
    protected Object getSampleValueForDocument() {
        return 123;
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "scaled_float").field("scaling_factor", 10.0);
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck(
            "scaling_factor",
            fieldMapping(this::minimalMapping),
            fieldMapping(b -> {
                b.field("type", "scaled_float");
                b.field("scaling_factor", 5.0);
            }));
        checker.registerConflictCheck("doc_values", b -> b.field("doc_values", false));
        checker.registerConflictCheck("index", b -> b.field("index", false));
        checker.registerConflictCheck("store", b -> b.field("store", true));
        checker.registerConflictCheck("null_value", b -> b.field("null_value", 1));
        checker.registerUpdateCheck(b -> b.field("coerce", false),
            m -> assertFalse(((ScaledFloatFieldMapper) m).coerce()));
        checker.registerUpdateCheck(b -> b.field("ignore_malformed", true),
            m -> assertTrue(((ScaledFloatFieldMapper) m).ignoreMalformed()));
    }

    public void testExistsQueryDocValuesDisabled() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("doc_values", false);
        }));
        assertExistsQuery(mapperService);
        assertParseMinimalWarnings();
    }

    public void testDefaults() throws Exception {
        XContentBuilder mapping = fieldMapping(b -> b.field("type", "scaled_float").field("scaling_factor", 10.0));
        DocumentMapper mapper = createDocumentMapper(mapping);
        assertEquals(Strings.toString(mapping), mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", 123)));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDimensionCount());
        assertFalse(pointField.fieldType().stored());
        assertEquals(1230, pointField.numericValue().longValue());
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertEquals(1230, dvField.numericValue().longValue());
        assertFalse(dvField.fieldType().stored());
    }

    public void testMissingScalingFactor() {
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(fieldMapping(b -> b.field("type", "scaled_float")))
        );
        assertThat(e.getMessage(), containsString("Failed to parse mapping: Field [scaling_factor] is required"));
    }

    public void testIllegalScalingFactor() {
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(fieldMapping(b -> b.field("type", "scaled_float").field("scaling_factor", -1)))
        );
        assertThat(e.getMessage(), containsString("[scaling_factor] must be a positive number, got [-1.0]"));
    }

    public void testNotIndexed() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "scaled_float").field("index", false).field("scaling_factor", 10.0))
        );

        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", 123)
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        IndexableField dvField = fields[0];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertEquals(1230, dvField.numericValue().longValue());
    }

    public void testNoDocValues() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "scaled_float").field("doc_values", false).field("scaling_factor", 10.0))
        );

        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", 123)
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDimensionCount());
        assertEquals(1230, pointField.numericValue().longValue());
    }

    public void testStore() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "scaled_float").field("store", true).field("scaling_factor", 10.0))
        );

        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", 123)
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(3, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDimensionCount());
        assertEquals(1230, pointField.numericValue().doubleValue(), 0d);
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        IndexableField storedField = fields[2];
        assertTrue(storedField.fieldType().stored());
        assertEquals(1230, storedField.numericValue().longValue());
    }

    public void testCoerce() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", BytesReference
            .bytes(XContentFactory.jsonBuilder()
                    .startObject()
                    .field("field", "123")
                    .endObject()),
            XContentType.JSON));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDimensionCount());
        assertEquals(1230, pointField.numericValue().longValue());
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());

        DocumentMapper mapper2 = createDocumentMapper(
            fieldMapping(b -> b.field("type", "scaled_float").field("scaling_factor", 10.0).field("coerce", false))
        );
        ThrowingRunnable runnable = () -> mapper2.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "123")
                        .endObject()),
                XContentType.JSON));
        MapperParsingException e = expectThrows(MapperParsingException.class, runnable);
        assertThat(e.getCause().getMessage(), containsString("passed as String"));
    }

    public void testIgnoreMalformed() throws Exception {
        doTestIgnoreMalformed("a", "For input string: \"a\"");

        List<String> values = Arrays.asList("NaN", "Infinity", "-Infinity");
        for (String value : values) {
            doTestIgnoreMalformed(value, "[scaled_float] only supports finite values, but got [" + value + "]");
        }
    }

    private void doTestIgnoreMalformed(String value, String exceptionMessageContains) throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ThrowingRunnable runnable = () -> mapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", value)
                        .endObject()),
            XContentType.JSON));
        MapperParsingException e = expectThrows(MapperParsingException.class, runnable);
        assertThat(e.getCause().getMessage(), containsString(exceptionMessageContains));

        DocumentMapper mapper2 = createDocumentMapper(
            fieldMapping(b -> b.field("type", "scaled_float").field("scaling_factor", 10.0).field("ignore_malformed", true))
        );
        ParsedDocument doc = mapper2.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", value)
                        .endObject()),
            XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(0, fields.length);
    }

    public void testNullValue() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .nullField("field")
                        .endObject()),
                XContentType.JSON));
        assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field"));

        mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "scaled_float")
            .field("scaling_factor", 10.0)
            .field("null_value", 2.5)));
        doc = mapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .nullField("field")
                        .endObject()),
                XContentType.JSON));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDimensionCount());
        assertFalse(pointField.fieldType().stored());
        assertEquals(25, pointField.numericValue().longValue());
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertFalse(dvField.fieldType().stored());
    }

    /**
     * `index_options` was deprecated and is rejected as of 7.0
     */
    public void testRejectIndexOptions() {
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(fieldMapping(b -> b.field("type", "scaled_float").field("index_options", randomIndexOptions())))
        );
        assertThat(e.getMessage(),
            containsString("Failed to parse mapping: unknown parameter [index_options] on mapper [field] of type [scaled_float]"));
    }

    @Override
    protected void randomFetchTestFieldConfig(XContentBuilder b) throws IOException {
        // Large floats are a terrible idea but the round trip should still work no matter how badly you configure the field
        b.field("type", "scaled_float").field("scaling_factor", randomDoubleBetween(0, Float.MAX_VALUE, true));
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        /*
         * randomDoubleBetween will smear the random values out across a huge
         * range of valid values.
         */
        double v = randomDoubleBetween(-Float.MAX_VALUE, Float.MAX_VALUE, true);
        switch (between(0, 3)) {
            case 0:
                return v;
            case 1:
                return (float) v;
            case 2:
                return Double.toString(v);
            case 3:
                return Float.toString((float) v);
            default:
                throw new IllegalArgumentException();
        }
    }
}
