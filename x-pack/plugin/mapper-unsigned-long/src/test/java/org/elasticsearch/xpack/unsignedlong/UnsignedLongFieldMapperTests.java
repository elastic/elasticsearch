/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.unsignedlong;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.termvectors.TermVectorsService;
import org.elasticsearch.plugins.Plugin;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class UnsignedLongFieldMapperTests extends MapperTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new UnsignedLongMapperPlugin());
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "unsigned_long");
    }

    @Override
    protected Object getSampleValueForDocument() {
        return 123;
    }

    @Override
    protected boolean supportsSearchLookup() {
        return false;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("doc_values", b -> b.field("doc_values", false));
        checker.registerConflictCheck("index", b -> b.field("index", false));
        checker.registerConflictCheck("store", b -> b.field("store", true));
        checker.registerConflictCheck("null_value", b -> b.field("null_value", 1));
        checker.registerUpdateCheck(
            b -> b.field("ignore_malformed", true),
            m -> assertTrue(((UnsignedLongFieldMapper) m).ignoreMalformed())
        );
    }

    public void testDefaults() throws Exception {
        XContentBuilder mapping = fieldMapping(b -> b.field("type", "unsigned_long"));
        DocumentMapper mapper = createDocumentMapper(mapping);
        assertEquals(Strings.toString(mapping), mapper.mappingSource().toString());

        // test indexing of values as string
        {
            ParsedDocument doc = mapper.parse(source(b -> b.field("field", "18446744073709551615")));
            IndexableField[] fields = doc.rootDoc().getFields("field");
            assertEquals(2, fields.length);
            IndexableField pointField = fields[0];
            assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
            assertFalse(pointField.fieldType().stored());
            assertEquals(9223372036854775807L, pointField.numericValue().longValue());
            IndexableField dvField = fields[1];
            assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
            assertEquals(9223372036854775807L, dvField.numericValue().longValue());
            assertFalse(dvField.fieldType().stored());
        }

        // test indexing values as integer numbers
        {
            ParsedDocument doc = mapper.parse(source(b -> b.field("field", 9223372036854775807L)));
            IndexableField[] fields = doc.rootDoc().getFields("field");
            assertEquals(2, fields.length);
            IndexableField pointField = fields[0];
            assertEquals(-1L, pointField.numericValue().longValue());
            IndexableField dvField = fields[1];
            assertEquals(-1L, dvField.numericValue().longValue());
        }

        // test that indexing values as number with decimal is not allowed
        {
            ThrowingRunnable runnable = () -> mapper.parse(source(b -> b.field("field", 10.5)));
            MapperParsingException e = expectThrows(MapperParsingException.class, runnable);
            assertThat(e.getCause().getMessage(), containsString("Value \"10.5\" has a decimal part"));
        }
    }

    public void testNotIndexed() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "unsigned_long").field("index", false)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "18446744073709551615")));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        IndexableField dvField = fields[0];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertEquals(9223372036854775807L, dvField.numericValue().longValue());
    }

    public void testNoDocValues() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "unsigned_long").field("doc_values", false)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "18446744073709551615")));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        assertEquals(9223372036854775807L, pointField.numericValue().longValue());
    }

    public void testStore() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "unsigned_long").field("store", true)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "18446744073709551615")));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(3, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        assertEquals(9223372036854775807L, pointField.numericValue().longValue());
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertEquals(9223372036854775807L, dvField.numericValue().longValue());
        IndexableField storedField = fields[2];
        assertTrue(storedField.fieldType().stored());
        assertEquals("18446744073709551615", storedField.stringValue());
    }

    public void testCoerceMappingParameterIsIllegal() {
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(fieldMapping(b -> b.field("type", "unsigned_long").field("coerce", false)))
        );
        assertThat(
            e.getMessage(),
            containsString("Failed to parse mapping: unknown parameter [coerce] on mapper [field] of type [unsigned_long]")
        );
    }

    public void testNullValue() throws IOException {
        // test that if null value is not defined, field is not indexed
        {
            DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
            ParsedDocument doc = mapper.parse(source(b -> b.nullField("field")));
            assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field"));
        }

        // test that if null value is defined, it is used
        {
            DocumentMapper mapper = createDocumentMapper(
                fieldMapping(b -> b.field("type", "unsigned_long").field("null_value", "18446744073709551615"))
            );
            ParsedDocument doc = mapper.parse(source(b -> b.nullField("field")));
            ;
            IndexableField[] fields = doc.rootDoc().getFields("field");
            assertEquals(2, fields.length);
            IndexableField pointField = fields[0];
            assertEquals(9223372036854775807L, pointField.numericValue().longValue());
            IndexableField dvField = fields[1];
            assertEquals(9223372036854775807L, dvField.numericValue().longValue());
        }
    }

    public void testIgnoreMalformed() throws Exception {
        // test ignore_malformed is false by default
        {
            DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
            Object malformedValue1 = "a";
            ThrowingRunnable runnable = () -> mapper.parse(source(b -> b.field("field", malformedValue1)));
            MapperParsingException e = expectThrows(MapperParsingException.class, runnable);
            assertThat(e.getCause().getMessage(), containsString("For input string: \"a\""));

            Object malformedValue2 = Boolean.FALSE;
            runnable = () -> mapper.parse(source(b -> b.field("field", malformedValue2)));
            e = expectThrows(MapperParsingException.class, runnable);
            assertThat(e.getCause().getMessage(), containsString("For input string: \"false\""));
        }

        // test ignore_malformed when set to true ignored malformed documents
        {
            DocumentMapper mapper = createDocumentMapper(
                fieldMapping(b -> b.field("type", "unsigned_long").field("ignore_malformed", true))
            );
            Object malformedValue1 = "a";
            ParsedDocument doc = mapper.parse(source(b -> b.field("field", malformedValue1)));
            IndexableField[] fields = doc.rootDoc().getFields("field");
            assertEquals(0, fields.length);
            assertArrayEquals(new String[] { "field" }, TermVectorsService.getValues(doc.rootDoc().getFields("_ignored")));

            Object malformedValue2 = Boolean.FALSE;
            ParsedDocument doc2 = mapper.parse(source(b -> b.field("field", malformedValue2)));
            IndexableField[] fields2 = doc2.rootDoc().getFields("field");
            assertEquals(0, fields2.length);
            assertArrayEquals(new String[] { "field" }, TermVectorsService.getValues(doc2.rootDoc().getFields("_ignored")));
        }
    }

    public void testDecimalParts() throws IOException {
        XContentBuilder mapping = fieldMapping(b -> b.field("type", "unsigned_long"));
        DocumentMapper mapper = createDocumentMapper(mapping);
        {
            ThrowingRunnable runnable = () -> mapper.parse(source(b -> b.field("field", randomFrom("100.5", 100.5, 100.5f))));
            MapperParsingException e = expectThrows(MapperParsingException.class, runnable);
            assertThat(e.getCause().getMessage(), containsString("Value \"100.5\" has a decimal part"));
        }
        {
            ThrowingRunnable runnable = () -> mapper.parse(source(b -> b.field("field", randomFrom("0.9", 0.9, 0.9f))));
            MapperParsingException e = expectThrows(MapperParsingException.class, runnable);
            assertThat(e.getCause().getMessage(), containsString("Value \"0.9\" has a decimal part"));
        }
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", randomFrom("100.", "100.0", "100.00", 100.0, 100.0f))));
        assertThat(doc.rootDoc().getFields("field")[0].numericValue().longValue(), equalTo(Long.MIN_VALUE + 100L));
        assertThat(doc.rootDoc().getFields("field")[1].numericValue().longValue(), equalTo(Long.MIN_VALUE + 100L));

        doc = mapper.parse(source(b -> b.field("field", randomFrom("0.", "0.0", ".00", 0.0, 0.0f))));
        assertThat(doc.rootDoc().getFields("field")[0].numericValue().longValue(), equalTo(Long.MIN_VALUE));
        assertThat(doc.rootDoc().getFields("field")[1].numericValue().longValue(), equalTo(Long.MIN_VALUE));
    }

    public void testIndexingOutOfRangeValues() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        for (Object outOfRangeValue : new Object[] { "-1", -1L, "18446744073709551616", new BigInteger("18446744073709551616") }) {
            ThrowingRunnable runnable = () -> mapper.parse(source(b -> b.field("field", outOfRangeValue)));
            expectThrows(MapperParsingException.class, runnable);
        }
    }

    public void testExistsQueryDocValuesDisabled() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("doc_values", false);
        }));
        assertExistsQuery(mapperService);
        assertParseMinimalWarnings();
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        Number n = randomNumericValue();
        return randomBoolean() ? n : n.toString();
    }

    private Number randomNumericValue() {
        switch (randomInt(8)) {
            case 0:
                return randomNonNegativeByte();
            case 1:
                return (short) between(0, Short.MAX_VALUE);
            case 2:
                return randomInt(Integer.MAX_VALUE);
            case 3:
            case 4:
                return randomNonNegativeLong();
            default:
                BigInteger big = BigInteger.valueOf(randomLongBetween(0, Long.MAX_VALUE)).shiftLeft(1);
                return big.add(randomBoolean() ? BigInteger.ONE : BigInteger.ZERO);
        }
    }
}
