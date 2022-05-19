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
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;

import static org.elasticsearch.index.query.RangeQueryBuilder.GTE_FIELD;
import static org.elasticsearch.index.query.RangeQueryBuilder.GT_FIELD;
import static org.elasticsearch.index.query.RangeQueryBuilder.LTE_FIELD;
import static org.elasticsearch.index.query.RangeQueryBuilder.LT_FIELD;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.startsWith;

public abstract class RangeFieldMapperTests extends MapperTestCase {

    @Override
    protected boolean supportsSearchLookup() {
        return false;
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
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("doc_values", b -> b.field("doc_values", false));
        checker.registerConflictCheck("index", b -> b.field("index", false));
        checker.registerConflictCheck("store", b -> b.field("store", true));
        checker.registerUpdateCheck(b -> b.field("coerce", false), m -> assertFalse(((RangeFieldMapper) m).coerce()));
    }

    private String getFromField() {
        return random().nextBoolean() ? GT_FIELD.getPreferredName() : GTE_FIELD.getPreferredName();
    }

    private String getToField() {
        return random().nextBoolean() ? LT_FIELD.getPreferredName() : LTE_FIELD.getPreferredName();
    }

    protected abstract XContentBuilder rangeSource(XContentBuilder in) throws IOException;

    protected final XContentBuilder rangeSource(XContentBuilder in, String lower, String upper) throws IOException {
        return in.startObject("field").field(getFromField(), lower).field(getToField(), upper).endObject();
    }

    @Override
    protected final Object getSampleValueForDocument() {
        return Collections.emptyMap();
    }

    @Override
    protected Object getSampleValueForQuery() {
        return rangeValue();
    }

    public final void testDefaults() throws Exception {

        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        ParsedDocument doc = mapper.parse(source(this::rangeSource));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField dvField = fields[0];
        assertEquals(DocValuesType.BINARY, dvField.fieldType().docValuesType());

        IndexableField pointField = fields[1];
        assertEquals(2, pointField.fieldType().pointIndexDimensionCount());
        assertFalse(pointField.fieldType().stored());
    }

    public final void testNotIndexed() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("index", false);
        }));
        ParsedDocument doc = mapper.parse(source(this::rangeSource));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
    }

    public final void testNoDocValues() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("doc_values", false);
        }));
        ParsedDocument doc = mapper.parse(source(this::rangeSource));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(2, pointField.fieldType().pointIndexDimensionCount());
    }

    protected abstract String storedValue();

    public final void testStore() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("store", true);
        }));
        ParsedDocument doc = mapper.parse(source(this::rangeSource));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(3, fields.length);
        IndexableField dvField = fields[0];
        assertEquals(DocValuesType.BINARY, dvField.fieldType().docValuesType());
        IndexableField pointField = fields[1];
        assertEquals(2, pointField.fieldType().pointIndexDimensionCount());
        IndexableField storedField = fields[2];
        assertTrue(storedField.fieldType().stored());
        assertThat(storedField.stringValue(), containsString(storedValue()));
    }

    protected boolean supportsCoerce() {
        return true;
    }

    public final void testCoerce() throws IOException {
        assumeTrue("Type does not support coerce", supportsCoerce());

        DocumentMapper mapper2 = createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("coerce", false);
        }));

        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> mapper2.parse(source(b -> b.startObject("field").field(getFromField(), "5.2").field(getToField(), "10").endObject()))
        );
        assertThat(e.getCause().getMessage(), containsString("passed as String"));
    }

    protected boolean supportsDecimalCoerce() {
        return true;
    }

    public final void testDecimalCoerce() throws IOException {
        assumeTrue("Type does not support decimal coerce", supportsDecimalCoerce());
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        ParsedDocument doc1 = mapper.parse(
            source(
                b -> b.startObject("field")
                    .field(GT_FIELD.getPreferredName(), "2.34")
                    .field(LT_FIELD.getPreferredName(), "5.67")
                    .endObject()
            )
        );

        ParsedDocument doc2 = mapper.parse(
            source(b -> b.startObject("field").field(GT_FIELD.getPreferredName(), "2").field(LT_FIELD.getPreferredName(), "5").endObject())
        );

        IndexableField[] fields1 = doc1.rootDoc().getFields("field");
        IndexableField[] fields2 = doc2.rootDoc().getFields("field");

        assertEquals(fields1[1].binaryValue(), fields2[1].binaryValue());
    }

    protected abstract Object rangeValue();

    public final void testNullField() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(b -> b.nullField("field")));
        assertNull(doc.rootDoc().get("field"));
    }

    private void assertNullBounds(CheckedConsumer<XContentBuilder, IOException> toCheck, boolean checkMin, boolean checkMax)
        throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("store", true);
        }));

        ParsedDocument doc1 = mapper.parse(source(toCheck));
        ParsedDocument doc2 = mapper.parse(source(b -> b.startObject("field").endObject()));
        String[] full = storedValue(doc2).split(" : ");
        String storedValue = storedValue(doc1);
        if (checkMin) {
            assertThat(storedValue, startsWith(full[0]));
        }
        if (checkMax) {
            assertThat(storedValue, endsWith(full[1]));
        }
    }

    private static String storedValue(ParsedDocument doc) {
        assertEquals(3, doc.rootDoc().getFields("field").length);
        IndexableField[] fields = doc.rootDoc().getFields("field");
        IndexableField storedField = fields[2];
        return storedField.stringValue();
    }

    public final void testNullBounds() throws IOException {

        // null, null => min, max
        assertNullBounds(b -> b.startObject("field").nullField("gte").nullField("lte").endObject(), true, true);

        // null, val => min, val
        Object val = rangeValue();
        assertNullBounds(b -> b.startObject("field").nullField("gte").field("lte", val).endObject(), true, false);

        // val, null -> val, max
        assertNullBounds(b -> b.startObject("field").field("gte", val).nullField("lte").endObject(), false, true);
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        // Doc value fetching crashes.
        // https://github.com/elastic/elasticsearch/issues/70269
        // TODO when we fix doc values fetcher we should add tests for date and ip ranges.
        assumeFalse("DocValuesFetcher doesn't work", true);
        return null;
    }
}
