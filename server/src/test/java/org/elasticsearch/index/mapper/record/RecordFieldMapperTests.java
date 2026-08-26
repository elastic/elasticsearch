/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.record;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class RecordFieldMapperTests extends MapperTestCase {

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "record");
    }

    @Override
    protected Object getSampleValueForDocument() {
        return Map.of("key", "value");
    }

    @Override
    protected Object getSampleObjectForDocument() {
        return getSampleValueForDocument();
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("doc_values", b -> b.field("doc_values", false));
        checker.registerConflictCheck("index", b -> b.field("index", false));
        checker.registerConflictCheck("index_options", b -> b.field("index_options", "freqs"));
        checker.registerConflictCheck("null_value", b -> b.field("null_value", "foo"));
        checker.registerConflictCheck("similarity", b -> b.field("similarity", "boolean"));

        checker.registerUpdateCheck(
            "eager_global_ordinals",
            b -> b.field("eager_global_ordinals", true),
            m -> assertTrue(m.fieldType().eagerGlobalOrdinals())
        );
        checker.registerUpdateCheck(
            "ignore_above",
            b -> b.field("ignore_above", 256),
            m -> assertEquals(256, ((RecordFieldMapper) m).fieldType().ignoreAbove().get())
        );
        checker.registerUpdateCheck(
            "split_queries_on_whitespace",
            b -> b.field("split_queries_on_whitespace", true),
            m -> assertEquals("_whitespace", m.fieldType().getTextSearchInfo().searchAnalyzer().name())
        );
        checker.registerUpdateCheck(
            "depth_limit",
            b -> b.field("depth_limit", 10),
            m -> assertEquals(10, ((RecordFieldMapper) m).depthLimit())
        );
    }

    @Override
    protected void assertExistsQuery(MappedFieldType fieldType, Query query, LuceneDocument fields) {
        if (fieldType.hasDocValues()) {
            assertThat(query, instanceOf(FieldExistsQuery.class));
            assertEquals("field._keyed", ((FieldExistsQuery) query).getField());
        } else {
            super.assertExistsQuery(fieldType, query, fields);
        }
    }

    @Override
    protected boolean supportsStoredFields() {
        return false;
    }

    @Override
    protected boolean supportsIgnoreMalformed() {
        return false;
    }

    public void testDefaults() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument parsedDoc = mapper.parse(source(b -> b.startObject("field").field("key", "value").endObject()));

        // Root inverted-index term
        List<IndexableField> fields = parsedDoc.rootDoc().getFields("field");
        assertEquals(2, fields.size());
        assertEquals(new BytesRef("value"), fields.get(0).binaryValue());
        assertFalse(fields.get(0).fieldType().stored());
        assertEquals(DocValuesType.NONE, fields.get(0).fieldType().docValuesType());

        assertEquals(new BytesRef("value"), fields.get(1).binaryValue());
        assertEquals(DocValuesType.SORTED_SET, fields.get(1).fieldType().docValuesType());

        // Keyed inverted-index term
        List<IndexableField> keyedFields = parsedDoc.rootDoc().getFields("field._keyed");
        assertEquals(2, keyedFields.size());
        assertEquals(new BytesRef("key\0value"), keyedFields.get(0).binaryValue());
        assertEquals(DocValuesType.NONE, keyedFields.get(0).fieldType().docValuesType());
        assertEquals(new BytesRef("key\0value"), keyedFields.get(1).binaryValue());
        assertEquals(DocValuesType.SORTED_SET, keyedFields.get(1).fieldType().docValuesType());

        // No field-names field when doc values are present
        assertEquals(0, parsedDoc.rootDoc().getFields(FieldNamesFieldMapper.NAME).size());
    }

    public void testRecursiveObject() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument parsedDoc = mapper.parse(
            source(b -> b.startObject("field").startObject("nested").field("key", "value").endObject().endObject())
        );

        List<IndexableField> keyedFields = parsedDoc.rootDoc().getFields("field._keyed");
        // Expect "nested.key\0value"
        boolean found = keyedFields.stream().anyMatch(f -> new BytesRef("nested.key\0value").equals(f.binaryValue()));
        assertTrue("expected keyed term nested.key\\0value in " + keyedFields, found);
    }

    public void testArrayOfScalars() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument parsedDoc = mapper.parse(source(b -> b.startObject("field").array("tags", "a", "b", "c").endObject()));

        List<IndexableField> keyedFields = parsedDoc.rootDoc().getFields("field._keyed");
        long matches = keyedFields.stream().filter(f -> f.binaryValue() != null).filter(f -> {
            String s = f.binaryValue().utf8ToString();
            return s.startsWith("tags\0");
        }).count();
        assertThat("expected three keyed entries for tags array", matches, equalTo(3L));
    }

    public void testNullValueField() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "record");
            b.field("null_value", "NULL");
        }));
        ParsedDocument parsedDoc = mapper.parse(source(b -> b.startObject("field").nullField("key").endObject()));

        List<IndexableField> keyedFields = parsedDoc.rootDoc().getFields("field._keyed");
        boolean found = keyedFields.stream().anyMatch(f -> new BytesRef("key\0NULL").equals(f.binaryValue()));
        assertTrue("expected null_value substitution key\\0NULL", found);
    }

    public void testNullFieldValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument parsedDoc = mapper.parse(source(b -> b.nullField("field")));
        assertEquals(0, parsedDoc.rootDoc().getFields("field").size());
        assertEquals(0, parsedDoc.rootDoc().getFields("field._keyed").size());
    }

    public void testDepthLimit() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "record");
            b.field("depth_limit", 2);
        }));

        // depth 1: {a: {b: "v"}} — key path "a.b", depth 2 from root — should be accepted
        mapper.parse(source(b -> b.startObject("field").startObject("a").field("b", "v").endObject().endObject()));

        // depth 2: {a: {b: {c: "v"}}} — key path "a.b.c", depth 3 — should be rejected
        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(
                source(b -> b.startObject("field").startObject("a").startObject("b").field("c", "v").endObject().endObject().endObject())
            )
        );
        assertThat(e.getMessage(), containsString("depth_limit"));
    }

    public void testIgnoreAbove() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "record");
            b.field("ignore_above", 5);
        }));
        ParsedDocument parsedDoc = mapper.parse(
            source(b -> b.startObject("field").field("short", "ab").field("long", "toolong").endObject())
        );

        List<IndexableField> keyedFields = parsedDoc.rootDoc().getFields("field._keyed");
        boolean hasShort = keyedFields.stream()
            .anyMatch(f -> f.binaryValue() != null && f.binaryValue().utf8ToString().startsWith("short\0"));
        boolean hasLong = keyedFields.stream()
            .anyMatch(f -> f.binaryValue() != null && f.binaryValue().utf8ToString().startsWith("long\0"));
        assertTrue("value under ignore_above should be indexed", hasShort);
        assertFalse("value exceeding ignore_above should be dropped", hasLong);
    }

    public void testNoMultiFields() throws Exception {
        Exception e = expectThrows(Exception.class, () -> createDocumentMapper(fieldMapping(b -> {
            b.field("type", "record");
            b.startObject("fields");
            b.startObject("raw").field("type", "keyword").endObject();
            b.endObject();
        })));
        assertThat(e.getMessage(), containsString("does not support [fields]"));
    }

    public void testNoCopyTo() throws Exception {
        Exception e = expectThrows(Exception.class, () -> createDocumentMapper(fieldMapping(b -> {
            b.field("type", "record");
            b.array("copy_to", "other_field");
        })));
        assertThat(e.getMessage(), containsString("does not support [copy_to]"));
    }

    public void testExistsQueryDocValuesDisabled() throws IOException {
        var mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("doc_values", false);
        }));
        assertExistsQuery(mapperService);
    }

    public void testSubKeyFieldType() throws IOException {
        var mapperService = createMapperService(fieldMapping(this::minimalMapping));
        MappedFieldType root = mapperService.fieldType("field");
        assertThat(root, instanceOf(RecordFieldMapper.RootRecordFieldType.class));

        MappedFieldType keyed = mapperService.fieldType("field.some.path");
        assertThat(keyed, instanceOf(RecordFieldMapper.KeyedRecordFieldType.class));
        assertEquals("some.path", ((RecordFieldMapper.KeyedRecordFieldType) keyed).key());
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        assumeFalse("not implemented", true);
        return null;
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        throw new AssumptionViolatedException("not supported");
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }

    @Override
    protected List<SortShortcutSupport> getSortShortcutSupport() {
        return List.of(new SortShortcutSupport(this::minimalMapping, this::writeField, true));
    }

    @Override
    protected boolean supportsDocValuesSkippers() {
        return false;
    }
}
