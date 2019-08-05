/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.flattened.mapper;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.flattened.FlattenedMapperPlugin;
import org.elasticsearch.xpack.flattened.mapper.FlatObjectFieldMapper.KeyedFlatObjectFieldType;
import org.elasticsearch.xpack.flattened.mapper.FlatObjectFieldMapper.RootFlatObjectFieldType;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.apache.lucene.analysis.BaseTokenStreamTestCase.assertTokenStreamContents;
import static org.hamcrest.Matchers.equalTo;

public class FlatObjectFieldMapperTests extends ESSingleNodeTestCase {
    private IndexService indexService;
    private DocumentMapperParser parser;

    @Before
    public void setup() {
        indexService = createIndex("test");
        parser = indexService.mapperService().documentMapperParser();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(FlattenedMapperPlugin.class, XPackPlugin.class);
    }

    public void testDefaults() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "flattened")
                    .endObject()
                .endObject()
            .endObject()
        .endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        BytesReference doc = BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
            .startObject("field")
                .field("key", "value")
            .endObject()
        .endObject());

        ParsedDocument parsedDoc = mapper.parse(new SourceToParse("test", "type", "1", doc, XContentType.JSON));

        // Check the root fields.
        IndexableField[] fields = parsedDoc.rootDoc().getFields("field");
        assertEquals(2, fields.length);

        assertEquals("field", fields[0].name());
        assertEquals(new BytesRef("value"), fields[0].binaryValue());
        assertFalse(fields[0].fieldType().stored());
        assertTrue(fields[0].fieldType().omitNorms());
        assertEquals(DocValuesType.NONE, fields[0].fieldType().docValuesType());

        assertEquals("field", fields[1].name());
        assertEquals(new BytesRef("value"), fields[1].binaryValue());
        assertEquals(DocValuesType.SORTED_SET, fields[1].fieldType().docValuesType());

        // Check the keyed fields.
        IndexableField[] keyedFields = parsedDoc.rootDoc().getFields("field._keyed");
        assertEquals(2, keyedFields.length);

        assertEquals("field._keyed", keyedFields[0].name());
        assertEquals(new BytesRef("key\0value"), keyedFields[0].binaryValue());
        assertFalse(keyedFields[0].fieldType().stored());
        assertTrue(keyedFields[0].fieldType().omitNorms());
        assertEquals(DocValuesType.NONE, keyedFields[0].fieldType().docValuesType());

        assertEquals("field._keyed", keyedFields[1].name());
        assertEquals(new BytesRef("key\0value"), keyedFields[1].binaryValue());
        assertEquals(DocValuesType.SORTED_SET, keyedFields[1].fieldType().docValuesType());

        // Check that there is no 'field names' field.
        IndexableField[] fieldNamesFields = parsedDoc.rootDoc().getFields(FieldNamesFieldMapper.NAME);
        assertEquals(0, fieldNamesFields.length);
    }

    public void testDisableIndex() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "flattened")
                        .field("index", false)
                    .endObject()
                .endObject()
            .endObject()
        .endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        BytesReference doc = BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
            .startObject("field")
                .field("key", "value")
            .endObject()
        .endObject());

        ParsedDocument parsedDoc = mapper.parse(new SourceToParse("test", "type", "1", doc, XContentType.JSON));

        IndexableField[] fields = parsedDoc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        assertEquals(DocValuesType.SORTED_SET, fields[0].fieldType().docValuesType());

        IndexableField[] keyedFields = parsedDoc.rootDoc().getFields("field._keyed");
        assertEquals(1, keyedFields.length);
        assertEquals(DocValuesType.SORTED_SET, keyedFields[0].fieldType().docValuesType());
    }

    public void testDisableDocValues() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "flattened")
                        .field("doc_values", false)
                    .endObject()
                .endObject()
            .endObject()
        .endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        BytesReference doc = BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
            .startObject("field")
            .field("key", "value")
            .endObject()
            .endObject());

        ParsedDocument parsedDoc = mapper.parse(new SourceToParse("test", "type", "1", doc, XContentType.JSON));

        IndexableField[] fields = parsedDoc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        assertEquals(DocValuesType.NONE, fields[0].fieldType().docValuesType());

        IndexableField[] keyedFields = parsedDoc.rootDoc().getFields("field._keyed");
        assertEquals(1, keyedFields.length);
        assertEquals(DocValuesType.NONE, keyedFields[0].fieldType().docValuesType());

        IndexableField[] fieldNamesFields = parsedDoc.rootDoc().getFields(FieldNamesFieldMapper.NAME);
        assertEquals(1, fieldNamesFields.length);
        assertEquals("field", fieldNamesFields[0].stringValue());
    }

    public void testEnableStore() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "flattened")
                        .field("store", true)
                    .endObject()
                .endObject()
            .endObject()
        .endObject());

        expectThrows(UnsupportedOperationException.class, () ->
            parser.parse("type", new CompressedXContent(mapping)));
    }

    public void testIndexOptions() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "flattened")
                        .field("index_options", "freqs")
                    .endObject()
                .endObject()
            .endObject()
        .endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        for (String indexOptions : Arrays.asList("positions", "offsets")) {
            String invalidMapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("field")
                            .field("type", "flattened")
                            .field("index_options", indexOptions)
                        .endObject()
                    .endObject()
                .endObject()
            .endObject());
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                    () -> parser.parse("type", new CompressedXContent(invalidMapping)));
            assertEquals("The [flattened] field does not support positions, got [index_options]=" + indexOptions, e.getMessage());
        }
    }

    public void testNullField() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "flattened")
                    .endObject()
                .endObject()
            .endObject()
        .endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        BytesReference doc = BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
            .nullField("field")
        .endObject());

        ParsedDocument parsedDoc = mapper.parse(new SourceToParse("test", "type", "1", doc, XContentType.JSON));
        IndexableField[] fields = parsedDoc.rootDoc().getFields("field");
        assertEquals(0, fields.length);
    }

    public void testMalformedJson() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "flattened")
                    .endObject()
                .endObject()
            .endObject()
        .endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        BytesReference doc1 = BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
            .field("field", "not a JSON object")
        .endObject());

        expectThrows(MapperParsingException.class, () -> mapper.parse(
            new SourceToParse("test", "type", "1", doc1, XContentType.JSON)));

        BytesReference doc2 = new BytesArray("{ \"field\": { \"key\": \"value\" ");
        expectThrows(MapperParsingException.class, () -> mapper.parse(
            new SourceToParse("test", "type", "1", doc2, XContentType.JSON)));
    }

    public void testFieldMultiplicity() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "flattened")
                    .endObject()
                .endObject()
            .endObject()
        .endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        BytesReference doc = BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
            .startArray("field")
                .startObject()
                    .field("key1", "value")
                .endObject()
                .startObject()
                    .field("key2", true)
                    .field("key3", false)
                .endObject()
            .endArray()
        .endObject());

        ParsedDocument parsedDoc = mapper.parse(new SourceToParse("test", "type", "1", doc, XContentType.JSON));

        IndexableField[] fields = parsedDoc.rootDoc().getFields("field");
        assertEquals(6, fields.length);
        assertEquals(new BytesRef("value"), fields[0].binaryValue());
        assertEquals(new BytesRef("true"), fields[2].binaryValue());
        assertEquals(new BytesRef("false"), fields[4].binaryValue());

        IndexableField[] keyedFields = parsedDoc.rootDoc().getFields("field._keyed");
        assertEquals(6, keyedFields.length);
        assertEquals(new BytesRef("key1\0value"), keyedFields[0].binaryValue());
        assertEquals(new BytesRef("key2\0true"), keyedFields[2].binaryValue());
        assertEquals(new BytesRef("key3\0false"), keyedFields[4].binaryValue());
    }

    public void testDepthLimit() throws IOException {
         String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "flattened")
                        .field("depth_limit", 2)
                    .endObject()
                .endObject()
            .endObject()
        .endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        BytesReference doc = BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
            .startObject("field")
                .startObject("key1")
                    .startObject("key2")
                        .field("key3", "value")
                    .endObject()
                .endObject()
            .endObject()
        .endObject());

        expectThrows(MapperParsingException.class, () ->
            mapper.parse(new SourceToParse("test", "type", "1", doc, XContentType.JSON)));
    }

    public void testEagerGlobalOrdinals() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "flattened")
                        .field("eager_global_ordinals", true)
                    .endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        FieldMapper fieldMapper = (FieldMapper) mapper.mappers().getMapper("field");
        assertTrue(fieldMapper.fieldType().eagerGlobalOrdinals());
    }

    public void testIgnoreAbove() throws IOException {
         String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "flattened")
                        .field("ignore_above", 10)
                    .endObject()
                .endObject()
            .endObject()
        .endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        BytesReference doc = BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
            .startArray("field")
                .startObject()
                    .field("key", "a longer than usual value")
                .endObject()
            .endArray()
        .endObject());

        ParsedDocument parsedDoc = mapper.parse(new SourceToParse("test", "type", "1", doc, XContentType.JSON));
        IndexableField[] fields = parsedDoc.rootDoc().getFields("field");
        assertEquals(0, fields.length);
    }

    public void testNullValues() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "flattened")
                    .endObject()
                    .startObject("other_field")
                        .field("type", "flattened")
                        .field("null_value", "placeholder")
                    .endObject()
                .endObject()
            .endObject()
        .endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        BytesReference doc = BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
            .startObject("field")
                .nullField("key")
            .endObject()
            .startObject("other_field")
                .nullField("key")
            .endObject()
        .endObject());
        ParsedDocument parsedDoc = mapper.parse(new SourceToParse("test", "type", "1", doc, XContentType.JSON));

        IndexableField[] fields = parsedDoc.rootDoc().getFields("field");
        assertEquals(0, fields.length);

        IndexableField[] otherFields = parsedDoc.rootDoc().getFields("other_field");
        assertEquals(2, otherFields.length);
        assertEquals(new BytesRef("placeholder"), otherFields[0].binaryValue());
        assertEquals(new BytesRef("placeholder"), otherFields[1].binaryValue());

        IndexableField[] prefixedOtherFields = parsedDoc.rootDoc().getFields("other_field._keyed");
        assertEquals(2, prefixedOtherFields.length);
        assertEquals(new BytesRef("key\0placeholder"), prefixedOtherFields[0].binaryValue());
        assertEquals(new BytesRef("key\0placeholder"), prefixedOtherFields[1].binaryValue());
    }

    public void testSplitQueriesOnWhitespace() throws IOException {
        MapperService mapperService = indexService.mapperService();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "flattened")
                        .field("split_queries_on_whitespace", true)
                    .endObject()
                .endObject()
            .endObject().endObject());
        mapperService.merge("type", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);

        RootFlatObjectFieldType rootFieldType = (RootFlatObjectFieldType) mapperService.fullName("field");
        assertThat(rootFieldType.searchAnalyzer().name(), equalTo("whitespace"));
        assertTokenStreamContents(rootFieldType.searchAnalyzer().analyzer().tokenStream("", "Hello World"),
            new String[] {"Hello", "World"});

        KeyedFlatObjectFieldType keyedFieldType = (KeyedFlatObjectFieldType) mapperService.fullName("field.key");
        assertThat(keyedFieldType.searchAnalyzer().name(), equalTo("whitespace"));
        assertTokenStreamContents(keyedFieldType.searchAnalyzer().analyzer().tokenStream("", "Hello World"),
            new String[] {"Hello", "World"});
    }
}
