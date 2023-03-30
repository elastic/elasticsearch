/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper.KeyedFlattenedFieldType;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper.RootFlattenedFieldType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.lucene.tests.analysis.BaseTokenStreamTestCase.assertTokenStreamContents;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class FlattenedFieldMapperTests extends MapperTestCase {

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "flattened");
    }

    @Override
    protected Object getSampleValueForDocument() {
        return Map.of("key", "value");
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("doc_values", b -> b.field("doc_values", false));
        checker.registerConflictCheck("index", b -> b.field("index", false));
        checker.registerConflictCheck("index_options", b -> b.field("index_options", "freqs"));
        checker.registerConflictCheck("null_value", b -> b.field("null_value", "foo"));
        checker.registerConflictCheck("similarity", b -> b.field("similarity", "boolean"));

        checker.registerUpdateCheck(b -> b.field("eager_global_ordinals", true), m -> assertTrue(m.fieldType().eagerGlobalOrdinals()));
        checker.registerUpdateCheck(b -> b.field("ignore_above", 256), m -> assertEquals(256, ((FlattenedFieldMapper) m).ignoreAbove()));
        checker.registerUpdateCheck(
            b -> b.field("split_queries_on_whitespace", true),
            m -> assertEquals("_whitespace", m.fieldType().getTextSearchInfo().searchAnalyzer().name())
        );
        checker.registerUpdateCheck(b -> b.field("depth_limit", 10), m -> assertEquals(10, ((FlattenedFieldMapper) m).depthLimit()));
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

        // Check the root fields.
        List<IndexableField> fields = parsedDoc.rootDoc().getFields("field");
        assertEquals(2, fields.size());

        assertEquals("field", fields.get(0).name());
        assertEquals(new BytesRef("value"), fields.get(0).binaryValue());
        assertFalse(fields.get(0).fieldType().stored());
        assertTrue(fields.get(0).fieldType().omitNorms());
        assertEquals(DocValuesType.NONE, fields.get(0).fieldType().docValuesType());

        assertEquals("field", fields.get(1).name());
        assertEquals(new BytesRef("value"), fields.get(1).binaryValue());
        assertEquals(DocValuesType.SORTED_SET, fields.get(1).fieldType().docValuesType());

        // Check the keyed fields.
        List<IndexableField> keyedFields = parsedDoc.rootDoc().getFields("field._keyed");
        assertEquals(2, keyedFields.size());

        assertEquals("field._keyed", keyedFields.get(0).name());
        assertEquals(new BytesRef("key\0value"), keyedFields.get(0).binaryValue());
        assertFalse(keyedFields.get(0).fieldType().stored());
        assertTrue(keyedFields.get(0).fieldType().omitNorms());
        assertEquals(DocValuesType.NONE, keyedFields.get(0).fieldType().docValuesType());

        assertEquals("field._keyed", keyedFields.get(1).name());
        assertEquals(new BytesRef("key\0value"), keyedFields.get(1).binaryValue());
        assertEquals(DocValuesType.SORTED_SET, keyedFields.get(1).fieldType().docValuesType());

        // Check that there is no 'field names' field.
        List<IndexableField> fieldNamesFields = parsedDoc.rootDoc().getFields(FieldNamesFieldMapper.NAME);
        assertEquals(0, fieldNamesFields.size());
    }

    public void testDisableIndex() throws Exception {

        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "flattened");
            b.field("index", false);
        }));
        ParsedDocument parsedDoc = mapper.parse(source(b -> b.startObject("field").field("key", "value").endObject()));

        List<IndexableField> fields = parsedDoc.rootDoc().getFields("field");
        assertEquals(1, fields.size());
        assertEquals(DocValuesType.SORTED_SET, fields.get(0).fieldType().docValuesType());

        List<IndexableField> keyedFields = parsedDoc.rootDoc().getFields("field._keyed");
        assertEquals(1, keyedFields.size());
        assertEquals(DocValuesType.SORTED_SET, keyedFields.get(0).fieldType().docValuesType());
    }

    public void testDisableDocValues() throws Exception {

        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "flattened");
            b.field("doc_values", false);
        }));
        ParsedDocument parsedDoc = mapper.parse(source(b -> b.startObject("field").field("key", "value").endObject()));

        List<IndexableField> fields = parsedDoc.rootDoc().getFields("field");
        assertEquals(1, fields.size());
        assertEquals(DocValuesType.NONE, fields.get(0).fieldType().docValuesType());

        List<IndexableField> keyedFields = parsedDoc.rootDoc().getFields("field._keyed");
        assertEquals(1, keyedFields.size());
        assertEquals(DocValuesType.NONE, keyedFields.get(0).fieldType().docValuesType());

        List<IndexableField> fieldNamesFields = parsedDoc.rootDoc().getFields(FieldNamesFieldMapper.NAME);
        assertEquals(1, fieldNamesFields.size());
        assertEquals("field", fieldNamesFields.get(0).stringValue());
    }

    public void testIndexOptions() throws IOException {

        createDocumentMapper(fieldMapping(b -> {
            b.field("type", "flattened");
            b.field("index_options", "freqs");
        }));

        for (String indexOptions : Arrays.asList("positions", "offsets")) {
            Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
                b.field("type", "flattened");
                b.field("index_options", indexOptions);
            })));
            assertThat(
                e.getMessage(),
                containsString("Unknown value [" + indexOptions + "] for field [index_options] - accepted values are [docs, freqs]")
            );
        }
    }

    public void testNullField() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument parsedDoc = mapper.parse(source(b -> b.nullField("field")));
        List<IndexableField> fields = parsedDoc.rootDoc().getFields("field");
        assertEquals(0, fields.size());
    }

    public void testBlankFieldName() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument parsedDoc = mapper.parse(source(b -> b.startObject("field").field("", "value").endObject()));
        List<IndexableField> fields = parsedDoc.rootDoc().getFields("field");
        assertEquals(2, fields.size());
    }

    public void testDotOnlyFieldName() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument parsedDoc = mapper.parse(
            source(b -> b.startObject("field").field(".", "value1").field("..", "value2").field("...", "value3").endObject())
        );
        List<IndexableField> fields = parsedDoc.rootDoc().getFields("field");
        assertEquals(6, fields.size());
    }

    public void testMixOfOrdinaryAndFlattenedFields() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            topMapping(
                b -> b.field("dynamic", "strict")
                    .startObject("properties")
                    .startObject("field")
                    .field("type", "flattened")
                    .endObject()
                    .startObject("a")
                    .field("type", "object")
                    .startObject("properties")
                    .startObject("b")
                    .field("type", "object")
                    .startObject("properties")
                    .startObject("c")
                    .field("type", "keyword")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .startObject("d")
                    .field("type", "object")
                    .startObject("properties")
                    .startObject("e")
                    .field("type", "keyword")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        ParsedDocument parsedDoc = mapper.parse(
            source(
                b -> b.startObject("field")
                    .field("", "value")
                    .field("subfield1", "value1")
                    .field("subfield2", "value2")
                    .endObject()
                    .startObject("a")
                    .startObject("b")
                    .field("c", "value3")
                    .endObject()
                    .endObject()
                    .field("d.e", "value4")
            )
        );
        assertNull(parsedDoc.dynamicMappingsUpdate());
        List<IndexableField> fields = parsedDoc.rootDoc().getFields("field");
        assertEquals(6, fields.size());
        fields = parsedDoc.rootDoc().getFields("a.b");
        assertEquals(0, fields.size());
        fields = parsedDoc.rootDoc().getFields("a.b.c");
        assertEquals(1, fields.size());
        fields = parsedDoc.rootDoc().getFields("d");
        assertEquals(0, fields.size());
        fields = parsedDoc.rootDoc().getFields("d.e");
        assertEquals(1, fields.size());
    }

    public void testMalformedJson() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> b.field("field", "not a JSON object"))));

        BytesReference doc2 = new BytesArray("{ \"field\": { \"key\": \"value\" ");
        expectThrows(MapperParsingException.class, () -> mapper.parse(new SourceToParse("1", doc2, XContentType.JSON)));
    }

    public void testFieldMultiplicity() throws Exception {

        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        ParsedDocument parsedDoc = mapper.parse(source(b -> {
            b.startArray("field");
            {
                b.startObject().field("key1", "value").endObject();
                b.startObject();
                {
                    b.field("key2", true);
                    b.field("key3", false);
                }
                b.endObject();
            }
            b.endArray();
        }));

        List<IndexableField> fields = parsedDoc.rootDoc().getFields("field");
        assertEquals(6, fields.size());
        assertEquals(new BytesRef("value"), fields.get(0).binaryValue());
        assertEquals(new BytesRef("true"), fields.get(2).binaryValue());
        assertEquals(new BytesRef("false"), fields.get(4).binaryValue());

        List<IndexableField> keyedFields = parsedDoc.rootDoc().getFields("field._keyed");
        assertEquals(6, keyedFields.size());
        assertEquals(new BytesRef("key1\0value"), keyedFields.get(0).binaryValue());
        assertEquals(new BytesRef("key2\0true"), keyedFields.get(2).binaryValue());
        assertEquals(new BytesRef("key3\0false"), keyedFields.get(4).binaryValue());
    }

    public void testDepthLimit() throws IOException {
        // First verify the default behavior when depth_limit is not set.
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));

        mapperService.documentMapper().parse(source(b -> {
            b.startObject("field");
            {
                b.startObject("key1");
                {
                    b.startObject("key2").field("key3", "value").endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        // Set a lower value for depth_limit and check that the field is rejected.
        merge(mapperService, fieldMapping(b -> {
            b.field("type", "flattened");
            b.field("depth_limit", 2);
        }));

        expectThrows(MapperParsingException.class, () -> mapperService.documentMapper().parse(source(b -> {
            b.startObject("field");
            {
                b.startObject("key1");
                {
                    b.startObject("key2").field("key3", "value").endObject();
                }
                b.endObject();
            }
            b.endObject();
        })));
    }

    public void testEagerGlobalOrdinals() throws IOException {

        DocumentMapper defMapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        FieldMapper fieldMapper = (FieldMapper) defMapper.mappers().getMapper("field");
        assertFalse(fieldMapper.fieldType().eagerGlobalOrdinals());

        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "flattened");
            b.field("eager_global_ordinals", true);
        }));

        fieldMapper = (FieldMapper) mapper.mappers().getMapper("field");
        assertTrue(fieldMapper.fieldType().eagerGlobalOrdinals());
    }

    public void testIgnoreAbove() throws IOException {
        // First verify the default behavior when ignore_above is not set.
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        DocumentMapper mapper = mapperService.documentMapper();

        ParsedDocument parsedDoc = mapper.parse(source(b -> {
            b.startArray("field");
            {
                b.startObject().field("key", "a longer then usual value").endObject();
            }
            b.endArray();
        }));
        List<IndexableField> fields = parsedDoc.rootDoc().getFields("field");
        assertEquals(2, fields.size());

        // Set a lower value for ignore_above and check that the field is skipped.
        DocumentMapper newMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "flattened");
            b.field("ignore_above", 10);
        }));

        parsedDoc = newMapper.parse(source(b -> {
            b.startArray("field");
            {
                b.startObject().field("key", "a longer then usual value").endObject();
            }
            b.endArray();
        }));
        List<IndexableField> newFields = parsedDoc.rootDoc().getFields("field");
        assertEquals(0, newFields.size());

        // using a key bigger than ignore_above should not prevent the field from being indexed, although we store key:value pairs
        parsedDoc = newMapper.parse(source(b -> {
            b.startArray("field");
            {
                b.startObject().field("key_longer_than_10chars", "value").endObject();
            }
            b.endArray();
        }));
        newFields = parsedDoc.rootDoc().getFields("field");
        assertEquals(2, fields.size());
    }

    /**
     * using a key:value pair above the Lucene term length limit would throw an error on indexing
     * that we pre-empt with a nices exception
     */
    public void testImmenseKeyedTermException() throws IOException {
        DocumentMapper newMapper = createDocumentMapper(fieldMapping(b -> { b.field("type", "flattened"); }));

        String longKey = "x".repeat(32800);
        MapperParsingException ex = expectThrows(MapperParsingException.class, () -> newMapper.parse(source(b -> {
            b.startArray("field");
            {
                b.startObject().field(longKey, "value").endObject();
            }
            b.endArray();
        })));
        assertEquals(
            "Flattened field [field] contains one immense field whose keyed encoding is longer "
                + "than the allowed max length of 32766 bytes. Key length: "
                + longKey.length()
                + ", value length: 5 for key starting with [xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx]",
            ex.getCause().getMessage()
        );

        String value = "x".repeat(32800);
        ex = expectThrows(MapperParsingException.class, () -> newMapper.parse(source(b -> {
            b.startArray("field");
            {
                b.startObject().field("key", value).endObject();
            }
            b.endArray();
        })));
        assertEquals(
            "Flattened field [field] contains one immense field whose keyed encoding is longer "
                + "than the allowed max length of 32766 bytes. Key length: 3, value length: "
                + value.length()
                + " for key starting with [key]",
            ex.getCause().getMessage()
        );
    }

    public void testNullValues() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("field").field("type", "flattened").endObject();
            b.startObject("other_field");
            {
                b.field("type", "flattened");
                b.field("null_value", "placeholder");
            }
            b.endObject();
        }));

        ParsedDocument parsedDoc = mapper.parse(source(b -> {
            b.startObject("field").nullField("key").endObject();
            b.startObject("other_field").nullField("key").endObject();
        }));

        List<IndexableField> fields = parsedDoc.rootDoc().getFields("field");
        assertEquals(0, fields.size());

        List<IndexableField> otherFields = parsedDoc.rootDoc().getFields("other_field");
        assertEquals(2, otherFields.size());
        assertEquals(new BytesRef("placeholder"), otherFields.get(0).binaryValue());
        assertEquals(new BytesRef("placeholder"), otherFields.get(1).binaryValue());

        List<IndexableField> prefixedOtherFields = parsedDoc.rootDoc().getFields("other_field._keyed");
        assertEquals(2, prefixedOtherFields.size());
        assertEquals(new BytesRef("key\0placeholder"), prefixedOtherFields.get(0).binaryValue());
        assertEquals(new BytesRef("key\0placeholder"), prefixedOtherFields.get(1).binaryValue());
    }

    public void testSplitQueriesOnWhitespace() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "flattened");
            b.field("split_queries_on_whitespace", true);
        }));

        RootFlattenedFieldType rootFieldType = (RootFlattenedFieldType) mapperService.fieldType("field");
        assertThat(rootFieldType.getTextSearchInfo().searchAnalyzer().name(), equalTo("_whitespace"));
        assertTokenStreamContents(
            rootFieldType.getTextSearchInfo().searchAnalyzer().analyzer().tokenStream("", "Hello World"),
            new String[] { "Hello", "World" }
        );

        KeyedFlattenedFieldType keyedFieldType = (KeyedFlattenedFieldType) mapperService.fieldType("field.key");
        assertThat(keyedFieldType.getTextSearchInfo().searchAnalyzer().name(), equalTo("_whitespace"));
        assertTokenStreamContents(
            keyedFieldType.getTextSearchInfo().searchAnalyzer().analyzer().tokenStream("", "Hello World"),
            new String[] { "Hello", "World" }
        );
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        assumeFalse("Test implemented in a follow up", true);
        return null;
    }

    public void testDynamicTemplateAndDottedPaths() throws IOException {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.startArray("dynamic_templates");
            b.startObject();
            b.startObject("no_deep_objects");
            b.field("path_match", "*.*.*");
            b.field("match_mapping_type", "object");
            b.startObject("mapping");
            b.field("type", "flattened");
            b.endObject();
            b.endObject();
            b.endObject();
            b.endArray();
        }));

        ParsedDocument doc = mapper.parse(source(b -> b.field("a.b.c.d", "value")));
        List<IndexableField> fields = doc.rootDoc().getFields("a.b.c");
        assertEquals(new BytesRef("value"), fields.get(0).binaryValue());
        List<IndexableField> keyed = doc.rootDoc().getFields("a.b.c._keyed");
        assertEquals(new BytesRef("d\0value"), keyed.get(0).binaryValue());
    }

    public void testAggregationsDocValuesDisabled() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("doc_values", false);
        }));
        assertAggregatableConsistency(mapperService.fieldType("field"));
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        throw new AssumptionViolatedException("not supported");
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }
}
