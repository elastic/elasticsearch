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
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentParsingException;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

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
        checker.registerConflictCheck("time_series_dimensions", b -> b.field("time_series_dimensions", List.of("one", "two")));

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

    public void testNotDimension() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        FlattenedFieldMapper.RootFlattenedFieldType ft = (FlattenedFieldMapper.RootFlattenedFieldType) mapperService.fieldType("field");
        assertFalse(ft.isDimension());
    }

    public void testDimension() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_dimensions", List.of("key1", "key2", "field3.key3"));
        }));

        FlattenedFieldMapper.RootFlattenedFieldType ft = (FlattenedFieldMapper.RootFlattenedFieldType) mapperService.fieldType("field");
        assertTrue(ft.isDimension());
    }

    public void testDimensionAndIgnoreAbove() {
        Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_dimensions", List.of("key1", "key2", "field3.key3")).field("ignore_above", 5);
        })));
        assertThat(
            e.getCause().getMessage(),
            containsString("Field [ignore_above] cannot be set in conjunction with field [time_series_dimensions]")
        );
    }

    public void testDimensionIndexedAndDocvalues() {
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
                minimalMapping(b);
                b.field("time_series_dimensions", List.of("key1", "key2", "field3.key3")).field("index", false).field("doc_values", false);
            })));
            assertThat(
                e.getCause().getMessage(),
                containsString("Field [time_series_dimensions] requires that [index] and [doc_values] are true")
            );
        }
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
                minimalMapping(b);
                b.field("time_series_dimensions", List.of("key1", "key2", "field3.key3")).field("index", true).field("doc_values", false);
            })));
            assertThat(
                e.getCause().getMessage(),
                containsString("Field [time_series_dimensions] requires that [index] and [doc_values] are true")
            );
        }
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
                minimalMapping(b);
                b.field("time_series_dimensions", List.of("key1", "key2", "field3.key3")).field("index", false).field("doc_values", true);
            })));
            assertThat(
                e.getCause().getMessage(),
                containsString("Field [time_series_dimensions] requires that [index] and [doc_values] are true")
            );
        }
    }

    public void testDimensionMultiValuedField() throws IOException {
        XContentBuilder mapping = fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_dimensions", List.of("key1", "key2", "field3.key3"));
        });
        DocumentMapper mapper = randomBoolean() ? createDocumentMapper(mapping) : createTimeSeriesModeDocumentMapper(mapping);

        Exception e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.array("field.key1", "value1", "value2")))
        );
        assertThat(e.getCause().getMessage(), containsString("Dimension field [field.key1] cannot be a multi-valued field"));
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

        expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> b.field("field", "not a JSON object"))));

        BytesReference doc2 = new BytesArray("{ \"field\": { \"key\": \"value\" ");
        expectThrows(DocumentParsingException.class, () -> mapper.parse(new SourceToParse("1", doc2, XContentType.JSON)));
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

        expectThrows(DocumentParsingException.class, () -> mapperService.documentMapper().parse(source(b -> {
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

    public void testAllDimensionsInRoutingPath() throws IOException {
        MapperService mapper = createMapperService(
            fieldMapping(b -> b.field("type", "flattened").field("time_series_dimensions", List.of("key1", "subfield.key2")))
        );
        IndexSettings settings = createIndexSettings(
            Version.CURRENT,
            Settings.builder()
                .put(IndexSettings.MODE.getKey(), "time_series")
                .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), List.of("field.key1", "field.subfield.key2"))
                .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-04-28T00:00:00Z")
                .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-04-29T00:00:00Z")
                .build()
        );
        mapper.documentMapper().validate(settings, false);  // Doesn't throw
    }

    public void testSomeDimensionsInRoutingPath() throws IOException {
        MapperService mapper = createMapperService(
            fieldMapping(
                b -> b.field("type", "flattened").field("time_series_dimensions", List.of("key1", "subfield.key2", "key3", "subfield.key4"))
            )
        );
        IndexSettings settings = createIndexSettings(
            Version.CURRENT,
            Settings.builder()
                .put(IndexSettings.MODE.getKey(), "time_series")
                .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), List.of("field.key1", "field.subfield.key2"))
                .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-04-28T00:00:00Z")
                .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-04-29T00:00:00Z")
                .build()
        );
        mapper.documentMapper().validate(settings, false);  // Doesn't throw
    }

    public void testMissingDimensionInRoutingPath() throws IOException {
        MapperService mapper = createMapperService(
            fieldMapping(b -> b.field("type", "flattened").field("time_series_dimensions", List.of("key1", "subfield.key2")))
        );
        IndexSettings settings = createIndexSettings(
            Version.CURRENT,
            Settings.builder()
                .put(IndexSettings.MODE.getKey(), "time_series")
                .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), List.of("field.key1", "field.subfield.key2", "field.key3"))
                .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-04-28T00:00:00Z")
                .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-04-29T00:00:00Z")
                .build()
        );
        Exception ex = expectThrows(IllegalArgumentException.class, () -> mapper.documentMapper().validate(settings, false));
        assertEquals(
            "All fields that match routing_path must be keywords with [time_series_dimension: true] "
                + "or flattened fields with a list of dimensions in [time_series_dimensions] and "
                + "without the [script] parameter. [field.key3] was [flattened].",
            ex.getMessage()
        );
    }

    public void testRoutingPathWithKeywordsAndFlattenedFields() throws IOException {
        DocumentMapper documentMapper = createDocumentMapper(mapping(b -> {
            b.startObject("flattened_field")
                .field("type", "flattened")
                .field("time_series_dimensions", List.of("key1", "key2"))
                .endObject();
            b.startObject("keyword_field");
            {
                b.field("type", "keyword");
                b.field("time_series_dimension", "true");
            }
            b.endObject();
        }));
        IndexSettings settings = createIndexSettings(
            Version.CURRENT,
            Settings.builder()
                .put(IndexSettings.MODE.getKey(), "time_series")
                .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), List.of("flattened_field.key1", "keyword_field"))
                .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-04-28T00:00:00Z")
                .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-04-29T00:00:00Z")
                .build()
        );
        documentMapper.validate(settings, false);
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
        DocumentParsingException ex = expectThrows(DocumentParsingException.class, () -> newMapper.parse(source(b -> {
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
        ex = expectThrows(DocumentParsingException.class, () -> newMapper.parse(source(b -> {
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
        return new FlattenedFieldSyntheticSourceSupport();
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }

    private static void randomMapExample(final TreeMap<Object, Object> example, int depth, int maxDepth) {
        for (int i = 0; i < randomIntBetween(2, 5); i++) {
            int j = depth >= maxDepth ? randomIntBetween(1, 2) : randomIntBetween(1, 3);
            switch (j) {
                case 1 -> example.put(randomAlphaOfLength(10), randomAlphaOfLength(10));
                case 2 -> {
                    int size = randomIntBetween(2, 10);
                    final Set<String> stringSet = new HashSet<>();
                    while (stringSet.size() < size) {
                        stringSet.add(String.valueOf(randomIntBetween(10_000, 20_000)));
                    }
                    final List<String> randomList = new ArrayList<>(stringSet);
                    Collections.sort(randomList);
                    example.put(randomAlphaOfLength(6), randomList);
                }
                case 3 -> {
                    final TreeMap<Object, Object> nested = new TreeMap<>();
                    randomMapExample(nested, depth + 1, maxDepth);
                    example.put(randomAlphaOfLength(10), nested);
                }
                default -> throw new IllegalArgumentException("value: [" + j + "] unexpected");
            }
        }
    }

    private static class FlattenedFieldSyntheticSourceSupport implements SyntheticSourceSupport {

        @Override
        public SyntheticSourceExample example(int maxValues) throws IOException {

            // NOTE: values must be keywords and we use a TreeMap to preserve order (doc values are sorted and the result
            // is created with keys and nested keys in sorted order).
            final TreeMap<Object, Object> map = new TreeMap<>();
            randomMapExample(map, 0, maxValues);
            return new SyntheticSourceExample(map, map, this::mapping);
        }

        @Override
        public List<SyntheticSourceInvalidExample> invalidExample() throws IOException {
            return List.of(
                new SyntheticSourceInvalidExample(
                    equalTo("field [field] of type [flattened] doesn't support synthetic " + "source because it doesn't have doc values"),
                    b -> b.field("type", "flattened").field("doc_values", false)
                )
            );
        }

        private void mapping(XContentBuilder b) throws IOException {
            b.field("type", "flattened");
        }
    }

    @Override
    protected boolean supportsCopyTo() {
        return false;
    }
}
