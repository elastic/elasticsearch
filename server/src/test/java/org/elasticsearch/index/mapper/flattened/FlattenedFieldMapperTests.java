/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.TestBlock;
import org.elasticsearch.index.mapper.TimeSeriesRoutingHashFieldMapper;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper.KeyedFlattenedFieldType;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper.RootFlattenedFieldType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.lucene.tests.analysis.BaseTokenStreamTestCase.assertTokenStreamContents;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
        checker.registerConflictCheck("time_series_dimensions", b -> b.field("time_series_dimensions", List.of("one", "two")));

        checker.registerUpdateCheck(
            "eager_global_ordinals",
            b -> b.field("eager_global_ordinals", true),
            m -> assertTrue(m.fieldType().eagerGlobalOrdinals())
        );
        checker.registerUpdateCheck(
            "ignore_above",
            b -> b.field("ignore_above", 256),
            m -> assertEquals(256, ((FlattenedFieldMapper) m).fieldType().ignoreAbove().get())
        );
        checker.registerUpdateCheck(
            "split_queries_on_whitespace",
            b -> b.field("split_queries_on_whitespace", true),
            m -> assertEquals("_whitespace", m.fieldType().getTextSearchInfo().searchAnalyzer().name())
        );
        checker.registerUpdateCheck(
            "depth_limit",
            b -> b.field("depth_limit", 10),
            m -> assertEquals(10, ((FlattenedFieldMapper) m).depthLimit())
        );
        checker.registerUpdateCheck("properties", b -> {
            b.startObject("properties");
            b.startObject("host").field("type", "keyword").endObject();
            b.endObject();
        }, m -> assertNotNull(((FlattenedFieldMapper) m).fieldType().getChildFieldType("host")));
    }

    @Override
    protected void assertExistsQuery(MappedFieldType fieldType, Query query, LuceneDocument fields) {
        if (fieldType.hasDocValues()) {
            assertThat(query, instanceOf(FieldExistsQuery.class));
            FieldExistsQuery fieldExistsQuery = (FieldExistsQuery) query;
            assertEquals("field._keyed", fieldExistsQuery.getField());
        } else {
            super.assertExistsQuery(fieldType, query, fields);
        }
    }

    public void testExistsQueryDocValuesDisabled() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("doc_values", false);
        }));
        assertExistsQuery(mapperService);
    }

    public void testExistsQueryMatchesDocuments() throws IOException {
        boolean indexed = randomBoolean();
        boolean docValues = indexed ? randomBoolean() : true;
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("index", indexed);
            b.field("doc_values", docValues);
        }));
        withLuceneIndex(mapperService, iw -> {
            iw.addDocument(
                mapperService.documentMapper().parse(source(b -> b.startObject("field").field("key", "value").endObject())).rootDoc()
            );
            iw.addDocument(mapperService.documentMapper().parse(source(b -> b.nullField("field"))).rootDoc());
        }, reader -> {
            IndexSearcher searcher = newSearcher(reader);
            MappedFieldType ft = mapperService.fieldType("field");
            Query existsQuery = ft.existsQuery(null);
            assertEquals(1, searcher.count(existsQuery));
        });
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

    public void testBinaryDocValuesType() throws Exception {
        DocumentMapper mapper = createMapperService(
            Settings.builder().put(IndexSettings.USE_TIME_SERIES_DOC_VALUES_FORMAT_SETTING.getKey(), true).build(),
            fieldMapping(this::minimalMapping)
        ).documentMapper();
        ParsedDocument parsedDoc = mapper.parse(source(b -> b.startObject("field").field("key", "value").endObject()));

        // Check the root fields.
        List<IndexableField> fields = parsedDoc.rootDoc().getFields("field");
        assertEquals(2, fields.size());

        assertEquals("field", fields.get(1).name());
        assertEquals(new BytesRef("value"), fields.get(1).binaryValue());
        assertEquals(DocValuesType.BINARY, fields.get(1).fieldType().docValuesType());

        // Check the keyed fields.
        List<IndexableField> keyedFields = parsedDoc.rootDoc().getFields("field._keyed");
        assertEquals(2, keyedFields.size());

        assertEquals("field._keyed", keyedFields.get(1).name());
        assertEquals(new BytesRef("key\0value"), keyedFields.get(1).binaryValue());
        assertEquals(DocValuesType.BINARY, keyedFields.get(1).fieldType().docValuesType());
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

    public void testDimensionMultiValuedFieldTSDB() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_dimensions", List.of("key1", "key2", "field3.key3"));
        }), IndexMode.TIME_SERIES);

        ParsedDocument doc = mapper.parse(source(null, b -> {
            b.array("field.key1", "value1", "value2");
            b.field("@timestamp", Instant.now());
        }, TimeSeriesRoutingHashFieldMapper.encode(randomInt())));
        assertThat(doc.docs().get(0).getFields("field"), hasSize(greaterThan(1)));
    }

    public void testDimensionMultiValuedFieldNonTSDB() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_dimensions", List.of("key1", "key2", "field3.key3"));
        }), randomFrom(IndexMode.STANDARD, IndexMode.LOGSDB));

        ParsedDocument doc = mapper.parse(source(b -> {
            b.array("field.key1", "value1", "value2");
            b.field("@timestamp", Instant.now());
        }));
        assertThat(doc.docs().get(0).getFields("field"), hasSize(greaterThan(1)));
    }

    public void testDisableIndex() throws Exception {

        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "flattened");
            b.field("index", false);
        }));
        ParsedDocument parsedDoc = mapper.parse(source(b -> b.startObject("field").field("key", "value").endObject()));

        // No root fields: index=false and no root doc values on new indices.
        List<IndexableField> fields = parsedDoc.rootDoc().getFields("field");
        assertEquals(0, fields.size());

        List<IndexableField> keyedFields = parsedDoc.rootDoc().getFields("field._keyed");
        assertEquals(1, keyedFields.size());
        assertEquals(DocValuesType.SORTED_SET, keyedFields.get(0).fieldType().docValuesType());
    }

    public void testDisableIndexBinary() throws Exception {
        DocumentMapper mapper = createMapperService(
            Settings.builder().put(IndexSettings.USE_TIME_SERIES_DOC_VALUES_FORMAT_SETTING.getKey(), true).build(),
            fieldMapping(b -> {
                b.field("type", "flattened");
                b.field("index", false);
            })
        ).documentMapper();
        ParsedDocument parsedDoc = mapper.parse(source(b -> b.startObject("field").field("key", "value").endObject()));

        // No root fields: index=false and no root doc values on new indices.
        List<IndexableField> fields = parsedDoc.rootDoc().getFields("field");
        assertEquals(0, fields.size());

        List<IndexableField> keyedFields = parsedDoc.rootDoc().getFields("field._keyed");
        assertEquals(1, keyedFields.size());
        assertEquals(DocValuesType.BINARY, keyedFields.get(0).fieldType().docValuesType());
    }

    public void testDisableIndexWithStoreRootDocValuesSetting() throws Exception {
        DocumentMapper mapper = createMapperService(
            Settings.builder().put(IndexSettings.STORE_FLATTENED_ROOT_DOC_VALUES.getKey(), true).build(),
            fieldMapping(b -> {
                b.field("type", "flattened");
                b.field("index", false);
            })
        ).documentMapper();
        ParsedDocument parsedDoc = mapper.parse(source(b -> b.startObject("field").field("key", "value").endObject()));

        // Has root field: index=false, but store_root_doc_values=true
        List<IndexableField> fields = parsedDoc.rootDoc().getFields("field");
        assertEquals(1, fields.size());
        assertEquals(DocValuesType.SORTED_SET, fields.get(0).fieldType().docValuesType());

        List<IndexableField> keyedFields = parsedDoc.rootDoc().getFields("field._keyed");
        assertEquals(1, keyedFields.size());
        assertEquals(DocValuesType.SORTED_SET, keyedFields.get(0).fieldType().docValuesType());
    }

    public void testDisableIndexWithStoreRootDocValuesSettingBinary() throws Exception {
        DocumentMapper mapper = createMapperService(
            Settings.builder()
                .put(IndexSettings.USE_TIME_SERIES_DOC_VALUES_FORMAT_SETTING.getKey(), true)
                .put(IndexSettings.STORE_FLATTENED_ROOT_DOC_VALUES.getKey(), true)
                .build(),
            fieldMapping(b -> {
                b.field("type", "flattened");
                b.field("index", false);
            })
        ).documentMapper();
        ParsedDocument parsedDoc = mapper.parse(source(b -> b.startObject("field").field("key", "value").endObject()));

        List<IndexableField> fields = parsedDoc.rootDoc().getFields("field");
        assertEquals(1, fields.size());
        assertEquals(DocValuesType.BINARY, fields.get(0).fieldType().docValuesType());

        List<IndexableField> keyedFields = parsedDoc.rootDoc().getFields("field._keyed");
        assertEquals(1, keyedFields.size());
        assertEquals(DocValuesType.BINARY, keyedFields.get(0).fieldType().docValuesType());
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
            IndexVersion.current(),
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
            IndexVersion.current(),
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
            IndexVersion.current(),
            Settings.builder()
                .put(IndexSettings.MODE.getKey(), "time_series")
                .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), List.of("field.key1", "field.subfield.key2", "field.key3"))
                .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-04-28T00:00:00Z")
                .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-04-29T00:00:00Z")
                .build()
        );
        Exception ex = expectThrows(IllegalArgumentException.class, () -> mapper.documentMapper().validate(settings, false));
        assertEquals(
            "All fields that match routing_path must be configured with [time_series_dimension: true] "
                + "or flattened fields with a list of dimensions in [time_series_dimensions] and "
                + "without the [script] parameter. [field._keyed] was not a dimension.",
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
            IndexVersion.current(),
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
        assertEquals(2, newFields.size());
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

    public void testPropertiesKeywordSubfield() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "flattened");
            b.startObject("properties");
            {
                b.startObject("host.name").field("type", "keyword").endObject();
            }
            b.endObject();
        }));

        ParsedDocument parsedDoc = mapper.parse(
            source(b -> b.startObject("field").field("host.name", "my-host").field("other", "val").endObject())
        );

        // Only the unmapped key "other" should appear in the flattened root/keyed fields.
        // The mapped key "host.name" is excluded from the flattened representation.
        List<IndexableField> rootFields = parsedDoc.rootDoc().getFields("field");
        assertEquals(2, rootFields.size());
        assertEquals(new BytesRef("val"), rootFields.get(0).binaryValue());

        List<IndexableField> keyedFields = parsedDoc.rootDoc().getFields("field._keyed");
        assertEquals(2, keyedFields.size());
        assertEquals(new BytesRef("other\0val"), keyedFields.get(0).binaryValue());

        // The mapped sub-field should produce its own typed field
        List<IndexableField> subFields = parsedDoc.rootDoc().getFields("field.host.name");
        assertThat(subFields.size(), equalTo(1));
        assertEquals(new BytesRef("my-host"), subFields.get(0).binaryValue());

        // The unmapped key should NOT produce a sub-field
        List<IndexableField> unmappedSubFields = parsedDoc.rootDoc().getFields("field.other");
        assertEquals(0, unmappedSubFields.size());
    }

    public void testPropertiesNestedObjectNotation() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "flattened");
            b.startObject("properties");
            {
                b.startObject("host.name").field("type", "keyword").endObject();
            }
            b.endObject();
        }));

        // Use nested object notation instead of a flat dotted key
        ParsedDocument parsedDoc = mapper.parse(source(b -> {
            b.startObject("field");
            {
                b.startObject("host").field("name", "my-host").endObject();
                b.field("other", "val");
            }
            b.endObject();
        }));

        // The mapped key "host.name" should still be excluded from root/keyed fields
        List<IndexableField> rootFields = parsedDoc.rootDoc().getFields("field");
        assertEquals(2, rootFields.size());
        assertEquals(new BytesRef("val"), rootFields.get(0).binaryValue());

        List<IndexableField> keyedFields = parsedDoc.rootDoc().getFields("field._keyed");
        assertEquals(2, keyedFields.size());
        assertEquals(new BytesRef("other\0val"), keyedFields.get(0).binaryValue());

        // The mapped sub-field should still be indexed through its own mapper
        List<IndexableField> subFields = parsedDoc.rootDoc().getFields("field.host.name");
        assertThat(subFields.size(), equalTo(1));
        assertEquals(new BytesRef("my-host"), subFields.get(0).binaryValue());
    }

    public void testPropertiesMultipleTypes() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "flattened");
            b.startObject("properties");
            {
                b.startObject("host.name").field("type", "keyword").endObject();
                b.startObject("host.ip").field("type", "ip").endObject();
            }
            b.endObject();
        }));

        ParsedDocument parsedDoc = mapper.parse(
            source(b -> b.startObject("field").field("host.name", "my-host").field("host.ip", "192.168.1.1").endObject())
        );

        List<IndexableField> nameFields = parsedDoc.rootDoc().getFields("field.host.name");
        assertFalse("Expected keyword sub-field to produce fields", nameFields.isEmpty());

        List<IndexableField> ipFields = parsedDoc.rootDoc().getFields("field.host.ip");
        assertFalse("Expected ip sub-field to produce fields", ipFields.isEmpty());
    }

    public void testPropertiesFieldTypeResolution() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "flattened");
            b.startObject("properties");
            {
                b.startObject("host.name").field("type", "keyword").endObject();
            }
            b.endObject();
        }));

        // The mapped property should resolve to a keyword field type
        MappedFieldType hostNameType = mapperService.fieldType("field.host.name");
        assertNotNull(hostNameType);
        assertEquals("keyword", hostNameType.typeName());

        // An unmapped key should still resolve to a flattened keyed type
        MappedFieldType unmappedType = mapperService.fieldType("field.unmapped_key");
        assertNotNull(unmappedType);
        assertEquals("flattened", unmappedType.typeName());
    }

    public void testPropertiesSerialization() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "flattened");
            b.startObject("properties");
            {
                b.startObject("host.name").field("type", "keyword").endObject();
                b.startObject("service.name").field("type", "keyword").field("doc_values", false).endObject();
            }
            b.endObject();
        }));

        // Verify roundtrip: parse the serialized mapping and verify it produces the same output
        MapperService reparsedService = createMapperService(mapper.mappingSource().string());
        assertEquals(mapper.mappingSource(), reparsedService.documentMapper().mappingSource());
    }

    public void testPropertiesMerge() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "flattened");
            b.startObject("properties");
            {
                b.startObject("host.name").field("type", "keyword").endObject();
            }
            b.endObject();
        }));

        // Add a new property
        merge(mapperService, fieldMapping(b -> {
            b.field("type", "flattened");
            b.startObject("properties");
            {
                b.startObject("host.name").field("type", "keyword").endObject();
                b.startObject("service.name").field("type", "keyword").endObject();
            }
            b.endObject();
        }));

        MappedFieldType hostNameType = mapperService.fieldType("field.host.name");
        assertNotNull(hostNameType);
        assertEquals("keyword", hostNameType.typeName());

        MappedFieldType serviceNameType = mapperService.fieldType("field.service.name");
        assertNotNull(serviceNameType);
        assertEquals("keyword", serviceNameType.typeName());
    }

    public void testPropertiesMergeTypeConflict() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "flattened");
            b.startObject("properties");
            {
                b.startObject("host.name").field("type", "keyword").endObject();
            }
            b.endObject();
        }));

        // Attempt to change the type of an existing property
        Exception e = expectThrows(IllegalArgumentException.class, () -> merge(mapperService, fieldMapping(b -> {
            b.field("type", "flattened");
            b.startObject("properties");
            {
                b.startObject("host.name").field("type", "ip").endObject();
            }
            b.endObject();
        })));
        assertThat(e.getMessage(), containsString("cannot be changed from type"));
    }

    public void testPropertiesDisallowedTypes() {
        for (String type : List.of("object", "nested", "flattened", "alias", "join", "completion", "geo_point")) {
            MapperParsingException e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
                b.field("type", "flattened");
                b.startObject("properties");
                if (type.equals("alias")) {
                    b.startObject("sub").field("type", "alias").field("path", "some_field").endObject();
                } else {
                    b.startObject("sub").field("type", type).endObject();
                }
                b.endObject();
            })));
            assertThat(
                "Type [" + type + "] should be disallowed",
                e.getMessage(),
                containsString("not supported as a mapped sub-field of flattened")
            );
        }
    }

    public void testPropertiesNoTypeSpecified() {
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
            b.field("type", "flattened");
            b.startObject("properties");
            b.startObject("sub").field("doc_values", false).endObject();
            b.endObject();
        })));
        assertThat(e.getMessage(), containsString("No type specified for property [sub]"));
    }

    public void testPropertiesExpectedMap() {
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
            b.field("type", "flattened");
            b.startObject("properties");
            b.field("sub", "not_a_map");
            b.endObject();
        })));
        assertThat(e.getMessage(), containsString("Expected map for property [sub]"));
    }

    public void testPropertiesCopyToDisallowed() {
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
            b.field("type", "flattened");
            b.startObject("properties");
            b.startObject("status").field("type", "keyword").field("copy_to", "target_field").endObject();
            b.endObject();
        })));
        assertThat(e.getMessage(), containsString("[copy_to] is not supported on properties of flattened field"));
    }

    public void testPropertiesMultiFieldsDisallowed() {
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
            b.field("type", "flattened");
            b.startObject("properties");
            b.startObject("status");
            {
                b.field("type", "keyword");
                b.startObject("fields");
                b.startObject("raw").field("type", "keyword").endObject();
                b.endObject();
            }
            b.endObject();
            b.endObject();
        })));
        assertThat(e.getMessage(), containsString("[fields] (multi-fields) is not supported on properties of flattened field"));
    }

    public void testPropertiesSubFieldMappingError() {
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
            b.field("type", "flattened");
            b.startObject("properties");
            b.startObject("status").field("type", "keyword").field("not_a_real_param", true).endObject();
            b.endObject();
        })));
        assertThat(e.getMessage(), containsString("unknown parameter [not_a_real_param] on mapper [status]"));
    }

    public void testFlattenedAsMultiFieldIsAccepted() throws Exception {
        createDocumentMapper(mapping(b -> {
            b.startObject("my_field");
            {
                b.field("type", "keyword");
                b.startObject("fields");
                b.startObject("flat").field("type", "flattened").endObject();
                b.endObject();
            }
            b.endObject();
        }));
    }

    public void testPropertiesTotalFieldsCount() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "flattened");
            b.startObject("properties");
            {
                b.startObject("a").field("type", "keyword").endObject();
                b.startObject("b").field("type", "keyword").endObject();
            }
            b.endObject();
        }));

        FlattenedFieldMapper flattenedMapper = (FlattenedFieldMapper) mapper.mappers().getMapper("field");
        // 1 (flattened) + 2 (keyword properties)
        assertEquals(3, flattenedMapper.getTotalFieldsCount());
    }

    public void testPropertiesMultiValueArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "flattened");
            b.startObject("properties");
            {
                b.startObject("tags").field("type", "keyword").endObject();
            }
            b.endObject();
        }));

        ParsedDocument parsedDoc = mapper.parse(
            source(b -> b.startObject("field").array("tags", "web", "prod", "us-east").field("other", "val").endObject())
        );

        List<IndexableField> tagFields = parsedDoc.rootDoc().getFields("field.tags");
        assertEquals(3, tagFields.size());
        Set<String> tagValues = new HashSet<>();
        for (IndexableField f : tagFields) {
            tagValues.add(f.binaryValue().utf8ToString());
        }
        assertEquals(Set.of("web", "prod", "us-east"), tagValues);

        List<IndexableField> rootFields = parsedDoc.rootDoc().getFields("field");
        assertEquals(2, rootFields.size());
    }

    public void testPropertiesNullValueForwarded() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "flattened");
            b.startObject("properties");
            {
                b.startObject("status").field("type", "keyword").field("null_value", "N/A").endObject();
            }
            b.endObject();
        }));

        ParsedDocument parsedDoc = mapper.parse(source(b -> b.startObject("field").nullField("status").field("other", "val").endObject()));

        List<IndexableField> statusFields = parsedDoc.rootDoc().getFields("field.status");
        assertThat(statusFields.size(), greaterThan(0));
        boolean foundNullValue = false;
        for (IndexableField f : statusFields) {
            if (f.binaryValue() != null && f.binaryValue().utf8ToString().equals("N/A")) {
                foundNullValue = true;
                break;
            }
        }
        assertTrue("Sub-field null_value should produce N/A", foundNullValue);
    }

    public void testPropertiesNullValueNotForwardedWithoutConfig() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "flattened");
            b.startObject("properties");
            {
                b.startObject("status").field("type", "keyword").endObject();
            }
            b.endObject();
        }));

        ParsedDocument parsedDoc = mapper.parse(source(b -> b.startObject("field").nullField("status").field("other", "val").endObject()));

        List<IndexableField> statusFields = parsedDoc.rootDoc().getFields("field.status");
        assertEquals(0, statusFields.size());
    }

    public void testPropertiesPreservedOnMergeWhenOmitted() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "flattened");
            b.startObject("properties");
            {
                b.startObject("host.name").field("type", "keyword").endObject();
                b.startObject("host.ip").field("type", "ip").endObject();
            }
            b.endObject();
        }));

        merge(mapperService, fieldMapping(b -> {
            b.field("type", "flattened");
            b.startObject("properties");
            {
                b.startObject("host.name").field("type", "keyword").endObject();
            }
            b.endObject();
        }));

        assertNotNull(mapperService.fieldType("field.host.name"));
        assertEquals("keyword", mapperService.fieldType("field.host.name").typeName());
        assertNotNull(mapperService.fieldType("field.host.ip"));
        assertEquals("ip", mapperService.fieldType("field.host.ip").typeName());
    }

    public void testPropertiesAdditionalTypes() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "flattened");
            b.startObject("properties");
            {
                b.startObject("is_active").field("type", "boolean").endObject();
                b.startObject("score").field("type", "double").endObject();
                b.startObject("weight").field("type", "float").endObject();
                b.startObject("count").field("type", "integer").endObject();
                b.startObject("created").field("type", "date").endObject();
            }
            b.endObject();
        }));

        ParsedDocument parsedDoc = mapper.parse(source(b -> {
            b.startObject("field");
            {
                b.field("is_active", true);
                b.field("score", 9.5);
                b.field("weight", 1.5f);
                b.field("count", 42);
                b.field("created", "2024-01-15");
                b.field("unmapped", "val");
            }
            b.endObject();
        }));

        assertThat(parsedDoc.rootDoc().getFields("field.is_active").size(), greaterThan(0));
        assertThat(parsedDoc.rootDoc().getFields("field.score").size(), greaterThan(0));
        assertThat(parsedDoc.rootDoc().getFields("field.weight").size(), greaterThan(0));
        assertThat(parsedDoc.rootDoc().getFields("field.count").size(), greaterThan(0));
        assertThat(parsedDoc.rootDoc().getFields("field.created").size(), greaterThan(0));

        List<IndexableField> rootFields = parsedDoc.rootDoc().getFields("field");
        assertEquals(2, rootFields.size());
        assertEquals(new BytesRef("val"), rootFields.get(0).binaryValue());
    }

    public void testPropertiesIgnoreAboveDoesNotAffectMappedProperty() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "flattened");
            b.field("ignore_above", 3);
            b.startObject("properties");
            {
                b.startObject("status").field("type", "keyword").endObject();
            }
            b.endObject();
        }));

        ParsedDocument parsedDoc = mapper.parse(
            source(b -> b.startObject("field").field("status", "healthy").field("other", "long_value").endObject())
        );

        List<IndexableField> statusFields = parsedDoc.rootDoc().getFields("field.status");
        assertThat("ignore_above on flattened should not affect mapped property", statusFields.size(), greaterThan(0));
        assertEquals(new BytesRef("healthy"), statusFields.get(0).binaryValue());

        List<IndexableField> rootFields = parsedDoc.rootDoc().getFields("field");
        assertEquals("long_value exceeds ignore_above=3, should be skipped from root fields", 0, rootFields.size());
    }

    public void testPropertiesDepthLimitDoesNotAffectTopLevelMappedProperty() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "flattened");
            b.field("depth_limit", 1);
            b.startObject("properties");
            {
                b.startObject("status").field("type", "keyword").endObject();
            }
            b.endObject();
        }));

        ParsedDocument parsedDoc = mapper.parse(source(b -> b.startObject("field").field("status", "ok").endObject()));

        List<IndexableField> statusFields = parsedDoc.rootDoc().getFields("field.status");
        assertThat(statusFields.size(), greaterThan(0));
    }

    public void testPropertiesEmptyStringValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "flattened");
            b.startObject("properties");
            {
                b.startObject("name").field("type", "keyword").endObject();
            }
            b.endObject();
        }));

        ParsedDocument parsedDoc = mapper.parse(source(b -> b.startObject("field").field("name", "").field("other", "val").endObject()));

        List<IndexableField> nameFields = parsedDoc.rootDoc().getFields("field.name");
        assertThat(nameFields.size(), greaterThan(0));
        assertEquals(new BytesRef(""), nameFields.get(0).binaryValue());
    }

    public void testPropertiesWithDocValuesOnly() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "flattened");
            b.startObject("properties");
            {
                b.startObject("sortfield").field("type", "keyword").field("index", false).endObject();
            }
            b.endObject();
        }));

        ParsedDocument parsedDoc = mapper.parse(
            source(b -> b.startObject("field").field("sortfield", "value1").field("other", "value2").endObject())
        );

        List<IndexableField> subFields = parsedDoc.rootDoc().getFields("field.sortfield");
        assertEquals(1, subFields.size());
        assertEquals(DocValuesType.SORTED_SET, subFields.get(0).fieldType().docValuesType());
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        return new FlattenedFieldSyntheticSourceSupport();
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }

    private static void randomMapExample(final Map<String, Object> example, int depth, int maxDepth) {
        for (int i = 0; i < randomIntBetween(2, 5); i++) {
            int j = depth >= maxDepth ? randomIntBetween(1, 2) : randomIntBetween(1, 3);
            switch (j) {
                case 1 -> example.put(randomAlphaOfLength(10), randomAlphaOfLengthBetween(1, 10));
                case 2 -> {
                    int size = randomIntBetween(2, 10);
                    final Set<String> stringSet = new HashSet<>();
                    while (stringSet.size() < size) {
                        stringSet.add(String.valueOf(randomIntBetween(10_000, 2_000_000)));
                    }
                    final List<String> randomList = new ArrayList<>(stringSet);
                    Collections.sort(randomList);
                    example.put(randomAlphaOfLength(6), randomList);
                }
                case 3 -> {
                    final Map<String, Object> nested = new HashMap<>();
                    randomMapExample(nested, depth + 1, maxDepth);
                    example.put(randomAlphaOfLength(10), nested);
                }
                default -> throw new IllegalArgumentException("value: [" + j + "] unexpected");
            }
        }
    }

    private static class FlattenedFieldSyntheticSourceSupport implements SyntheticSourceSupport {
        private final Integer ignoreAbove = randomBoolean() ? randomIntBetween(4, 10) : null;

        @Override
        public SyntheticSourceExample example(int maxValues) throws IOException {
            if (randomBoolean()) {
                // Create a singleton value
                var value = randomObject();
                return new SyntheticSourceExample(value, mergeIntoExpectedMap(List.of(value)), this::mapping);
            }

            // Create an array of flattened field values
            var values = new ArrayList<Map<String, Object>>();
            for (int i = 0; i < maxValues; i++) {
                values.add(randomObject());
            }
            var merged = mergeIntoExpectedMap(values);

            return new SyntheticSourceExample(values, merged, this::mapping);
        }

        private Map<String, Object> randomObject() {
            var maxDepth = randomIntBetween(1, 3);

            final Map<String, Object> map = new HashMap<>();
            randomMapExample(map, 0, maxDepth);

            return map;
        }

        // Since arrays are moved to leafs in synthetic source, the result is not an array of objects
        // but one big object containing merged values from all input objects.
        // This function performs that transformation.
        private Map<String, Object> mergeIntoExpectedMap(List<Map<String, Object>> inputValues) {
            // Fields are sorted since they come (mostly) from doc_values.
            var result = new TreeMap<String, Object>();
            doMerge(inputValues, result);
            return result;
        }

        @SuppressWarnings("unchecked")
        private void doMerge(List<Map<String, Object>> inputValues, TreeMap<String, Object> result) {
            for (var iv : inputValues) {
                for (var field : iv.entrySet()) {
                    if (field.getValue() instanceof Map<?, ?> inputNestedMap) {
                        var intermediateResultMap = result.get(field.getKey());
                        if (intermediateResultMap == null) {
                            var map = new TreeMap<String, Object>();

                            result.put(field.getKey(), map);
                            doMerge(List.of((Map<String, Object>) inputNestedMap), map);
                        } else if (intermediateResultMap instanceof Map<?, ?> m) {
                            doMerge(List.of((Map<String, Object>) inputNestedMap), (TreeMap<String, Object>) m);
                        } else {
                            throw new IllegalStateException("Conflicting entries in merged map");
                        }
                    } else {
                        var valueAtCurrentLevel = result.get(field.getKey());
                        if (valueAtCurrentLevel == null) {
                            result.put(field.getKey(), field.getValue());
                        } else if (valueAtCurrentLevel instanceof List) {
                            ((List<Object>) valueAtCurrentLevel).add(field.getValue());
                        } else {
                            var list = new ArrayList<>();
                            list.add(valueAtCurrentLevel);
                            list.add(field.getValue());

                            result.put(field.getKey(), list);
                        }
                    }
                }
            }
        }

        @Override
        public List<SyntheticSourceInvalidExample> invalidExample() throws IOException {
            return List.of();
        }

        private void mapping(XContentBuilder b) throws IOException {
            b.field("type", "flattened");
            if (ignoreAbove != null) {
                b.field("ignore_above", ignoreAbove);
            }
        }
    }

    public void testSyntheticSourceWithOnlyIgnoredValues() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("field").field("type", "flattened").field("ignore_above", 1).endObject();
        })).documentMapper();

        var syntheticSource = syntheticSource(mapper, b -> {
            b.startObject("field");
            {
                b.field("key1", "val1");
                b.startObject("obj1");
                {
                    b.field("key2", "val2");
                    b.field("key3", List.of("val3", "val4"));
                }
                b.endObject();
            }
            b.endObject();
        });
        assertThat(syntheticSource, equalTo("{\"field\":{\"key1\":\"val1\",\"obj1\":{\"key2\":\"val2\",\"key3\":[\"val3\",\"val4\"]}}}"));
    }

    public void testSyntheticSourceWithCommonLeafField() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(
            mapping(b -> { b.startObject("field").field("type", "flattened").endObject(); })
        ).documentMapper();

        var syntheticSource = syntheticSource(mapper, b -> {
            b.startObject("field");
            {
                b.startObject("obj1").field("key", "foo").endObject();
                b.startObject("obj2").field("key", "bar").endObject();
            }
            b.endObject();
        });
        assertThat(syntheticSource, equalTo("""
            {"field":{"obj1":{"key":"foo"},"obj2":{"key":"bar"}}}"""));
    }

    public void testSyntheticSourceWithScalarObjectMismatch() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(
            mapping(b -> { b.startObject("field").field("type", "flattened").endObject(); })
        ).documentMapper();

        var syntheticSource = syntheticSource(mapper, b -> {
            b.startObject("field");
            {
                b.field("key1.key2", "foo");
                b.startObject("key1");
                {
                    b.startObject("key2").field("key3", "bar").endObject();
                    b.field("key4", "baz");
                }
                b.endObject();
                b.field("key5", "qux");
            }
            b.endObject();
        });
        assertThat(syntheticSource, equalTo("""
            {"field":{"key1":{"key2":"foo","key2.key3":"bar","key4":"baz"},"key5":"qux"}}"""));
    }

    public void testSyntheticSourceWithScalarObjectMismatchArray() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(
            mapping(b -> { b.startObject("field").field("type", "flattened").endObject(); })
        ).documentMapper();

        var syntheticSource = syntheticSource(mapper, b -> {
            b.startObject("field");
            {
                b.array("key1.key2", "qux", "foo");
                b.startObject("key1");
                {
                    b.field("key2.key3", "baz");
                    b.field("key2", "bar");
                }
                b.endObject();
            }
            b.endObject();
        });
        assertThat(syntheticSource, equalTo("""
            {"field":{"key1":{"key2":["bar","foo","qux"],"key2.key3":"baz"}}}"""));
    }

    public void testSyntheticSourceWithEmptyObject() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(
            mapping(b -> { b.startObject("field").field("type", "flattened").endObject(); })
        ).documentMapper();

        var syntheticSource = syntheticSource(mapper, b -> {
            b.startObject("field");
            {
                b.field("key1", "foo");
                b.startObject("key2").endObject();
            }
            b.endObject();
        });
        // Objects without any values are not included in the synthetic source
        assertThat(syntheticSource, equalTo("""
            {"field":{"key1":"foo"}}"""));
    }

    public void testSyntheticSourceWithMatchesInNestedPath() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(
            mapping(b -> { b.startObject("field").field("type", "flattened").endObject(); })
        ).documentMapper();

        // This test covers a scenario that previously had a bug.
        // Since a.b.c and b.b.d have a matching middle key `b`, and b.b.d starts with a `b`,
        // startObject was not called for the first `b` in b.b.d.
        // For a full explanation see this comment: https://github.com/elastic/elasticsearch/pull/129600#issuecomment-3024476134
        var syntheticSource = syntheticSource(mapper, b -> {
            b.startObject("field");
            {
                b.startObject("a");
                {
                    b.startObject("b").field("c", "1").endObject();
                }
                b.endObject();
                b.startObject("b");
                {
                    b.startObject("b").field("d", "2").endObject();
                }
                b.endObject();
            }
            b.endObject();
        });
        assertThat(syntheticSource, equalTo("""
            {"field":{"a":{"b":{"c":"1"}},"b":{"b":{"d":"2"}}}}"""));
    }

    public void testMultipleDotsInPath() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(
            mapping(b -> { b.startObject("field").field("type", "flattened").endObject(); })
        ).documentMapper();

        var syntheticSource = syntheticSource(mapper, b -> {
            b.startObject("field");
            {
                b.startObject(".");
                {
                    b.field(".", "bar");
                }
                b.endObject();
            }
            b.endObject();
        });
        // This behavior is weird to say the least. But this is the only reasonable way to interpret the meaning of the path `...`
        assertThat(syntheticSource, equalTo("""
            {"field":{"":{"":{"":{"":"bar"}}}}}"""));
    }

    public void testSyntheticSourceWithMappedProperties() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("field");
            {
                b.field("type", "flattened");
                b.startObject("properties");
                {
                    b.startObject("host.name").field("type", "keyword").endObject();
                }
                b.endObject();
            }
            b.endObject();
        })).documentMapper();

        var syntheticSource = syntheticSource(mapper, b -> {
            b.startObject("field");
            {
                b.startObject("host").field("name", "server-a").endObject();
                b.field("unmapped_key", "some_value");
            }
            b.endObject();
        });
        assertThat(syntheticSource, equalTo("{\"field\":{\"unmapped_key\":\"some_value\",\"host.name\":\"server-a\"}}"));
    }

    public void testSyntheticSourceWithMappedPropertiesOnly() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("field");
            {
                b.field("type", "flattened");
                b.startObject("properties");
                {
                    b.startObject("status").field("type", "keyword").endObject();
                    b.startObject("code").field("type", "long").endObject();
                }
                b.endObject();
            }
            b.endObject();
        })).documentMapper();

        var syntheticSource = syntheticSource(mapper, b -> {
            b.startObject("field");
            {
                b.field("status", "ok");
                b.field("code", 200);
            }
            b.endObject();
        });
        assertThat(syntheticSource, equalTo("{\"field\":{\"code\":200,\"status\":\"ok\"}}"));
    }

    public void testSyntheticSourceMappedPropertyFieldOrdering() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("field");
            {
                b.field("type", "flattened");
                b.startObject("properties");
                {
                    b.startObject("c_mapped").field("type", "keyword").endObject();
                }
                b.endObject();
            }
            b.endObject();
        })).documentMapper();

        var syntheticSource = syntheticSource(mapper, b -> {
            b.startObject("field");
            {
                b.field("a_unmapped", "val_a");
                b.field("c_mapped", "val_c");
                b.field("e_unmapped", "val_e");
            }
            b.endObject();
        });
        // Mapped properties are written after unmapped keys in the current implementation
        assertThat(syntheticSource, equalTo("{\"field\":{\"a_unmapped\":\"val_a\",\"e_unmapped\":\"val_e\",\"c_mapped\":\"val_c\"}}"));
    }

    public void testExistsQueryOnMappedProperty() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "flattened");
            b.startObject("properties");
            {
                b.startObject("host.name").field("type", "keyword").endObject();
            }
            b.endObject();
        }));

        withLuceneIndex(mapperService, iw -> {
            iw.addDocument(
                mapperService.documentMapper()
                    .parse(source(b -> b.startObject("field").field("host.name", "server-a").field("other", "val").endObject()))
                    .rootDoc()
            );
            iw.addDocument(
                mapperService.documentMapper().parse(source(b -> b.startObject("field").field("other", "val2").endObject())).rootDoc()
            );
        }, reader -> {
            IndexSearcher searcher = newSearcher(reader);
            MappedFieldType hostNameType = mapperService.fieldType("field.host.name");
            Query existsQuery = hostNameType.existsQuery(null);
            assertEquals(1, searcher.count(existsQuery));
        });
    }

    public void testSyntheticSourceWithUnmappedKeysOnly() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("field");
            {
                b.field("type", "flattened");
                b.startObject("properties");
                {
                    b.startObject("host.name").field("type", "keyword").endObject();
                }
                b.endObject();
            }
            b.endObject();
        })).documentMapper();

        var syntheticSource = syntheticSource(mapper, b -> {
            b.startObject("field");
            {
                b.field("unmapped", "value");
            }
            b.endObject();
        });
        assertThat(syntheticSource, equalTo("{\"field\":{\"unmapped\":\"value\"}}"));
    }

    public void testKeyedFlattenedDocValuesBlockLoaderWithMappedProperties() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "flattened");
            b.startObject("properties");
            {
                b.startObject("status").field("type", "keyword").endObject();
                b.startObject("code").field("type", "long").endObject();
            }
            b.endObject();
        }));

        MappedFieldType unmappedKeyType = mapperService.fieldType("field.unmapped_key");
        assertThat(unmappedKeyType, instanceOf(KeyedFlattenedFieldType.class));
        BlockLoader unmappedBlockLoader = unmappedKeyType.blockLoader(null);
        assertThat(unmappedBlockLoader, instanceOf(KeyedFlattenedDocValuesBlockLoader.class));

        MappedFieldType.BlockLoaderContext blContext = mock(MappedFieldType.BlockLoaderContext.class);
        when(blContext.fieldExtractPreference()).thenReturn(MappedFieldType.FieldExtractPreference.DOC_VALUES);
        when(blContext.ordinalsByteSize()).thenReturn(ByteSizeValue.ofMb(1));

        MappedFieldType statusFieldType = mapperService.fieldType("field.status");
        assertEquals("keyword", statusFieldType.typeName());
        BlockLoader statusBlockLoader = statusFieldType.blockLoader(blContext);

        MappedFieldType codeFieldType = mapperService.fieldType("field.code");
        assertEquals("long", codeFieldType.typeName());
        BlockLoader codeBlockLoader = codeFieldType.blockLoader(blContext);

        withLuceneIndex(mapperService, iw -> {
            iw.addDocument(
                mapperService.documentMapper()
                    .parse(
                        source(
                            b -> b.startObject("field")
                                .field("status", "ok")
                                .field("code", 200)
                                .field("unmapped_key", "some_value")
                                .endObject()
                        )
                    )
                    .rootDoc()
            );
            iw.addDocument(
                mapperService.documentMapper()
                    .parse(source(b -> b.startObject("field").field("status", "active").field("code", 201).endObject()))
                    .rootDoc()
            );
        }, reader -> {
            LeafReaderContext leaf = reader.leaves().get(0);

            BlockLoader.ColumnAtATimeReader unmappedReader = unmappedBlockLoader.columnAtATimeReader(leaf)
                .apply(newLimitedBreaker(ByteSizeValue.ofMb(1)));
            try {
                TestBlock block = (TestBlock) unmappedReader.read(TestBlock.factory(), TestBlock.docs(0, 1), 0, false);
                assertThat(block.size(), equalTo(2));
                assertThat(block.get(0), equalTo(new BytesRef("some_value")));
                assertNull("doc without the unmapped key should be null", block.get(1));
            } finally {
                unmappedReader.close();
            }

            BlockLoader.ColumnAtATimeReader statusReader = statusBlockLoader.columnAtATimeReader(leaf)
                .apply(newLimitedBreaker(ByteSizeValue.ofMb(1)));
            try {
                TestBlock block = (TestBlock) statusReader.read(TestBlock.factory(), TestBlock.docs(0, 1), 0, false);
                assertThat(block.size(), equalTo(2));
                assertThat(block.get(0), equalTo(new BytesRef("ok")));
                assertThat(block.get(1), equalTo(new BytesRef("active")));
            } finally {
                statusReader.close();
            }

            BlockLoader.ColumnAtATimeReader codeReader = codeBlockLoader.columnAtATimeReader(leaf)
                .apply(newLimitedBreaker(ByteSizeValue.ofMb(1)));
            try {
                TestBlock block = (TestBlock) codeReader.read(TestBlock.factory(), TestBlock.docs(0, 1), 0, false);
                assertThat(block.size(), equalTo(2));
                assertThat(block.get(0), equalTo(200L));
                assertThat(block.get(1), equalTo(201L));
            } finally {
                codeReader.close();
            }
        });
    }

    public void testBlockLoaderIncludesMappedProperties() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "flattened");
            b.startObject("properties");
            {
                b.startObject("status").field("type", "keyword").endObject();
                b.startObject("code").field("type", "long").endObject();
            }
            b.endObject();
        }));

        MappedFieldType fieldType = mapperService.fieldType("field");
        assertThat(fieldType, instanceOf(RootFlattenedFieldType.class));
        BlockLoader blockLoader = fieldType.blockLoader(null);
        assertThat(blockLoader, instanceOf(RootFlattenedDocValuesBlockLoader.class));

        withLuceneIndex(mapperService, iw -> {
            iw.addDocument(
                mapperService.documentMapper()
                    .parse(
                        source(
                            b -> b.startObject("field")
                                .field("status", "ok")
                                .field("code", 200)
                                .field("unmapped_key", "some_value")
                                .endObject()
                        )
                    )
                    .rootDoc()
            );
        }, reader -> {
            LeafReaderContext leaf = reader.leaves().get(0);
            BlockLoader.ColumnAtATimeReader columnReader = blockLoader.columnAtATimeReader(leaf)
                .apply(newLimitedBreaker(ByteSizeValue.ofMb(1)));
            try {
                TestBlock block = (TestBlock) columnReader.read(TestBlock.factory(), TestBlock.docs(0), 0, false);
                assertThat(block.size(), equalTo(1));

                BytesRef bytesRef = (BytesRef) block.get(0);
                assertNotNull("block loader should return non-null value for doc with mapped properties", bytesRef);

                Map<String, Object> parsed = XContentHelper.convertToMap(new BytesArray(bytesRef.utf8ToString()), false, XContentType.JSON)
                    .v2();

                assertThat("mapped keyword property must be present", parsed.get("status"), equalTo("ok"));
                assertThat("mapped long property must be present", parsed.get("code"), equalTo(200));
                assertThat("unmapped key must be present", parsed.get("unmapped_key"), equalTo("some_value"));
            } finally {
                columnReader.close();
            }
        });
    }

    public void testBlockLoaderWithMappedPropertiesOnly() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "flattened");
            b.startObject("properties");
            {
                b.startObject("status").field("type", "keyword").endObject();
            }
            b.endObject();
        }));

        BlockLoader blockLoader = mapperService.fieldType("field").blockLoader(null);

        withLuceneIndex(mapperService, iw -> {
            iw.addDocument(
                mapperService.documentMapper().parse(source(b -> b.startObject("field").field("status", "active").endObject())).rootDoc()
            );
        }, reader -> {
            LeafReaderContext leaf = reader.leaves().get(0);
            BlockLoader.ColumnAtATimeReader columnReader = blockLoader.columnAtATimeReader(leaf)
                .apply(newLimitedBreaker(ByteSizeValue.ofMb(1)));
            try {
                TestBlock block = (TestBlock) columnReader.read(TestBlock.factory(), TestBlock.docs(0), 0, false);
                assertThat(block.size(), equalTo(1));

                BytesRef bytesRef = (BytesRef) block.get(0);
                assertNotNull("block loader should not return null when only mapped properties have values", bytesRef);

                Map<String, Object> parsed = XContentHelper.convertToMap(new BytesArray(bytesRef.utf8ToString()), false, XContentType.JSON)
                    .v2();

                assertThat(parsed.get("status"), equalTo("active"));
            } finally {
                columnReader.close();
            }
        });
    }

    @Override
    protected boolean supportsCopyTo() {
        return false;
    }

    @Override
    public void assertStoredFieldsEquals(String info, IndexReader leftReader, IndexReader rightReader) throws IOException {
        assert leftReader.maxDoc() == rightReader.maxDoc();
        StoredFields leftStoredFields = leftReader.storedFields();
        StoredFields rightStoredFields = rightReader.storedFields();
        for (int i = 0; i < leftReader.maxDoc(); i++) {
            Document leftDoc = leftStoredFields.document(i);
            Document rightDoc = rightStoredFields.document(i);

            // Everything is from LuceneTestCase except this part.
            // LuceneTestCase sorts by name of the field only which results in a difference
            // between keyed ignored field values that have the same name.
            Comparator<IndexableField> comp = Comparator.comparing(IndexableField::name).thenComparing(IndexableField::binaryValue);
            List<IndexableField> leftFields = new ArrayList<>(leftDoc.getFields());
            List<IndexableField> rightFields = new ArrayList<>(rightDoc.getFields());
            Collections.sort(leftFields, comp);
            Collections.sort(rightFields, comp);

            Iterator<IndexableField> leftIterator = leftFields.iterator();
            Iterator<IndexableField> rightIterator = rightFields.iterator();
            while (leftIterator.hasNext()) {
                assertTrue(info, rightIterator.hasNext());
                assertStoredFieldEquals(info, leftIterator.next(), rightIterator.next());
            }
            assertFalse(info, rightIterator.hasNext());
        }
    }

    public void testMultiValueSortedSet() throws IOException {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        MapperService mapperService = createMapperService(
            fieldMapping(b -> b.field("type", "flattened").startObject("doc_values").field("multi_value", "sorted_set").endObject())
        );
        FlattenedFieldMapper mapper = (FlattenedFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper.fieldType().hasDocValues(), equalTo(true));
    }

    public void testMultiValueDefaultIsSortedSet() throws IOException {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "flattened")));
        FlattenedFieldMapper mapper = (FlattenedFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper.fieldType().hasDocValues(), equalTo(true));
    }

    public void testMultiValueSortedNotAllowed() throws IOException {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        var e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(
                fieldMapping(b -> b.field("type", "flattened").startObject("doc_values").field("multi_value", "sorted").endObject())
            )
        );
        assertThat(
            e.getMessage(),
            containsString("Unknown value [sorted] for field [multi_value] - accepted values are [no, sorted_set, arrays]")
        );
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
