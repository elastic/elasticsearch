/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RoutingFieldMapperTests extends MetadataMapperTestCase {

    @Override
    protected String fieldName() {
        return RoutingFieldMapper.NAME;
    }

    @Override
    protected boolean isConfigurable() {
        return true;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("required", b -> b.field("required", true));
        checker.registerConflictCheck("doc_values", b -> b.field("doc_values", true));
    }

    public void testRoutingMapper() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {}));

        ParsedDocument doc = docMapper.parse(
            new SourceToParse(
                "1",
                BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", "value").endObject()),
                XContentType.JSON,
                "routing_value"
            )
        );

        assertThat(doc.rootDoc().get("_routing"), equalTo("routing_value"));
        assertThat(doc.rootDoc().get("field"), equalTo("value"));
    }

    public void testIncludeInObjectNotAllowed() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {}));
        Exception e = expectThrows(DocumentParsingException.class, () -> docMapper.parse(source(b -> b.field("_routing", "foo"))));

        assertThat(e.getCause().getMessage(), containsString("Field [_routing] is a metadata field and cannot be added inside a document"));
    }

    public void testFetchRoutingFieldValue() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {}));
        withLuceneIndex(
            mapperService,
            iw -> { iw.addDocument(mapperService.documentMapper().parse(source("1", b -> {}, "abcd")).rootDoc()); },
            iw -> {
                SearchLookup lookup = new SearchLookup(mapperService::fieldType, fieldDataLookup(mapperService), (ctx, doc) -> null);
                SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
                when(searchExecutionContext.lookup()).thenReturn(lookup);
                RoutingFieldMapper.RoutingFieldType ft = (RoutingFieldMapper.RoutingFieldType) mapperService.fieldType("_routing");
                ValueFetcher valueFetcher = ft.valueFetcher(searchExecutionContext, null);
                IndexSearcher searcher = newSearcher(iw);
                LeafReaderContext context = searcher.getIndexReader().leaves().get(0);
                valueFetcher.setNextReader(context);
                assertEquals(List.of("abcd"), valueFetcher.fetchValues(Source.empty(XContentType.JSON), 0, new ArrayList<>()));
            }
        );
    }

    public void testDocValuesDefaultIsFalse() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {}));
        RoutingFieldMapper mapper = (RoutingFieldMapper) docMapper.mappers().getMapper("_routing");
        assertNotNull(mapper);
        assertFalse("doc_values should default to false", mapper.docValues());
    }

    public void testDocValuesCanBeEnabled() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(topMapping(b -> b.startObject("_routing").field("doc_values", true).endObject()));
        RoutingFieldMapper mapper = (RoutingFieldMapper) docMapper.mappers().getMapper("_routing");
        assertNotNull(mapper);
        assertTrue("doc_values should be true when configured", mapper.docValues());
    }

    public void testDocValuesEnabledIfIndexModeIsColumnar() throws Exception {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        var mapperService = createMapperService(Settings.builder().put("index.mode", "columnar").build(), topMapping(b -> {}));
        DocumentMapper docMapper = mapperService.documentMapper();
        RoutingFieldMapper mapper = (RoutingFieldMapper) docMapper.mappers().getMapper("_routing");
        assertNotNull(mapper);
        assertTrue("doc_values should be true when configured", mapper.docValues());
    }

    public void testDocValuesRoutingStoresAsSortedDocValues() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(topMapping(b -> b.startObject("_routing").field("doc_values", true).endObject()));

        ParsedDocument doc = docMapper.parse(
            new SourceToParse(
                "1",
                BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", "value").endObject()),
                XContentType.JSON,
                "routing_value"
            )
        );

        List<IndexableField> routingFields = doc.rootDoc().getFields("_routing");
        // expect exactly one field: sorted doc values with skip index, no inverted index
        assertEquals("expected exactly one _routing field (doc values only)", 1, routingFields.size());

        IndexableField dvField = routingFields.get(0);
        assertEquals("doc values type must be SORTED", DocValuesType.SORTED, dvField.fieldType().docValuesType());
        assertEquals("must have no inverted index", IndexOptions.NONE, dvField.fieldType().indexOptions());
        assertFalse("must not be stored", dvField.fieldType().stored());
    }

    public void testDocValuesRoutingNoValue() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(topMapping(b -> b.startObject("_routing").field("doc_values", true).endObject()));

        ParsedDocument doc = docMapper.parse(
            new SourceToParse(
                "1",
                BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", "value").endObject()),
                XContentType.JSON,
                null // no routing
            )
        );

        assertEquals("no routing fields when routing value is absent", 0, doc.rootDoc().getFields("_routing").size());
    }

    public void testBuilderReturnsExpectedSingleton() {
        for (boolean requiredByDefault : new boolean[] { false, true }) {
            for (boolean docValuesEnabledByDefault : new boolean[] { false, true }) {
                for (boolean required : new boolean[] { false, true }) {
                    for (boolean docValues : new boolean[] { false, true }) {
                        String desc = "requiredByDefault="
                            + requiredByDefault
                            + " docValuesEnabledByDefault="
                            + docValuesEnabledByDefault
                            + " required="
                            + required
                            + " docValues="
                            + docValues;

                        RoutingFieldMapper first = buildRoutingMapper(requiredByDefault, docValuesEnabledByDefault, required, docValues);
                        RoutingFieldMapper second = buildRoutingMapper(requiredByDefault, docValuesEnabledByDefault, required, docValues);
                        assertSame(desc + " (two builds should return the same singleton)", first, second);
                        assertSame(
                            desc + " (build must match InstancesLookup)",
                            RoutingFieldMapper.InstancesLookup.lookup(requiredByDefault, required, docValuesEnabledByDefault, docValues),
                            first
                        );
                    }
                }
            }
        }
    }

    private static RoutingFieldMapper buildRoutingMapper(
        boolean requiredByDefault,
        boolean docValuesEnabledByDefault,
        boolean required,
        boolean docValues
    ) {
        RoutingFieldMapper.Builder builder = new RoutingFieldMapper.Builder(requiredByDefault, docValuesEnabledByDefault);
        builder.required.setValue(required);
        builder.docValues.setValue(docValues);
        return builder.build();
    }

    public void testFetchDocValuesRoutingFieldValue() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> b.startObject("_routing").field("doc_values", true).endObject()));
        withLuceneIndex(
            mapperService,
            iw -> { iw.addDocument(mapperService.documentMapper().parse(source("1", b -> {}, "abcd")).rootDoc()); },
            iw -> {
                SearchExecutionContext searchExecutionContext = createSearchExecutionContext(mapperService, newSearcher(iw));
                RoutingFieldMapper.RoutingFieldType ft = (RoutingFieldMapper.RoutingFieldType) mapperService.fieldType("_routing");
                ValueFetcher valueFetcher = ft.valueFetcher(searchExecutionContext, null);
                IndexSearcher searcher = newSearcher(iw);
                LeafReaderContext context = searcher.getIndexReader().leaves().get(0);
                valueFetcher.setNextReader(context);
                assertEquals(List.of("abcd"), valueFetcher.fetchValues(Source.empty(XContentType.JSON), 0, new ArrayList<>()));
            }
        );
    }

    public void testSliceEnabledWritesRoutingOnNestedDocuments() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());

        Settings settings = Settings.builder().put(getIndexSettings()).put(IndexSettings.SLICE_ENABLED.getKey(), true).build();
        MapperService mapperService = createMapperService(settings, mapping(b -> {
            b.startObject("n");
            b.field("type", "nested");
            b.startObject("properties");
            b.startObject("k").field("type", "keyword").endObject();
            b.endObject();
            b.endObject();
        }));

        ParsedDocument doc = mapperService.documentMapper().parse(source("1", b -> {
            b.startArray("n");
            b.startObject().field("k", "v1").endObject();
            b.startObject().field("k", "v2").endObject();
            b.endArray();
        }, "routing_value"));

        assertEquals("expected nested docs and one root doc", 3, doc.docs().size());
        for (LuceneDocument luceneDocument : doc.docs()) {
            assertRoutingStoredAsDocValues(luceneDocument, "routing_value");
        }
    }

    public void testSliceEnabledIncludeInParentDoesNotDuplicateRootRouting() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());

        Settings settings = Settings.builder().put(getIndexSettings()).put(IndexSettings.SLICE_ENABLED.getKey(), true).build();
        MapperService mapperService = createMapperService(settings, mapping(b -> {
            b.startObject("n");
            b.field("type", "nested");
            b.field("include_in_parent", true);
            b.startObject("properties");
            b.startObject("k").field("type", "keyword").endObject();
            b.endObject();
            b.endObject();
        }));

        ParsedDocument doc = mapperService.documentMapper().parse(source("1", b -> {
            b.startArray("n");
            b.startObject().field("k", "v1").endObject();
            b.startObject().field("k", "v2").endObject();
            b.endArray();
        }, "routing_value"));

        for (int i = 0; i < doc.docs().size() - 1; i++) {
            assertRoutingStoredAsDocValues(doc.docs().get(i), "routing_value");
        }
        assertRoutingStoredAsDocValues(doc.rootDoc(), "routing_value");
    }

    private static void assertRoutingStoredAsDocValues(LuceneDocument document, String routing) {
        List<IndexableField> routingFields = document.getFields(RoutingFieldMapper.NAME);
        assertEquals("expected exactly one _routing field", 1, routingFields.size());
        IndexableField routingField = routingFields.get(0);
        assertEquals("routing value mismatch", routing, routingField.binaryValue().utf8ToString());
        assertEquals("doc values type must be SORTED", DocValuesType.SORTED, routingField.fieldType().docValuesType());
        assertEquals("must have no inverted index", IndexOptions.NONE, routingField.fieldType().indexOptions());
        assertFalse("must not be stored", routingField.fieldType().stored());
    }
}
