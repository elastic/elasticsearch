/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.lookup.SourceProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProvidedIdFieldMapperTests extends MapperServiceTestCase {

    public void testIncludeInObjectNotAllowed() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {}));

        Exception e = expectThrows(DocumentParsingException.class, () -> docMapper.parse(source(b -> b.field("_id", 1))));

        assertThat(e.getCause().getMessage(), containsString("Field [_id] is a metadata field and cannot be added inside a document"));
    }

    public void testDefaults() throws IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        ParsedDocument document = mapper.parse(source(b -> {}));
        List<IndexableField> fields = document.rootDoc().getFields(IdFieldMapper.NAME);
        assertEquals(1, fields.size());
        assertEquals(IndexOptions.DOCS, fields.get(0).fieldType().indexOptions());
        assertTrue(fields.get(0).fieldType().stored());
        assertEquals(Uid.encodeId("1"), fields.get(0).binaryValue());
    }

    public void testEnableFieldData() throws IOException {
        boolean[] enabled = new boolean[1];

        MapperService mapperService = createMapperService(() -> enabled[0], mapping(b -> {}));
        ProvidedIdFieldMapper.IdFieldType ft = (ProvidedIdFieldMapper.IdFieldType) mapperService.fieldType("_id");

        IllegalArgumentException exc = expectThrows(
            IllegalArgumentException.class,
            () -> ft.fielddataBuilder(FieldDataContext.noRuntimeFields("index", "test")).build(null, null)
        );
        assertThat(exc.getMessage(), containsString(IndicesService.INDICES_ID_FIELD_DATA_ENABLED_SETTING.getKey()));
        assertFalse(ft.isAggregatable());

        enabled[0] = true;
        ft.fielddataBuilder(FieldDataContext.noRuntimeFields("index", "test")).build(null, null);
        assertWarnings(ProvidedIdFieldMapper.ID_FIELD_DATA_DEPRECATION_MESSAGE);
        assertTrue(ft.isAggregatable());
    }

    public void testFetchIdFieldValue() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "keyword")));
        String id = randomAlphaOfLength(12);
        withLuceneIndex(mapperService, iw -> {
            iw.addDocument(mapperService.documentMapper().parse(source(id, b -> b.field("field", "value"), null)).rootDoc());
        }, iw -> {
            SearchLookup lookup = new SearchLookup(
                mapperService::fieldType,
                fieldDataLookup(mapperService),
                SourceProvider.fromLookup(mapperService.mappingLookup(), null, mapperService.getMapperMetrics().sourceFieldMetrics())
            );
            SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
            when(searchExecutionContext.lookup()).thenReturn(lookup);
            ProvidedIdFieldMapper.IdFieldType ft = (ProvidedIdFieldMapper.IdFieldType) mapperService.fieldType("_id");
            ValueFetcher valueFetcher = ft.valueFetcher(searchExecutionContext, null);
            IndexSearcher searcher = newSearcher(iw);
            LeafReaderContext context = searcher.getIndexReader().leaves().get(0);
            Source source = lookup.getSource(context, 0);
            valueFetcher.setNextReader(context);
            assertEquals(List.of(id), valueFetcher.fetchValues(source, 0, new ArrayList<>()));
        });
    }

    public void testSourceDescription() throws IOException {
        String id = randomAlphaOfLength(4);
        assertThat(
            ProvidedIdFieldMapper.DOCUMENT_ID_NO_FIELD_DATA.documentDescription(
                new TestDocumentParserContext(MappingLookup.EMPTY, source(id, b -> {}, randomAlphaOfLength(2)))
            ),
            equalTo("document with id '" + id + "'")
        );
    }

    public void testParsedDescription() throws IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        String id = randomAlphaOfLength(4);
        ParsedDocument document = mapper.parse(source(id, b -> {}, null));
        assertThat(ProvidedIdFieldMapper.DOCUMENT_ID_NO_FIELD_DATA.documentDescription(document), equalTo("[" + id + "]"));
    }

    public void testColumnarModeStoresBinaryDocValues() throws IOException {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.startObject("_id").field("mode", "columnar").endObject()));
        String id = randomAlphaOfLength(12);
        ParsedDocument document = mapper.parse(source(id, b -> {}, null));

        List<IndexableField> idFields = document.rootDoc().getFields(IdFieldMapper.NAME);
        assertEquals(2, idFields.size());

        IndexableField invertedIndexField = idFields.stream()
            .filter(f -> f.fieldType().docValuesType() == DocValuesType.NONE)
            .findFirst()
            .orElseThrow();
        assertEquals(IndexOptions.DOCS, invertedIndexField.fieldType().indexOptions());
        assertFalse("_id should not be stored in columnar mode", invertedIndexField.fieldType().stored());

        IndexableField docValuesField = idFields.stream()
            .filter(f -> f.fieldType().docValuesType() == DocValuesType.BINARY)
            .findFirst()
            .orElseThrow();
        assertThat(docValuesField, instanceOf(BinaryDocValuesField.class));
        assertEquals(Uid.encodeId(id), docValuesField.binaryValue());
    }

    public void testColumnarModeMappingSerialization() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> b.startObject("_id").field("mode", "columnar").endObject()));
        String mapping = mapperService.documentMapper().mapping().toString();
        assertThat(mapping, containsString("\"mode\":\"columnar\""));
    }

    public void testDefaultModeNotSerialized() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {}));
        String mapping = mapperService.documentMapper().mapping().toString();
        assertFalse("default mode should not be serialized", mapping.contains("\"mode\""));
    }

    public void testColumnarModeNotUpdateable() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {}));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> merge(mapperService, topMapping(b -> b.startObject("_id").field("mode", "columnar").endObject()))
        );
        assertThat(e.getMessage(), containsString("Cannot update parameter [mode]"));
    }

    public void testColumnarModeIdLoaderUsesDocValues() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> b.startObject("_id").field("mode", "columnar").endObject()));
        IdLoader idLoader = IdLoader.create(mapperService.getIndexSettings(), mapperService.mappingLookup());
        assertThat(idLoader, instanceOf(IdLoader.DocValuesIdLoader.class));
    }

    public void testDefaultModeIdLoaderUsesStoredFields() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {}));
        IdLoader idLoader = IdLoader.create(mapperService.getIndexSettings(), mapperService.mappingLookup());
        assertThat(idLoader, instanceOf(IdLoader.StoredIdLoader.class));
    }
}
