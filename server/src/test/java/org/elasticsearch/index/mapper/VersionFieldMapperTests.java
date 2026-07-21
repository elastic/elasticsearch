/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.column.Column;
import org.apache.lucene.document.column.LongColumn;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.sourcebatch.MappedColumns;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class VersionFieldMapperTests extends MetadataMapperTestCase {

    @Override
    protected String fieldName() {
        return VersionFieldMapper.NAME;
    }

    @Override
    protected boolean isConfigurable() {
        return false;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {}

    public void testIncludeInObjectNotAllowed() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {}));

        Exception e = expectThrows(DocumentParsingException.class, () -> docMapper.parse(source(b -> b.field("_version", 1))));

        assertThat(e.getCause().getMessage(), containsString("Field [_version] is a metadata field and cannot be added inside a document"));
    }

    public void testDefaults() throws IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        ParsedDocument document = mapper.parse(source(b -> b.field("field", "value")));
        List<IndexableField> fields = document.rootDoc().getFields(VersionFieldMapper.NAME);
        assertEquals(1, fields.size());
        assertEquals(IndexOptions.NONE, fields.get(0).fieldType().indexOptions());
        assertEquals(DocValuesType.NUMERIC, fields.get(0).fieldType().docValuesType());
    }

    public void testFetchFieldValue() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "keyword")));
        long version = randomLongBetween(1, 1000);
        withLuceneIndex(mapperService, iw -> {
            ParsedDocument parsedDoc = mapperService.documentMapper().parse(source(b -> b.field("field", "value")));
            parsedDoc.version().setLongValue(version);
            iw.addDocument(parsedDoc.rootDoc());
        }, iw -> {
            VersionFieldMapper.VersionFieldType ft = (VersionFieldMapper.VersionFieldType) mapperService.fieldType("_version");
            SearchLookup lookup = new SearchLookup(mapperService::fieldType, fieldDataLookup(mapperService), (ctx, doc) -> null);
            SearchExecutionContext searchExecutionContext = createSearchExecutionContext(mapperService);
            ValueFetcher valueFetcher = ft.valueFetcher(searchExecutionContext, null);
            IndexSearcher searcher = newSearcher(iw);
            LeafReaderContext context = searcher.getIndexReader().leaves().get(0);
            valueFetcher.setNextReader(context);
            assertEquals(List.of(version), valueFetcher.fetchValues(Source.empty(XContentType.JSON), 0, Collections.emptyList()));
        });
    }

    public void testColumnarParseRegistersVersionColumn() throws Exception {
        MapperService mapperService = createMapperService(mapping(b -> {}));
        VersionFieldMapper mapper = (VersionFieldMapper) mapperService.documentMapper().mappers().getMapper(VersionFieldMapper.NAME);
        assertNotNull(mapper);
        assertTrue("supportsColumnarParse must be true for _version", mapper.supportsColumnarParse(mapperService.getIndexSettings()));

        IndexRequest[] requests = new IndexRequest[] { new IndexRequest("index").id("1"), new IndexRequest("index").id("2") };
        BatchMappingContext context = new BatchMappingContext(requests, mapperService.mappingLookup(), mapperService.getIndexSettings());

        mapper.preColumnarParse(context);

        final MappedColumns mappedColumns = context.columns();
        Column versionColumn = null;
        for (Column column : mappedColumns.toColumnBatch().columns()) {
            if (column.name().equals(VersionFieldMapper.NAME)) {
                versionColumn = column;
            }
        }
        assertNotNull("expected a _version column", versionColumn);
        assertEquals("doc values type must be NUMERIC", DocValuesType.NUMERIC, versionColumn.fieldType().docValuesType());
        assertEquals("must have no inverted index", IndexOptions.NONE, versionColumn.fieldType().indexOptions());
        assertFalse("must not be stored", versionColumn.fieldType().stored());

        // The engine writes values later; the column should initially yield the default (0L) for each doc.
        LongColumn longColumn = (LongColumn) versionColumn;
        var cursor = longColumn.tuples();
        assertEquals(0, cursor.nextDoc());
        assertEquals(0L, cursor.longValue());
        assertEquals(1, cursor.nextDoc());
        assertEquals(0L, cursor.longValue());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, cursor.nextDoc());
    }

}
