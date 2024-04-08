/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IgnoredFieldMapperTests extends MetadataMapperTestCase {

    @Override
    protected String fieldName() {
        return IgnoredFieldMapper.NAME;
    }

    @Override
    protected boolean isConfigurable() {
        return false;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {}

    public void testIncludeInObjectNotAllowed() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {}));

        Exception e = expectThrows(DocumentParsingException.class, () -> docMapper.parse(source(b -> b.field("_ignored", 1))));

        assertThat(e.getCause().getMessage(), containsString("Field [_ignored] is a metadata field and cannot be added inside a document"));
    }

    public void testDefaults() throws IOException {
        DocumentMapper mapper = createDocumentMapper(
            mapping(b -> b.startObject("field").field("type", "keyword").field("ignore_above", 3).endObject())
        );
        ParsedDocument document = mapper.parse(source(b -> b.field("field", "value")));
        List<IndexableField> fields = document.rootDoc().getFields(IgnoredFieldMapper.NAME);
        assertEquals(1, fields.size());
        assertEquals(IndexOptions.DOCS, fields.get(0).fieldType().indexOptions());
        assertTrue(fields.get(0).fieldType().stored());
    }

    public void testFetchIgnoredFieldValue() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "keyword").field("ignore_above", 3)));
        withLuceneIndex(mapperService, iw -> {
            iw.addDocument(mapperService.documentMapper().parse(source(b -> b.field("field", "value"))).rootDoc());
        }, iw -> {
            SearchLookup lookup = new SearchLookup(mapperService::fieldType, fieldDataLookup(mapperService), (ctx, doc) -> null);
            SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
            when(searchExecutionContext.lookup()).thenReturn(lookup);
            IgnoredFieldMapper.IgnoredFieldType ft = (IgnoredFieldMapper.IgnoredFieldType) mapperService.fieldType("_ignored");
            ValueFetcher valueFetcher = ft.valueFetcher(searchExecutionContext, null);
            IndexSearcher searcher = newSearcher(iw);
            LeafReaderContext context = searcher.getIndexReader().leaves().get(0);
            valueFetcher.setNextReader(context);
            assertEquals(List.of("field"), valueFetcher.fetchValues(Source.empty(XContentType.JSON), 0, new ArrayList<>()));
        });
    }

}
