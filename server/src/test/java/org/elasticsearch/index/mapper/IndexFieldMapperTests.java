/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class IndexFieldMapperTests extends MapperServiceTestCase {

    public void testDefaultDisabledIndexMapper() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {}));
        ParsedDocument doc = docMapper.parse(source(b -> b.field("field", "value")));
        assertThat(doc.rootDoc().get("_index"), nullValue());
        assertThat(doc.rootDoc().get("field"), equalTo("value"));
    }

    public void testIndexNotConfigurable() {
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(topMapping(b -> b.startObject("_index").endObject()))
        );
        assertThat(e.getMessage(), containsString("_index is not configurable"));
    }

    public void testFetchFieldValue() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "keyword")));
        String index = mapperService.index().getName();
        withLuceneIndex(mapperService, iw -> {
            SourceToParse source = source(index, "id", b -> b.field("field", "value"), "", Map.of());
            iw.addDocument(mapperService.documentMapper().parse(source).rootDoc());
        }, iw -> {
            IndexFieldMapper.IndexFieldType ft = (IndexFieldMapper.IndexFieldType) mapperService.fieldType("_index");
            SearchLookup lookup = new SearchLookup(
                mapperService::fieldType,
                fieldDataLookup(mapperService.mappingLookup()::sourcePaths),
                new SourceLookup.ReaderSourceProvider()
            );
            SearchExecutionContext searchExecutionContext = createSearchExecutionContext(mapperService);
            ValueFetcher valueFetcher = ft.valueFetcher(searchExecutionContext, null);
            IndexSearcher searcher = newSearcher(iw);
            LeafReaderContext context = searcher.getIndexReader().leaves().get(0);
            lookup.source().setSegmentAndDocument(context, 0);
            valueFetcher.setNextReader(context);
            assertEquals(List.of(index), valueFetcher.fetchValues(lookup.source(), Collections.emptyList()));
        });
    }

}
