/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class VersionFieldMapperTests extends MapperServiceTestCase {

    public void testIncludeInObjectNotAllowed() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {}));

        Exception e = expectThrows(MapperParsingException.class, () -> docMapper.parse(source(b -> b.field("_version", 1))));

        assertThat(e.getCause().getMessage(), containsString("Field [_version] is a metadata field and cannot be added inside a document"));
    }

    public void testDefaults() throws IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        ParsedDocument document = mapper.parse(source(b -> b.field("field", "value")));
        IndexableField[] fields = document.rootDoc().getFields(VersionFieldMapper.NAME);
        assertEquals(1, fields.length);
        assertEquals(IndexOptions.NONE, fields[0].fieldType().indexOptions());
        assertEquals(DocValuesType.NUMERIC, fields[0].fieldType().docValuesType());
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
            SearchLookup lookup = new SearchLookup(mapperService::fieldType, fieldDataLookup());
            SearchExecutionContext searchExecutionContext = createSearchExecutionContext(mapperService);
            ValueFetcher valueFetcher = ft.valueFetcher(searchExecutionContext, null);
            IndexSearcher searcher = newSearcher(iw);
            LeafReaderContext context = searcher.getIndexReader().leaves().get(0);
            lookup.source().setSegmentAndDocument(context, 0);
            valueFetcher.setNextReader(context);
            assertEquals(List.of(version), valueFetcher.fetchValues(lookup.source(), Collections.emptyList()));
        });
    }

}
