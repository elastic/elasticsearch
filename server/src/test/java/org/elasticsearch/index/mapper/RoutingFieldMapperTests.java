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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("required", b -> b.field("required", true));
    }

    public void testRoutingMapper() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {}));

        ParsedDocument doc = docMapper.parse(
            new SourceToParse(
                "1",
                BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", "value").endObject()),
                XContentType.JSON,
                "routing_value",
                Map.of()
            )
        );

        assertThat(doc.rootDoc().get("_routing"), equalTo("routing_value"));
        assertThat(doc.rootDoc().get("field"), equalTo("value"));
    }

    public void testIncludeInObjectNotAllowed() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {}));
        Exception e = expectThrows(MapperParsingException.class, () -> docMapper.parse(source(b -> b.field("_routing", "foo"))));

        assertThat(e.getCause().getMessage(), containsString("Field [_routing] is a metadata field and cannot be added inside a document"));
    }

    public void testFetchRoutingFieldValue() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {}));
        withLuceneIndex(
            mapperService,
            iw -> { iw.addDocument(mapperService.documentMapper().parse(source("1", b -> {}, "abcd")).rootDoc()); },
            iw -> {
                SearchLookup lookup = new SearchLookup(mapperService::fieldType, fieldDataLookup());
                SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
                when(searchExecutionContext.lookup()).thenReturn(lookup);
                RoutingFieldMapper.RoutingFieldType ft = (RoutingFieldMapper.RoutingFieldType) mapperService.fieldType("_routing");
                ValueFetcher valueFetcher = ft.valueFetcher(searchExecutionContext, null);
                IndexSearcher searcher = newSearcher(iw);
                LeafReaderContext context = searcher.getIndexReader().leaves().get(0);
                lookup.source().setSegmentAndDocument(context, 0);
                valueFetcher.setNextReader(context);
                assertEquals(List.of("abcd"), valueFetcher.fetchValues(lookup.source(), new ArrayList<>()));
            }
        );
    }
}
