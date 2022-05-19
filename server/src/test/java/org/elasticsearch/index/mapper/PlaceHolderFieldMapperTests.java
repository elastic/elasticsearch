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
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xcontent.XContentBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PlaceHolderFieldMapperTests extends MapperServiceTestCase {

    // checks that parameters of unknown field types are preserved on legacy indices
    public void testPreserveParams() throws Exception {
        XContentBuilder mapping = mapping(b -> {
            b.startObject("myfield");
            b.field("type", "unknown");
            b.field("someparam", "value");
            b.endObject();
        });
        MapperService service = createMapperService(Version.fromString("5.0.0"), Settings.EMPTY, () -> false, mapping);
        assertThat(service.fieldType("myfield"), instanceOf(PlaceHolderFieldMapper.PlaceHolderFieldType.class));
        assertEquals(Strings.toString(mapping), Strings.toString(service.documentMapper().mapping()));

        // check that field can be updated
        mapping = mapping(b -> {
            b.startObject("myfield");
            b.field("type", "unknown");
            b.field("someparam", "other");
            b.endObject();
        });
        merge(service, mapping);
        assertThat(service.fieldType("myfield"), instanceOf(PlaceHolderFieldMapper.PlaceHolderFieldType.class));
        assertEquals(Strings.toString(mapping), Strings.toString(service.documentMapper().mapping()));
    }

    public void testFetchValue() throws Exception {
        MapperService mapperService = createMapperService(Version.fromString("5.0.0"), fieldMapping(b -> b.field("type", "unknown")));
        withLuceneIndex(mapperService, iw -> {
            iw.addDocument(
                createMapperService(fieldMapping(b -> b.field("type", "keyword"))).documentMapper()
                    .parse(source(b -> b.field("field", "value")))
                    .rootDoc()
            );
        }, iw -> {
            SearchLookup lookup = new SearchLookup(mapperService::fieldType, fieldDataLookup());
            SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
            when(searchExecutionContext.lookup()).thenReturn(lookup);
            when(searchExecutionContext.sourcePath("field")).thenReturn(Set.of("field"));
            ValueFetcher valueFetcher = mapperService.fieldType("field").valueFetcher(searchExecutionContext, null);
            IndexSearcher searcher = newSearcher(iw);
            LeafReaderContext context = searcher.getIndexReader().leaves().get(0);
            lookup.source().setSegmentAndDocument(context, 0);
            valueFetcher.setNextReader(context);
            assertEquals(List.of("value"), valueFetcher.fetchValues(lookup.source(), new ArrayList<>()));
        });
    }
}
