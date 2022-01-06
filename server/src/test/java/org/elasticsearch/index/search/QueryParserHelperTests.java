/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.search;

import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;

public class QueryParserHelperTests extends MapperServiceTestCase {

    public void testEmptyFieldResolution() throws IOException {

        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("field1").field("type", "text").endObject();
            b.startObject("field2").field("type", "text").endObject();
        }));

        // We check that expanded fields are actually present in the underlying lucene index,
        // so a resolution against an empty index will result in 0 fields
        SearchExecutionContext context = createSearchExecutionContext(mapperService);
        {
            Map<String, Float> resolvedFields = QueryParserHelper.resolveMappingFields(context, Map.of("*", 1.0f));
            assertTrue(resolvedFields.isEmpty());
        }

        // If you ask for a specific mapped field, we will resolve it
        {
            Map<String, Float> resolvedFields = QueryParserHelper.resolveMappingFields(context, Map.of("field1", 1.0f));
            assertThat(resolvedFields.keySet(), containsInAnyOrder("field1"));
        }

        // But unmapped fields will not be resolved
        {
            Map<String, Float> resolvedFields = QueryParserHelper.resolveMappingFields(context, Map.of("unmapped", 1.0f));
            assertTrue(resolvedFields.isEmpty());
        }
    }

    public void testIndexedFieldResolution() throws IOException {

        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("field1").field("type", "text").endObject();
            b.startObject("field2").field("type", "text").endObject();
            b.startObject("field3").field("type", "text").endObject();
        }));

        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> {
            b.field("field1", "foo");
            b.field("field2", "bar");
        }));

        withLuceneIndex(mapperService, iw -> iw.addDocument(doc.rootDoc()), ir -> {

            SearchExecutionContext context = createSearchExecutionContext(mapperService, new IndexSearcher(ir));

            // field1 and field2 are present in the index, so they get resolved; field3 is in the mappings but
            // not in the actual index, so it is ignored
            {
                Map<String, Float> resolvedFields = QueryParserHelper.resolveMappingFields(context, Map.of("field*", 1.0f));
                assertThat(resolvedFields.keySet(), containsInAnyOrder("field1", "field2"));
                assertFalse(resolvedFields.containsKey("field3"));
            }

            // unmapped fields get ignored
            {
                Map<String, Float> resolvedFields = QueryParserHelper.resolveMappingFields(context, Map.of("*", 1.0f, "unmapped", 2.0f));
                assertThat(resolvedFields.keySet(), containsInAnyOrder("field1", "field2"));
                assertFalse(resolvedFields.containsKey("unmapped"));
            }

            {
                Map<String, Float> resolvedFields = QueryParserHelper.resolveMappingFields(context, Map.of("unmapped", 1.0f));
                assertTrue(resolvedFields.isEmpty());
            }
        });
    }

    public void testFieldExpansionAboveLimitThrowsException() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            for (int i = 0; i < 10; i++) {
                b.startObject("field" + i).field("type", "long").endObject();
            }
        }));
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> {
            for (int i = 0; i < 10; i++) {
                b.field("field" + i, 1L);
            }
        }));

        withLuceneIndex(mapperService, iw -> iw.addDocument(doc.rootDoc()), ir -> {
            SearchExecutionContext context = createSearchExecutionContext(mapperService, new IndexSearcher(ir));

            int originalMaxClauseCount = IndexSearcher.getMaxClauseCount();
            try {
                IndexSearcher.setMaxClauseCount(4);
                Exception e = expectThrows(
                    IllegalArgumentException.class,
                    () -> QueryParserHelper.resolveMappingFields(context, Map.of("field*", 1.0f))
                );
                assertThat(e.getMessage(), containsString("field expansion matches too many fields"));

                IndexSearcher.setMaxClauseCount(10);
                assertThat(QueryParserHelper.resolveMappingFields(context, Map.of("field*", 1.0f)).keySet(), hasSize(10));
            } finally {
                IndexSearcher.setMaxClauseCount(originalMaxClauseCount);
            }
        });
    }
}
