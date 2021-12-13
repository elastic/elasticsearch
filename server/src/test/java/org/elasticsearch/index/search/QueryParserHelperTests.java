/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.search;

import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;

public class QueryParserHelperTests extends MapperServiceTestCase {

    public void testUnmappedFieldsDoNotContributeToFieldCount() throws IOException {

        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("field1").field("type", "text").endObject();
            b.startObject("field2").field("type", "text").endObject();
        }));

        SearchExecutionContext context = createSearchExecutionContext(mapperService);
        {
            Map<String, Float> resolvedFields = QueryParserHelper.resolveMappingFields(context, Map.of("*", 1.0f));
            assertThat(resolvedFields.keySet(), containsInAnyOrder("field1", "field2"));
        }

        {
            Map<String, Float> resolvedFields = QueryParserHelper.resolveMappingFields(context, Map.of("*", 1.0f, "unmapped", 2.0f));
            assertThat(resolvedFields.keySet(), containsInAnyOrder("field1", "field2"));
            assertFalse(resolvedFields.containsKey("unmapped"));
        }

        {
            Map<String, Float> resolvedFields = QueryParserHelper.resolveMappingFields(context, Map.of("unmapped", 1.0f));
            assertTrue(resolvedFields.isEmpty());
        }
    }
}
