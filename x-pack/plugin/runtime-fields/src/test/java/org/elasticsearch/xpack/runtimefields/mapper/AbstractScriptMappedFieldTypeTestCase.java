/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

abstract class AbstractScriptMappedFieldTypeTestCase extends ESTestCase {
    protected QueryShardContext mockContext() {
        return mockContext(true);
    }

    protected QueryShardContext mockContext(boolean allowExpensiveQueries) {
        MapperService mapperService = mock(MapperService.class);
        QueryShardContext context = mock(QueryShardContext.class);
        when(context.allowExpensiveQueries()).thenReturn(allowExpensiveQueries);
        when(context.lookup()).thenReturn(new SearchLookup(mapperService, mft -> null));
        return context;
    }

}
