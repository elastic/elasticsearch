/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.SearchLookupAware;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

abstract class AbstractScriptMappedFieldTypeTestCase extends ESTestCase {
    public abstract void testDocValues() throws IOException;

    public abstract void testSort() throws IOException;

    public abstract void testUsedInScript() throws IOException;

    public abstract void testExistsQuery() throws IOException;

    public abstract void testExistsQueryIsExpensive() throws IOException;

    public abstract void testRangeQuery() throws IOException;

    public abstract void testRangeQueryIsExpensive() throws IOException;

    public abstract void testTermQuery() throws IOException;

    public abstract void testTermQueryIsExpensive() throws IOException;

    public abstract void testTermsQuery() throws IOException;

    public abstract void testTermsQueryIsExpensive() throws IOException;

    protected static QueryShardContext mockContext() {
        return mockContext(true);
    }

    protected static QueryShardContext mockContext(boolean allowExpensiveQueries) {
        return mockContext(allowExpensiveQueries, null);
    }

    protected static QueryShardContext mockContext(boolean allowExpensiveQueries, AbstractScriptMappedFieldType mappedFieldType) {
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.fieldType(anyString())).thenReturn(mappedFieldType);
        QueryShardContext context = mock(QueryShardContext.class);
        when(context.getMapperService()).thenReturn(mapperService);
        if (mappedFieldType != null) {
            when(context.fieldMapper(anyString())).thenReturn(mappedFieldType);
            when(context.getSearchAnalyzer(any())).thenReturn(mappedFieldType.getTextSearchInfo().getSearchAnalyzer());
        }
        when(context.allowExpensiveQueries()).thenReturn(allowExpensiveQueries);
        SearchLookup lookup = new SearchLookup(mapperService, mft -> {
            IndexFieldData<?> ifd = mft.fielddataBuilder("test").build(null, null, mapperService);
            if (ifd instanceof SearchLookupAware) {
                ((SearchLookupAware) ifd).setSearchLookup(context.lookup());
            }
            return ifd;
        });
        when(context.lookup()).thenReturn(lookup);
        return context;
    }
}
