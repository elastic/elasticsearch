/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Base test case for subclasses of MappedFieldType */
public abstract class FieldTypeTestCase extends ESTestCase {

    public static final SearchExecutionContext MOCK_CONTEXT = createMockSearchExecutionContext(true);
    public static final SearchExecutionContext MOCK_CONTEXT_DISALLOW_EXPENSIVE = createMockSearchExecutionContext(false);

    protected SearchExecutionContext randomMockContext() {
        return randomFrom(MOCK_CONTEXT, MOCK_CONTEXT_DISALLOW_EXPENSIVE);
    }

    private static SearchExecutionContext createMockSearchExecutionContext(boolean allowExpensiveQueries) {
        SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
        when(searchExecutionContext.allowExpensiveQueries()).thenReturn(allowExpensiveQueries);
        when(searchExecutionContext.isSourceEnabled()).thenReturn(true);
        SourceLookup sourceLookup = mock(SourceLookup.class);
        SearchLookup searchLookup = mock(SearchLookup.class);
        when(searchLookup.source()).thenReturn(sourceLookup);
        when(searchExecutionContext.lookup()).thenReturn(searchLookup);
        when(searchExecutionContext.indexVersionCreated()).thenReturn(Version.CURRENT);
        return searchExecutionContext;
    }

    public static List<?> fetchSourceValue(MappedFieldType fieldType, Object sourceValue) throws IOException {
        return fetchSourceValue(fieldType, sourceValue, null);
    }

    public static List<?> fetchSourceValue(MappedFieldType fieldType, Object sourceValue, String format) throws IOException {
        String field = fieldType.name();
        SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
        when(searchExecutionContext.isSourceEnabled()).thenReturn(true);
        when(searchExecutionContext.sourcePath(field)).thenReturn(Set.of(field));

        ValueFetcher fetcher = fieldType.valueFetcher(searchExecutionContext, format);
        SourceLookup lookup = new SourceLookup(new SourceLookup.MapSourceProvider(Collections.singletonMap(field, sourceValue)));
        return fetcher.fetchValues(lookup, new ArrayList<>());
    }

    public static List<?> fetchSourceValues(MappedFieldType fieldType, Object... values) throws IOException {
        String field = fieldType.name();
        SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
        when(searchExecutionContext.isSourceEnabled()).thenReturn(true);
        when(searchExecutionContext.sourcePath(field)).thenReturn(Set.of(field));

        ValueFetcher fetcher = fieldType.valueFetcher(searchExecutionContext, null);
        SourceLookup lookup = new SourceLookup(new SourceLookup.MapSourceProvider(Collections.singletonMap(field, List.of(values))));
        return fetcher.fetchValues(lookup, new ArrayList<>());
    }
}
