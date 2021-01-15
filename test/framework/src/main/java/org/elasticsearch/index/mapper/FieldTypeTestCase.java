/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.mapper;

import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
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
        return searchExecutionContext;
    }

    public static List<?> fetchSourceValue(MappedFieldType fieldType, Object sourceValue) throws IOException {
        return fetchSourceValue(fieldType, sourceValue, null);
    }

    public static List<?> fetchSourceValue(MappedFieldType fieldType, Object sourceValue, String format) throws IOException {
        String field = fieldType.name();
        SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
        when(searchExecutionContext.sourcePath(field)).thenReturn(Set.of(field));

        ValueFetcher fetcher = fieldType.valueFetcher(searchExecutionContext, format);
        SourceLookup lookup = new SourceLookup();
        lookup.setSource(Collections.singletonMap(field, sourceValue));
        return fetcher.fetchValues(lookup);
    }
}
