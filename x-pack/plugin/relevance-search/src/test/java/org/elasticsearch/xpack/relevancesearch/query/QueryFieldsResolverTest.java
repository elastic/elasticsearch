/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.query;

import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryFieldsResolverTest extends ESTestCase {

    private static SearchExecutionContext createSearchExecutionContext(Map<String, String> fieldNamesToTypes) {
        final SearchExecutionContext context = mock(SearchExecutionContext.class);

        when(context.getMatchingFieldNames("*")).thenReturn(fieldNamesToTypes.keySet());
        fieldNamesToTypes.forEach((String fieldName, String fieldType) -> {
            final MappedFieldType mappedFieldType = createMappedFieldType(fieldType);
            when(context.getFieldType(fieldName)).thenReturn(mappedFieldType);
        });

        return context;
    }

    private static MappedFieldType createMappedFieldType(String typeName) {
        MappedFieldType fieldType = mock(MappedFieldType.class);
        when(fieldType.typeName()).thenReturn(typeName);
        when(fieldType.isSearchable()).thenReturn(true);
        if ("text".equals(typeName)) {
            NamedAnalyzer namedAnalyzer = new NamedAnalyzer("analyzer_name", AnalyzerScope.INDEX, null);
            TextSearchInfo textSearchInfo = new TextSearchInfo(null, null, namedAnalyzer, namedAnalyzer);
            when(fieldType.getTextSearchInfo()).thenReturn(textSearchInfo);
        }
        return fieldType;
    }

    public void testRetrievesSearchableTextFields() {

        Map<String, String> fieldNamesToTypes = Map.of("textField1", "text", "geoLocationField", "geo_location", "textField2", "text");

        final Set<String> queryFields = QueryFieldsResolver.getQueryFields(createSearchExecutionContext((fieldNamesToTypes)));
        assertEquals(Set.of("textField1", "textField2"), queryFields);
    }

}
