/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.query;

import org.elasticsearch.common.Strings;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Retrieves relevance_match query fields to be used when none are specified in settings.
 */
public class QueryFieldsResolver {

    public static Set<String> getQueryFields(SearchExecutionContext context) {

        final Set<String> fieldNames = context.getMatchingFieldNames("*");

        // Group field names by analyzer
        Map<String, Set<String>> analyzerFields = new HashMap<>();
        for (String fieldName : fieldNames) {
            // TODO Skip subfields as of now. We'll need to understand what analyzers to use, or conventions for choosing multifields
            // correctly
            if (fieldName.contains(".")) {
                continue;
            }
            final MappedFieldType fieldType = context.getFieldType(fieldName);
            // Get searchable text fields
            if (fieldType.isSearchable() && "text".equals(fieldType.typeName())) {
                final String analyzerName = fieldType.getTextSearchInfo().searchAnalyzer().name();
                if (Strings.hasLength(analyzerName)) {
                    analyzerFields.computeIfAbsent(analyzerName, k -> new HashSet<>()).add(fieldName);
                }
            }
        }

        if (analyzerFields.isEmpty()) {
            return Collections.emptySet();
        }

        // Get field list that is longer
        return analyzerFields.entrySet()
            .stream()
            .max(Comparator.comparingInt((Entry<String, Set<String>> a) -> a.getValue().size()))
            .get()
            .getValue();
    }
}
