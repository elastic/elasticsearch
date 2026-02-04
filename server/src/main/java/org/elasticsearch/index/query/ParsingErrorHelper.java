/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.elasticsearch.xcontent.XContentParser;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Helper class to generate more actionable error messages when parsing queries fails.
 * This improves user experience by providing clearer guidance on how to fix parsing errors.
 */
public final class ParsingErrorHelper {

    private ParsingErrorHelper() {
        // Utility class, no instances
    }

    /**
     * Creates an error message for an unsupported field in a query.
     * 
     * @param queryName The name of the query (e.g., "range", "multi_match")
     * @param fieldName The unsupported field name
     * @param supportedFields The list of supported fields for this query, or null if not available
     * @return A more actionable error message
     */
    public static String unsupportedFieldMessage(String queryName, String fieldName, Collection<String> supportedFields) {
        StringBuilder message = new StringBuilder();
        message.append("[").append(queryName).append("] query does not support [").append(fieldName).append("]");
        
        if (supportedFields != null && !supportedFields.isEmpty()) {
            message.append(". Supported fields are: ");
            message.append(supportedFields.stream()
                .sorted()
                .collect(Collectors.joining(", ", "[", "]")));
        }
        
        return message.toString();
    }

    /**
     * Creates an error message for malformed query structure.
     * 
     * @param queryName The name of the query
     * @param expectedToken What token was expected
     * @param foundToken What token was actually found
     * @param fieldName The current field name
     * @param suggestion Optional suggestion for fixing the issue
     * @return A more actionable error message
     */
    public static String malformedQueryMessage(
        String queryName,
        String expectedToken,
        String foundToken,
        String fieldName,
        String suggestion
    ) {
        StringBuilder message = new StringBuilder();
        message.append("[").append(queryName).append("] malformed query, expected [")
            .append(expectedToken).append("] but found [").append(foundToken).append("]");
        
        if (fieldName != null && !fieldName.isEmpty()) {
            message.append(" at field [").append(fieldName).append("]");
        }
        
        if (suggestion != null && !suggestion.isEmpty()) {
            message.append(". ").append(suggestion);
        }
        
        return message.toString();
    }

    /**
     * Creates an error message for field type mismatches in aggregations.
     * 
     * @param fieldName The field name
     * @param fieldType The actual field type
     * @param aggregationType The aggregation type
     * @param supportedTypes The supported field types for this aggregation
     * @return A more actionable error message
     */
    public static String unsupportedFieldTypeForAggregation(
        String fieldName,
        String fieldType,
        String aggregationType,
        Collection<String> supportedTypes
    ) {
        StringBuilder message = new StringBuilder();
        message.append("Field [").append(fieldName).append("] of type [").append(fieldType)
            .append("] is not supported for aggregation [").append(aggregationType).append("]");
        
        if (supportedTypes != null && !supportedTypes.isEmpty()) {
            message.append(". Supported field types are: ");
            message.append(supportedTypes.stream()
                .sorted()
                .collect(Collectors.joining(", ", "[", "]")));
        }
        
        return message.toString();
    }

    /**
     * Creates a suggestion message for common JSON structure errors.
     * 
     * @param token The current token from the parser
     * @param context The parsing context (e.g., "range query field definition")
     * @return A suggestion for fixing the JSON structure
     */
    public static String getJsonStructureSuggestion(XContentParser.Token token, String context) {
        if (token == XContentParser.Token.VALUE_STRING || token == XContentParser.Token.VALUE_NUMBER) {
            return "Did you mean to wrap this value in an object? For " + context + 
                   ", you need to specify an object with properties like 'gte', 'lte', etc.";
        } else if (token == XContentParser.Token.FIELD_NAME) {
            return "Unexpected field found. Check that this field is placed within the correct query object structure.";
        }
        return null;
    }

    /**
     * Common supported fields for various query types.
     * This can be expanded as needed for different query types.
     */
    public static class CommonFields {
        public static final Set<String> RANGE_QUERY_FIELDS = Set.of(
            "gte", "gt", "lte", "lt", "boost", "format", "time_zone", "relation", "_name"
        );
        
        public static final Set<String> MULTI_MATCH_QUERY_FIELDS = Set.of(
            "query", "fields", "type", "analyzer", "boost", "slop", "fuzziness",
            "prefix_length", "max_expansions", "operator", "minimum_should_match",
            "fuzzy_rewrite", "tie_breaker", "lenient", "zero_terms_query",
            "_name", "auto_generate_synonyms_phrase_query", "fuzzy_transpositions"
        );
    }
}