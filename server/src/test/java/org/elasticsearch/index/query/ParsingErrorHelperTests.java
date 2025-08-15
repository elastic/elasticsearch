/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class ParsingErrorHelperTests extends ESTestCase {

    public void testUnsupportedFieldMessage() {
        // Test with supported fields provided
        String message = ParsingErrorHelper.unsupportedFieldMessage(
            "range",
            "unknown_field",
            Set.of("gte", "lte", "boost")
        );
        
        assertEquals(
            "[range] query does not support [unknown_field]. Supported fields are: [boost, gte, lte]",
            message
        );
        
        // Test without supported fields
        String messageNoFields = ParsingErrorHelper.unsupportedFieldMessage(
            "range",
            "unknown_field",
            null
        );
        
        assertEquals(
            "[range] query does not support [unknown_field]",
            messageNoFields
        );
    }

    public void testMalformedQueryMessage() {
        String message = ParsingErrorHelper.malformedQueryMessage(
            "multi_match",
            "END_OBJECT",
            "FIELD_NAME",
            "type",
            "The 'type' field should be inside the multi_match query object"
        );
        
        assertEquals(
            "[multi_match] malformed query, expected [END_OBJECT] but found [FIELD_NAME] at field [type]. " +
            "The 'type' field should be inside the multi_match query object",
            message
        );
        
        // Test without suggestion
        String messageNoSuggestion = ParsingErrorHelper.malformedQueryMessage(
            "multi_match",
            "END_OBJECT",
            "FIELD_NAME",
            "type",
            null
        );
        
        assertEquals(
            "[multi_match] malformed query, expected [END_OBJECT] but found [FIELD_NAME] at field [type]",
            messageNoSuggestion
        );
    }

    public void testUnsupportedFieldTypeForAggregation() {
        String message = ParsingErrorHelper.unsupportedFieldTypeForAggregation(
            "my_field",
            "keyword",
            "date_histogram",
            List.of("date", "date_nanos", "numeric")
        );
        
        assertEquals(
            "Field [my_field] of type [keyword] is not supported for aggregation [date_histogram]. " +
            "Supported field types are: [date, date_nanos, numeric]",
            message
        );
        
        // Test without supported types
        String messageNoTypes = ParsingErrorHelper.unsupportedFieldTypeForAggregation(
            "my_field",
            "keyword",
            "date_histogram",
            null
        );
        
        assertEquals(
            "Field [my_field] of type [keyword] is not supported for aggregation [date_histogram]",
            messageNoTypes
        );
    }

    public void testGetJsonStructureSuggestion() {
        // Test for string value
        String stringSuggestion = ParsingErrorHelper.getJsonStructureSuggestion(
            XContentParser.Token.VALUE_STRING,
            "range query field"
        );
        
        assertEquals(
            "Did you mean to wrap this value in an object? For range query field, " +
            "you need to specify an object with properties like 'gte', 'lte', etc.",
            stringSuggestion
        );
        
        // Test for number value
        String numberSuggestion = ParsingErrorHelper.getJsonStructureSuggestion(
            XContentParser.Token.VALUE_NUMBER,
            "range query field"
        );
        
        assertEquals(
            "Did you mean to wrap this value in an object? For range query field, " +
            "you need to specify an object with properties like 'gte', 'lte', etc.",
            numberSuggestion
        );
        
        // Test for field name
        String fieldNameSuggestion = ParsingErrorHelper.getJsonStructureSuggestion(
            XContentParser.Token.FIELD_NAME,
            "multi_match query"
        );
        
        assertEquals(
            "Unexpected field found. Check that this field is placed within the correct query object structure.",
            fieldNameSuggestion
        );
        
        // Test for other tokens
        assertNull(ParsingErrorHelper.getJsonStructureSuggestion(
            XContentParser.Token.START_OBJECT,
            "some context"
        ));
    }

    public void testCommonFieldsConstants() {
        // Test that common fields are defined
        assertTrue(ParsingErrorHelper.CommonFields.RANGE_QUERY_FIELDS.contains("gte"));
        assertTrue(ParsingErrorHelper.CommonFields.RANGE_QUERY_FIELDS.contains("lte"));
        assertTrue(ParsingErrorHelper.CommonFields.RANGE_QUERY_FIELDS.contains("boost"));
        
        assertTrue(ParsingErrorHelper.CommonFields.MULTI_MATCH_QUERY_FIELDS.contains("query"));
        assertTrue(ParsingErrorHelper.CommonFields.MULTI_MATCH_QUERY_FIELDS.contains("fields"));
        assertTrue(ParsingErrorHelper.CommonFields.MULTI_MATCH_QUERY_FIELDS.contains("type"));
    }
}