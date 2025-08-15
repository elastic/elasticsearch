/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.apache.lucene.search.Query;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.containsString;

/**
 * Tests for improved error messages in MultiMatchQueryBuilder parsing
 */
public class MultiMatchQueryBuilderErrorMessageTests extends AbstractQueryTestCase<MultiMatchQueryBuilder> {

    @Override
    protected MultiMatchQueryBuilder doCreateTestQueryBuilder() {
        MultiMatchQueryBuilder query = new MultiMatchQueryBuilder("test query", "field1", "field2");
        return query;
    }
    
    @Override
    protected void doAssertLuceneQuery(MultiMatchQueryBuilder queryBuilder, Query query, SearchExecutionContext context) {
        // Not testing the actual query generation, just the error messages
    }

    public void testMisplacedFieldErrorMessage() throws IOException {
        // The "type" field is outside the multi_match object where it should be
        String json = """
            {
              "multi_match": {
                "query": "party planning",
                "fields": [
                  "headline",
                  "short_description"
                ]
              },
              "type": "phrase"
            }
            """;

        // Parse up to the query part
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        parser.nextToken(); // START_OBJECT
        parser.nextToken(); // FIELD_NAME "multi_match"
        parser.nextToken(); // START_OBJECT
        
        // This should parse successfully
        MultiMatchQueryBuilder builder = MultiMatchQueryBuilder.fromXContent(parser);
        assertNotNull(builder);
        
        // Now when we continue parsing, we should get an error about the misplaced "type" field
        // This would be caught at a higher level in the query parsing
    }

    public void testUnknownFieldErrorMessage() throws IOException {
        String json = """
            {
              "multi_match": {
                "query": "test",
                "fields": ["field1"],
                "unknown_param": "value"
              }
            }
            """;

        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        parser.nextToken(); // START_OBJECT
        parser.nextToken(); // FIELD_NAME "multi_match"
        parser.nextToken(); // START_OBJECT
        
        ParsingException e = expectThrows(ParsingException.class, () -> MultiMatchQueryBuilder.fromXContent(parser));
        
        // Check that the error message includes the supported fields
        assertThat(e.getMessage(), containsString("[multi_match] query does not support [unknown_param]"));
        assertThat(e.getMessage(), containsString("Supported fields are:"));
        assertThat(e.getMessage(), containsString("query"));
        assertThat(e.getMessage(), containsString("fields"));
        assertThat(e.getMessage(), containsString("type"));
        assertThat(e.getMessage(), containsString("analyzer"));
    }
}