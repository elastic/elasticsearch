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
import static org.hamcrest.CoreMatchers.equalTo;

/**
 * Tests for improved error messages in RangeQueryBuilder parsing
 */
public class RangeQueryBuilderErrorMessageTests extends AbstractQueryTestCase<RangeQueryBuilder> {

    @Override
    protected RangeQueryBuilder doCreateTestQueryBuilder() {
        RangeQueryBuilder query = new RangeQueryBuilder(randomAlphaOfLengthBetween(3, 10));
        query.from(randomIntBetween(1, 100));
        query.to(randomIntBetween(101, 200));
        return query;
    }
    
    @Override
    protected void doAssertLuceneQuery(RangeQueryBuilder queryBuilder, Query query, SearchExecutionContext context) {
        // Not testing the actual query generation, just the error messages
    }

    public void testInvalidFieldErrorMessage() throws IOException {
        String json = """
            {
              "range": {
                "date": {
                  "gte": "2015-06-20",
                  "lte": "2015-09-22",
                  "unknown_field": "value"
                }
              }
            }
            """;

        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        parser.nextToken();
        
        ParsingException e = expectThrows(ParsingException.class, () -> RangeQueryBuilder.fromXContent(parser));
        
        // Check that the error message includes the supported fields
        assertThat(e.getMessage(), containsString("[range] query does not support [unknown_field]"));
        assertThat(e.getMessage(), containsString("Supported fields are:"));
        assertThat(e.getMessage(), containsString("gte"));
        assertThat(e.getMessage(), containsString("lte"));
        assertThat(e.getMessage(), containsString("boost"));
    }

    public void testMalformedJsonErrorMessage() throws IOException {
        // This simulates the case where a user forgets to wrap the field parameters in an object
        String json = """
            {
              "range": {
                "date": "2015-06-20"
              }
            }
            """;

        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        parser.nextToken();
        
        ParsingException e = expectThrows(ParsingException.class, () -> RangeQueryBuilder.fromXContent(parser));
        
        // Check that the error message includes a helpful suggestion
        assertThat(e.getMessage(), containsString("[range] query does not support [date]"));
        assertThat(e.getMessage(), containsString("Did you mean to wrap this value in an object?"));
        assertThat(e.getMessage(), containsString("you need to specify an object with properties like 'gte', 'lte'"));
    }
}