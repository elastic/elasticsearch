/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.parser;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isA;

public class KqlParserExistsQueryTests extends AbstractKqlParserTestCase {

    public void testParseExistsQueryWithNoMatchingFields() {
        // Using an unquoted literal
        assertThat(parseKqlQuery(kqlExistsQuery("not_a_valid_field")), isA(MatchNoneQueryBuilder.class));

        // Using an a quoted string
        assertThat(parseKqlQuery(kqlExistsQuery("\"not_a_valid_field\"")), isA(MatchNoneQueryBuilder.class));

        // Not expanding wildcard with quoted string
        assertThat(parseKqlQuery(kqlExistsQuery("\"mapped_*\"")), isA(MatchNoneQueryBuilder.class));

        // Object fields are not supported by the exists query. Returning a MatchNoneQueryBuilder in this case.
        assertThat(parseKqlQuery(kqlExistsQuery(OBJECT_FIELD_NAME)), isA(MatchNoneQueryBuilder.class));
    }

    public void testParseExistsQueryWithASingleField() {
        for (String fieldName : searchableFields()) {
            ExistsQueryBuilder parsedQuery = asInstanceOf(ExistsQueryBuilder.class, parseKqlQuery(kqlExistsQuery(fieldName)));
            assertThat(parsedQuery.fieldName(), equalTo(fieldName));

            // Using quotes to wrap the field name does not change the result.
            assertThat(parseKqlQuery(kqlExistsQuery("\"" + fieldName + "\"")), equalTo(parsedQuery));
        }
    }

    public void testParseExistsQueryUsingWildcardFieldName() {
        String fieldNamePattern = "mapped_*";
        BoolQueryBuilder parsedQuery = asInstanceOf(BoolQueryBuilder.class, parseKqlQuery(kqlExistsQuery(fieldNamePattern)));
        assertThat(parsedQuery.minimumShouldMatch(), equalTo("1"));
        assertThat(parsedQuery.must(), empty());
        assertThat(parsedQuery.mustNot(), empty());
        assertThat(parsedQuery.filter(), empty());

        assertThat(
            parsedQuery.should(),
            containsInAnyOrder(searchableFields(fieldNamePattern).stream().map(QueryBuilders::existsQuery).toArray())
        );
    }

    private static String kqlExistsQuery(String field) {
        return wrapWithRandomWhitespaces(field) + wrapWithRandomWhitespaces(":") + wrapWithRandomWhitespaces("*");
    }
}
