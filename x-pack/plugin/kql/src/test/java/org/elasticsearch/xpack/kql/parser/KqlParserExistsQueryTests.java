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
import org.elasticsearch.index.query.SearchExecutionContext;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isA;

public class KqlParserExistsQueryTests extends AbstractKqlParserTestCase {

    public void testExistsQueryWithNonExistingField() {
        KqlParser parser = new KqlParser();
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();

        // Using an unquoted literal
        assertThat(parser.parseKqlQuery(kqlExistsQuery("foo"), searchExecutionContext), isA(MatchNoneQueryBuilder.class));

        // Using an a quoted string
        assertThat(parser.parseKqlQuery(kqlExistsQuery("\"foo\""), searchExecutionContext), isA(MatchNoneQueryBuilder.class));

        // Not expanding wildcard with quoted string
        assertThat(parser.parseKqlQuery(kqlExistsQuery("\"mapped_*\""), searchExecutionContext), isA(MatchNoneQueryBuilder.class));

        // Object fields are not supported by the exists query. Returning a MatchNoneQueryBuilder in this case.
        assertThat(parser.parseKqlQuery(kqlExistsQuery(OBJECT_FIELD_NAME), searchExecutionContext), isA(MatchNoneQueryBuilder.class));
    }

    public void testExistsQueryWithASingleField() {
        KqlParser parser = new KqlParser();
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();

        for (String fieldName : mappedLeafFields()) {
            ExistsQueryBuilder parsedQuery = asInstanceOf(
                ExistsQueryBuilder.class,
                parser.parseKqlQuery(kqlExistsQuery(fieldName), searchExecutionContext)
            );
            assertThat(parsedQuery.fieldName(), equalTo(fieldName));

            // Using quotes to wrap the field name does not change the result.
            assertThat(parser.parseKqlQuery(kqlExistsQuery("\"" + fieldName + "\""), searchExecutionContext), equalTo(parsedQuery));
        }
    }

    public void testExistsQueryUsingAWildcard() {
        KqlParser parser = new KqlParser();
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();

        BoolQueryBuilder parsedQuery = asInstanceOf(
            BoolQueryBuilder.class,
            parser.parseKqlQuery(kqlExistsQuery("mapped_*"), searchExecutionContext)
        );
        assertThat(parsedQuery.minimumShouldMatch(), equalTo("1"));
        assertThat(parsedQuery.must(), empty());
        assertThat(parsedQuery.mustNot(), empty());
        assertThat(parsedQuery.filter(), empty());

        assertThat(parsedQuery.should(), containsInAnyOrder(mappedLeafFields().stream().map(QueryBuilders::existsQuery).toArray()));
    }

    private String kqlExistsQuery(String field) {
        return wrapWithRandomWhitespaces(field) + wrapWithRandomWhitespaces(":") + wrapWithRandomWhitespaces("*");
    }
}
