/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.parser;

import org.elasticsearch.core.Strings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.io.IOException;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;

public class KqlParserBooleanQueryTests extends AbstractKqlParserTestCase {

    public void testParseNotQuery() throws IOException {
        for (String baseQuery : readQueries(SUPPORTED_QUERY_FILE_PATH)) {
            if (BOOLEAN_QUERY_FILTER.test(baseQuery)) {
                baseQuery = wrapWithRandomWhitespaces("(") + baseQuery + wrapWithRandomWhitespaces(")");
            }

            String notQuery = wrapWithRandomWhitespaces("NOT ") + baseQuery;

            BoolQueryBuilder parsedQuery = asInstanceOf(BoolQueryBuilder.class, parseKqlQuery(notQuery));
            assertThat(parsedQuery.filter(), empty());
            assertThat(parsedQuery.should(), empty());
            assertThat(parsedQuery.must(), empty());
            assertThat(parsedQuery.mustNot(), hasSize(1));
            assertThat(parsedQuery.mustNot(), hasItem(equalTo((parseKqlQuery(baseQuery)))));

            assertThat(
                parseKqlQuery("NOT" + wrapWithRandomWhitespaces("(") + baseQuery + wrapWithRandomWhitespaces(")")),
                equalTo(parsedQuery)
            );
        }
    }

    public void testParseOrQuery() throws IOException {
        List<String> supportedQueries = readQueries(SUPPORTED_QUERY_FILE_PATH);

        for (int runs = 0; runs < 100; runs++) {
            String queryA = randomFrom(supportedQueries);
            String queryB = randomFrom(supportedQueries);
            String orQuery = queryA + wrapWithRandomWhitespaces(randomFrom(" or ", " OR ", " Or ", " oR ")) + queryB;

            BoolQueryBuilder parsedQuery = asInstanceOf(BoolQueryBuilder.class, parseKqlQuery(orQuery));

            if (Stream.of(queryA, queryB).noneMatch(BOOLEAN_QUERY_FILTER)) {
                // If one of the subquery is a boolean query, it is impossible to test the content of the generated query because
                // operator precedence rules are applied. There are specific tests for it in testOperatorPrecedence.
                assertThat(parsedQuery.filter(), empty());
                assertThat(parsedQuery.must(), empty());
                assertThat(parsedQuery.mustNot(), empty());
                assertThat(parsedQuery.should(), hasSize(2));
                assertThat(parsedQuery.minimumShouldMatch(), equalTo("1"));
                assertThat(
                    parsedQuery.should(),
                    allOf(hasItem(equalTo((parseKqlQuery(queryA)))), hasItem(equalTo((parseKqlQuery(queryB)))))
                );
            }
        }

        for (int runs = 0; runs < 100; runs++) {
            String queryA = randomFrom(supportedQueries);
            String queryB = randomFrom(supportedQueries);
            String queryC = randomFrom(supportedQueries);
            String orQuery = Strings.format("%s OR %s OR %s", queryA, queryB, queryC);

            BoolQueryBuilder parsedQuery = asInstanceOf(BoolQueryBuilder.class, parseKqlQuery(orQuery));

            if (Stream.of(queryA, queryB, queryC).noneMatch(BOOLEAN_QUERY_FILTER)) {
                // If one of the subquery is a boolean query, it is impossible to test the content of the generated query because
                // operator precedence rules are applied. There are specific tests for it in testOperatorPrecedence.
                assertThat(parsedQuery.should(), hasSize(3));
                assertThat(
                    parsedQuery.should(),
                    allOf(
                        hasItem(equalTo((parseKqlQuery(queryA)))),
                        hasItem(equalTo((parseKqlQuery(queryB)))),
                        hasItem(equalTo((parseKqlQuery(queryC))))
                    )
                );
            }
        }
    }

    public void testParseAndQuery() throws IOException {
        List<String> supportedQueries = readQueries(SUPPORTED_QUERY_FILE_PATH);

        for (int runs = 0; runs < 100; runs++) {
            String queryA = randomFrom(supportedQueries);
            String queryB = randomFrom(supportedQueries);
            String andQuery = queryA + wrapWithRandomWhitespaces(randomFrom(" and ", " AND ", " And ", " anD ")) + queryB;

            BoolQueryBuilder parsedQuery = asInstanceOf(BoolQueryBuilder.class, parseKqlQuery(andQuery));

            if (Stream.of(queryA, queryB).noneMatch(BOOLEAN_QUERY_FILTER)) {
                // If one of the subquery is a boolean query, it is impossible to test the content of the generated query because
                // operator precedence rules are applied. There are specific tests for it in testOperatorPrecedence.
                assertThat(parsedQuery.filter(), empty());
                assertThat(parsedQuery.should(), empty());
                assertThat(parsedQuery.mustNot(), empty());
                assertThat(parsedQuery.must(), hasSize(2));
                assertThat(parsedQuery.must(), allOf(hasItem(equalTo((parseKqlQuery(queryA)))), hasItem(equalTo((parseKqlQuery(queryB))))));
            }
        }

        for (int runs = 0; runs < 100; runs++) {
            String queryA = randomFrom(supportedQueries);
            String queryB = randomFrom(supportedQueries);
            String queryC = randomFrom(supportedQueries);
            String andQuery = Strings.format("%s AND %s AND %s", queryA, queryB, queryC);

            if (Stream.of(queryA, queryB, queryC).noneMatch(BOOLEAN_QUERY_FILTER)) {
                // If one of the subquery is a boolean query, it is impossible to test the content of the generated query because
                // operator precedence rules are applied. There are specific tests for it in testOperatorPrecedence.
                BoolQueryBuilder parsedQuery = asInstanceOf(BoolQueryBuilder.class, parseKqlQuery(andQuery));
                assertThat(parsedQuery.must(), hasSize(3));
                assertThat(
                    parsedQuery.must(),
                    allOf(
                        hasItem(equalTo((parseKqlQuery(queryA)))),
                        hasItem(equalTo((parseKqlQuery(queryB)))),
                        hasItem(equalTo((parseKqlQuery(queryC))))
                    )
                );
            }
        }
    }

    public void testOperatorPrecedence() throws IOException {
        List<String> supportedQueries = readQueries(SUPPORTED_QUERY_FILE_PATH, Predicate.not(BOOLEAN_QUERY_FILTER));

        for (int runs = 0; runs < 100; runs++) {
            String queryA = randomFrom(supportedQueries);
            String queryB = randomFrom(supportedQueries);
            String queryC = randomFrom(supportedQueries);

            // <QueryA> AND <QueryB> OR <QueryC> is equivalent to <QueryA> AND (<QueryB> OR <QueryC>)
            {
                QueryBuilder parsedQuery = parseKqlQuery(Strings.format("%s AND %s OR %s", queryA, queryB, queryC));
                assertThat(
                    parsedQuery,
                    equalTo(
                        QueryBuilders.boolQuery()
                            .must(parseKqlQuery(queryA))
                            .must(
                                QueryBuilders.boolQuery().minimumShouldMatch(1).should(parseKqlQuery(queryB)).should(parseKqlQuery(queryC))
                            )
                    )
                );
                assertThat(parsedQuery, equalTo(parseKqlQuery(Strings.format("%s AND (%s OR %s)", queryA, queryB, queryC))));
            }

            // <QueryA> OR <QueryB> AND <QueryC> is equivalent to <QueryA> OR (<QueryB> AND <QueryC>)
            {
                QueryBuilder parsedQuery = parseKqlQuery(Strings.format("%s OR %s AND %s", queryA, queryB, queryC));
                assertThat(
                    parsedQuery,
                    equalTo(
                        QueryBuilders.boolQuery()
                            .minimumShouldMatch(1)
                            .should(parseKqlQuery(queryA))
                            .should(QueryBuilders.boolQuery().must(parseKqlQuery(queryB)).must(parseKqlQuery(queryC)))
                    )
                );
                assertThat(parsedQuery, equalTo(parseKqlQuery(Strings.format("%s OR (%s AND %s)", queryA, queryB, queryC))));
            }

            // <QueryA> AND <QueryB> is equivalent to (NOT <QueryA>) AND <QueryB>
            {
                QueryBuilder parsedQuery = parseKqlQuery(Strings.format("NOT %s AND %s", queryA, queryB));
                assertThat(
                    parsedQuery,
                    equalTo(
                        QueryBuilders.boolQuery().must(QueryBuilders.boolQuery().mustNot(parseKqlQuery(queryA))).must(parseKqlQuery(queryB))
                    )
                );
                assertThat(parsedQuery, equalTo(parseKqlQuery(Strings.format("(NOT %s) AND %s", queryA, queryB))));
            }

            // <QueryA> OR <QueryB> is equivalent to (NOT <QueryA>) OR <QueryB>
            {
                QueryBuilder parsedQuery = parseKqlQuery(Strings.format("NOT %s OR %s", queryA, queryB));
                assertThat(
                    parsedQuery,
                    equalTo(
                        QueryBuilders.boolQuery()
                            .minimumShouldMatch(1)
                            .should(QueryBuilders.boolQuery().mustNot(parseKqlQuery(queryA)))
                            .should(parseKqlQuery(queryB))
                    )
                );
                assertThat(parsedQuery, equalTo(parseKqlQuery(Strings.format("(NOT %s) OR %s", queryA, queryB))));
            }
        }
    }
}
