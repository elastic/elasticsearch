/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.parser;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.Strings.format;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class KqlNestedFieldQueryTests extends AbstractKqlParserTestCase {
    public void testInvalidNestedFieldName() {
        for (String invalidFieldName : List.of(OBJECT_FIELD_NAME, TEXT_FIELD_NAME, "not_a_field", "mapped_nest*")) {
            KqlParsingException e = assertThrows(
                KqlParsingException.class,
                () -> parseKqlQuery(format("%s : { %s: foo AND %s < 10 } ", invalidFieldName, TEXT_FIELD_NAME, INT_FIELD_NAME))
            );
            assertThat(e.getMessage(), Matchers.containsString(invalidFieldName));
            assertThat(e.getMessage(), Matchers.containsString("is not a valid nested field name"));
        }
    }

    public void testInlineNestedFieldMatchTextQuery() {
        for (String fieldName : List.of(TEXT_FIELD_NAME, INT_FIELD_NAME)) {
            {
                // Querying a nested text subfield.
                String nestedFieldName = format("%s.%s", NESTED_FIELD_NAME, fieldName);
                String searchTerms = randomSearchTerms();
                String kqlQueryString = format("%s: %s", nestedFieldName, searchTerms);

                NestedQueryBuilder nestedQuery = asInstanceOf(NestedQueryBuilder.class, parseKqlQuery(kqlQueryString));

                assertThat(nestedQuery.path(), equalTo(NESTED_FIELD_NAME));
                assertMatchQueryBuilder(nestedQuery.query(), nestedFieldName, searchTerms);
            }

            {
                // Several levels of nested fields.
                String nestedFieldName = format("%s.%s.%s", NESTED_FIELD_NAME, NESTED_FIELD_NAME, fieldName);
                String searchTerms = randomSearchTerms();
                String kqlQueryString = format("%s: %s", nestedFieldName, searchTerms);

                NestedQueryBuilder nestedQuery = asInstanceOf(NestedQueryBuilder.class, parseKqlQuery(kqlQueryString));
                assertThat(nestedQuery.path(), equalTo(NESTED_FIELD_NAME));

                NestedQueryBuilder nestedSubQuery = asInstanceOf(NestedQueryBuilder.class, nestedQuery.query());
                assertThat(nestedSubQuery.path(), equalTo(format("%s.%s", NESTED_FIELD_NAME, NESTED_FIELD_NAME)));

                assertMatchQueryBuilder(nestedSubQuery.query(), nestedFieldName, searchTerms);
            }
        }
    }

    public void testInlineNestedFieldMatchKeywordFieldQuery() {
        {
            // Querying a nested text subfield.
            String nestedFieldName = format("%s.%s", NESTED_FIELD_NAME, KEYWORD_FIELD_NAME);
            String searchTerms = randomSearchTerms();
            String kqlQueryString = format("%s: %s", nestedFieldName, searchTerms);

            NestedQueryBuilder nestedQuery = asInstanceOf(NestedQueryBuilder.class, parseKqlQuery(kqlQueryString));

            assertThat(nestedQuery.path(), equalTo(NESTED_FIELD_NAME));
            assertTermQueryBuilder(nestedQuery.query(), nestedFieldName, searchTerms);
        }

        {
            // Several levels of nested fields.
            String nestedFieldName = format("%s.%s.%s", NESTED_FIELD_NAME, NESTED_FIELD_NAME, KEYWORD_FIELD_NAME);
            String searchTerms = randomSearchTerms();
            String kqlQueryString = format("%s: %s", nestedFieldName, searchTerms);

            NestedQueryBuilder nestedQuery = asInstanceOf(NestedQueryBuilder.class, parseKqlQuery(kqlQueryString));
            assertThat(nestedQuery.path(), equalTo(NESTED_FIELD_NAME));

            NestedQueryBuilder nestedSubQuery = asInstanceOf(NestedQueryBuilder.class, nestedQuery.query());
            assertThat(nestedSubQuery.path(), equalTo(format("%s.%s", NESTED_FIELD_NAME, NESTED_FIELD_NAME)));

            assertTermQueryBuilder(nestedSubQuery.query(), nestedFieldName, searchTerms);
        }
    }

    public void testInlineNestedFieldRangeQuery() {
        {
            // Querying a nested text subfield.
            String nestedFieldName = format("%s.%s", NESTED_FIELD_NAME, INT_FIELD_NAME);
            String operator = randomFrom(">", ">=", "<", "<=");
            String kqlQueryString = format("%s %s %s", nestedFieldName, operator, randomDouble());

            NestedQueryBuilder nestedQuery = asInstanceOf(NestedQueryBuilder.class, parseKqlQuery(kqlQueryString));

            assertThat(nestedQuery.path(), equalTo(NESTED_FIELD_NAME));
            assertRangeQueryBuilder(nestedQuery.query(), nestedFieldName, rangeQueryBuilder -> {});
        }

        {
            // Several levels of nested fields.
            String nestedFieldName = format("%s.%s.%s", NESTED_FIELD_NAME, NESTED_FIELD_NAME, INT_FIELD_NAME);
            String operator = randomFrom(">", ">=", "<", "<=");
            String kqlQueryString = format("%s %s %s", nestedFieldName, operator, randomDouble());

            NestedQueryBuilder nestedQuery = asInstanceOf(NestedQueryBuilder.class, parseKqlQuery(kqlQueryString));
            assertThat(nestedQuery.path(), equalTo(NESTED_FIELD_NAME));

            NestedQueryBuilder nestedSubQuery = asInstanceOf(NestedQueryBuilder.class, nestedQuery.query());
            assertThat(nestedSubQuery.path(), equalTo(format("%s.%s", NESTED_FIELD_NAME, NESTED_FIELD_NAME)));

            assertRangeQueryBuilder(nestedSubQuery.query(), nestedFieldName, rangeQueryBuilder -> {});
        }
    }

    public void testNestedQuerySyntax() {
        // Single word - Keyword & text field
        List.of(KEYWORD_FIELD_NAME, TEXT_FIELD_NAME)
            .forEach(
                fieldName -> assertThat(
                    parseKqlQuery(format("%s : { %s : %s }", NESTED_FIELD_NAME, fieldName, "foo")),
                    equalTo(parseKqlQuery(format("%s.%s : %s", NESTED_FIELD_NAME, fieldName, "foo")))
                )
            );

        // Multiple words - Keyword & text field
        List.of(KEYWORD_FIELD_NAME, TEXT_FIELD_NAME)
            .forEach(
                fieldName -> assertThat(
                    parseKqlQuery(format("%s : { %s : %s }", NESTED_FIELD_NAME, fieldName, "foo bar")),
                    equalTo(parseKqlQuery(format("%s.%s : %s", NESTED_FIELD_NAME, fieldName, "foo bar")))
                )
            );

        // Range syntax
        {
            String operator = randomFrom("<", "<=", ">", ">=");
            double rangeValue = randomDouble();
            assertThat(
                parseKqlQuery(format("%s : { %s %s %s }", NESTED_FIELD_NAME, INT_FIELD_NAME, operator, rangeValue)),
                equalTo(parseKqlQuery(format("%s.%s %s %s", NESTED_FIELD_NAME, INT_FIELD_NAME, operator, rangeValue)))
            );
        }

        // Several level of nesting
        {
            QueryBuilder inlineQuery = parseKqlQuery(
                format("%s.%s.%s : %s", NESTED_FIELD_NAME, NESTED_FIELD_NAME, TEXT_FIELD_NAME, "foo bar")
            );

            assertThat(
                parseKqlQuery(format("%s : { %s : { %s : %s } }", NESTED_FIELD_NAME, NESTED_FIELD_NAME, TEXT_FIELD_NAME, "foo bar")),
                equalTo(inlineQuery)
            );

            assertThat(
                parseKqlQuery(format("%s.%s :  { %s : %s }", NESTED_FIELD_NAME, NESTED_FIELD_NAME, TEXT_FIELD_NAME, "foo bar")),
                equalTo(inlineQuery)
            );

            assertThat(
                parseKqlQuery(format("%s : { %s.%s : %s }", NESTED_FIELD_NAME, NESTED_FIELD_NAME, TEXT_FIELD_NAME, "foo bar")),
                equalTo(inlineQuery)
            );
        }
    }

    public void testBooleanAndNestedQuerySyntax() {
        NestedQueryBuilder nestedQuery = asInstanceOf(
            NestedQueryBuilder.class,
            parseKqlQuery(
                format("%s: { %s : foo AND %s: bar AND %s > 3}", NESTED_FIELD_NAME, TEXT_FIELD_NAME, KEYWORD_FIELD_NAME, INT_FIELD_NAME)
            )
        );
        assertThat(nestedQuery.path(), equalTo(NESTED_FIELD_NAME));

        BoolQueryBuilder subQuery = asInstanceOf(BoolQueryBuilder.class, nestedQuery.query());
        assertThat(subQuery.should(), empty());
        assertThat(subQuery.filter(), empty());
        assertThat(subQuery.mustNot(), empty());
        assertThat(subQuery.must(), hasSize(3));
        assertMatchQueryBuilder(
            subQuery.must().stream().filter(q -> q instanceof MatchQueryBuilder).findFirst().get(),
            format("%s.%s", NESTED_FIELD_NAME, TEXT_FIELD_NAME),
            "foo"
        );
        assertTermQueryBuilder(
            subQuery.must().stream().filter(q -> q instanceof TermQueryBuilder).findFirst().get(),
            format("%s.%s", NESTED_FIELD_NAME, KEYWORD_FIELD_NAME),
            "bar"
        );
        assertRangeQueryBuilder(
            subQuery.must().stream().filter(q -> q instanceof RangeQueryBuilder).findAny().get(),
            format("%s.%s", NESTED_FIELD_NAME, INT_FIELD_NAME),
            q -> {}
        );
    }

    public void testBooleanOrNestedQuerySyntax() {
        NestedQueryBuilder nestedQuery = asInstanceOf(
            NestedQueryBuilder.class,
            parseKqlQuery(
                format("%s: { %s : foo OR %s: bar OR %s > 3 }", NESTED_FIELD_NAME, TEXT_FIELD_NAME, KEYWORD_FIELD_NAME, INT_FIELD_NAME)
            )
        );

        assertThat(nestedQuery.path(), equalTo(NESTED_FIELD_NAME));

        BoolQueryBuilder subQuery = asInstanceOf(BoolQueryBuilder.class, nestedQuery.query());
        assertThat(subQuery.must(), empty());
        assertThat(subQuery.filter(), empty());
        assertThat(subQuery.mustNot(), empty());
        assertThat(subQuery.should(), hasSize(3));
        assertMatchQueryBuilder(
            subQuery.should().stream().filter(q -> q instanceof MatchQueryBuilder).findFirst().get(),
            format("%s.%s", NESTED_FIELD_NAME, TEXT_FIELD_NAME),
            "foo"
        );
        assertTermQueryBuilder(
            subQuery.should().stream().filter(q -> q instanceof TermQueryBuilder).findFirst().get(),
            format("%s.%s", NESTED_FIELD_NAME, KEYWORD_FIELD_NAME),
            "bar"
        );
        assertRangeQueryBuilder(
            subQuery.should().stream().filter(q -> q instanceof RangeQueryBuilder).findAny().get(),
            format("%s.%s", NESTED_FIELD_NAME, INT_FIELD_NAME),
            q -> {}
        );
    }

    public void testBooleanNotNestedQuerySyntax() {
        {
            NestedQueryBuilder nestedQuery = asInstanceOf(
                NestedQueryBuilder.class,
                parseKqlQuery(format("%s: { NOT %s : foo }", NESTED_FIELD_NAME, TEXT_FIELD_NAME))
            );

            assertThat(nestedQuery.path(), equalTo(NESTED_FIELD_NAME));

            BoolQueryBuilder subQuery = asInstanceOf(BoolQueryBuilder.class, nestedQuery.query());
            assertThat(subQuery.must(), empty());
            assertThat(subQuery.filter(), empty());
            assertThat(subQuery.should(), empty());
            assertThat(subQuery.mustNot(), hasSize(1));
            assertMatchQueryBuilder(subQuery.mustNot().get(0), format("%s.%s", NESTED_FIELD_NAME, TEXT_FIELD_NAME), "foo");
        }

        {
            NestedQueryBuilder nestedQuery = asInstanceOf(
                NestedQueryBuilder.class,
                parseKqlQuery(format("%s: { NOT %s : foo }", NESTED_FIELD_NAME, KEYWORD_FIELD_NAME))
            );

            assertThat(nestedQuery.path(), equalTo(NESTED_FIELD_NAME));

            BoolQueryBuilder subQuery = asInstanceOf(BoolQueryBuilder.class, nestedQuery.query());
            assertThat(subQuery.must(), empty());
            assertThat(subQuery.filter(), empty());
            assertThat(subQuery.should(), empty());
            assertThat(subQuery.mustNot(), hasSize(1));
            assertTermQueryBuilder(subQuery.mustNot().get(0), format("%s.%s", NESTED_FIELD_NAME, KEYWORD_FIELD_NAME), "foo");
        }

        {
            NestedQueryBuilder nestedQuery = asInstanceOf(
                NestedQueryBuilder.class,
                parseKqlQuery(format("%s: { NOT %s < 3 }", NESTED_FIELD_NAME, INT_FIELD_NAME))
            );

            assertThat(nestedQuery.path(), equalTo(NESTED_FIELD_NAME));

            BoolQueryBuilder subQuery = asInstanceOf(BoolQueryBuilder.class, nestedQuery.query());
            assertThat(subQuery.must(), empty());
            assertThat(subQuery.filter(), empty());
            assertThat(subQuery.should(), empty());
            assertThat(subQuery.mustNot(), hasSize(1));
            assertRangeQueryBuilder(subQuery.mustNot().get(0), format("%s.%s", NESTED_FIELD_NAME, INT_FIELD_NAME), q -> {});
        }
    }

    private static String randomSearchTerms() {
        return Stream.generate(ESTestCase::randomIdentifier).limit(randomIntBetween(1, 10)).collect(Collectors.joining(" "));
    }
}
