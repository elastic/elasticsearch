/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.parser;

import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.Strings.format;
import static org.hamcrest.Matchers.equalTo;

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
                String searchTerms = Stream.generate(ESTestCase::randomIdentifier)
                    .limit(randomIntBetween(1, 10))
                    .collect(Collectors.joining(" "));
                String kqlQueryString = format("%s: %s", nestedFieldName, searchTerms);

                NestedQueryBuilder nestedQuery = asInstanceOf(NestedQueryBuilder.class, parseKqlQuery(kqlQueryString));

                assertThat(nestedQuery.path(), equalTo(NESTED_FIELD_NAME));
                assertMatchQueryBuilder(nestedQuery.query(), nestedFieldName, searchTerms);
            }

            {
                // Several levels of nested fields.
                String nestedFieldName = format("%s.%s.%s", NESTED_FIELD_NAME, NESTED_FIELD_NAME, fieldName);
                String searchTerms = Stream.generate(ESTestCase::randomIdentifier)
                    .limit(randomIntBetween(1, 10))
                    .collect(Collectors.joining(" "));
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
            String searchTerms = Stream.generate(ESTestCase::randomIdentifier)
                .limit(randomIntBetween(1, 10))
                .collect(Collectors.joining(" "));
            String kqlQueryString = format("%s: %s", nestedFieldName, searchTerms);

            NestedQueryBuilder nestedQuery = asInstanceOf(NestedQueryBuilder.class, parseKqlQuery(kqlQueryString));

            assertThat(nestedQuery.path(), equalTo(NESTED_FIELD_NAME));
            assertTermQueryBuilder(nestedQuery.query(), nestedFieldName, searchTerms);
        }

        {
            // Several levels of nested fields.
            String nestedFieldName = format("%s.%s.%s", NESTED_FIELD_NAME, NESTED_FIELD_NAME, KEYWORD_FIELD_NAME);
            String searchTerms = Stream.generate(ESTestCase::randomIdentifier)
                .limit(randomIntBetween(1, 10))
                .collect(Collectors.joining(" "));
            String kqlQueryString = format("%s: %s", nestedFieldName, searchTerms);

            NestedQueryBuilder nestedQuery = asInstanceOf(NestedQueryBuilder.class, parseKqlQuery(kqlQueryString));
            assertThat(nestedQuery.path(), equalTo(NESTED_FIELD_NAME));

            NestedQueryBuilder nestedSubQuery = asInstanceOf(NestedQueryBuilder.class, nestedQuery.query());
            assertThat(nestedSubQuery.path(), equalTo(format("%s.%s", NESTED_FIELD_NAME, NESTED_FIELD_NAME)));

            assertTermQueryBuilder(nestedSubQuery.query(), nestedFieldName, searchTerms);
        }
    }
}
