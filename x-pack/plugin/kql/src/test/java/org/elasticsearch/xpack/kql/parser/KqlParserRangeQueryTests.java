/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.parser;

import org.elasticsearch.core.Strings;

import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class KqlParserRangeQueryTests extends AbstractKqlParserTestCase {

    public void testParseLtQuery() {
        for (String fieldName : List.of(DATE_FIELD_NAME, TEXT_FIELD_NAME, INT_FIELD_NAME, DOUBLE_FIELD_NAME, KEYWORD_FIELD_NAME)) {
            for (String query : queryValueSamples()) {
                assertRangeQueryBuilder(parseKqlQuery(Strings.format("%s < %s", fieldName, query)), fieldName, (rangeQuery) -> {
                    assertThat(rangeQuery.from(), nullValue());
                    assertThat(rangeQuery.to(), equalTo(query));
                    assertThat(rangeQuery.includeUpper(), equalTo(false));

                    // For range query adding quote does not change the generated of the query.
                    assertThat(parseKqlQuery(Strings.format("%s < %s", fieldName, quoteString(query))), equalTo(rangeQuery));
                });
            }
        }
    }

    public void testParseLteQuery() {
        for (String fieldName : List.of(DATE_FIELD_NAME, TEXT_FIELD_NAME, INT_FIELD_NAME, DOUBLE_FIELD_NAME, KEYWORD_FIELD_NAME)) {
            for (String query : queryValueSamples()) {
                assertRangeQueryBuilder(parseKqlQuery(Strings.format("%s <= %s", fieldName, query)), fieldName, (rangeQuery) -> {
                    assertThat(rangeQuery.from(), nullValue());
                    assertThat(rangeQuery.to(), equalTo(query));
                    assertThat(rangeQuery.includeUpper(), equalTo(true));
                    // For range query adding quote does not change the generated of the query.
                    assertThat(parseKqlQuery(Strings.format("%s <= %s", fieldName, quoteString(query))), equalTo(rangeQuery));
                });
            }
        }
    }

    public void testParseGtQuery() {
        for (String fieldName : List.of(DATE_FIELD_NAME, TEXT_FIELD_NAME, INT_FIELD_NAME, DOUBLE_FIELD_NAME, KEYWORD_FIELD_NAME)) {
            for (String query : queryValueSamples()) {
                assertRangeQueryBuilder(parseKqlQuery(Strings.format("%s > %s", fieldName, query)), fieldName, (rangeQuery) -> {
                    assertThat(rangeQuery.to(), nullValue());
                    assertThat(rangeQuery.from(), equalTo(query));
                    assertThat(rangeQuery.includeLower(), equalTo(false));
                    // For range query adding quote does not change the generated of the query.
                    assertThat(parseKqlQuery(Strings.format("%s > %s", fieldName, quoteString(query))), equalTo(rangeQuery));
                });
            }
        }
    }

    public void testParseGteQuery() {
        for (String fieldName : List.of(DATE_FIELD_NAME, TEXT_FIELD_NAME, INT_FIELD_NAME, DOUBLE_FIELD_NAME, KEYWORD_FIELD_NAME)) {
            for (String query : queryValueSamples()) {
                assertRangeQueryBuilder(parseKqlQuery(Strings.format("%s >= %s", fieldName, query)), fieldName, (rangeQuery) -> {
                    assertThat(rangeQuery.to(), nullValue());
                    assertThat(rangeQuery.from(), equalTo(query));
                    assertThat(rangeQuery.includeLower(), equalTo(true));
                    // For range query adding quote does not change the generated of the query.
                    assertThat(parseKqlQuery(Strings.format("%s >= %s", fieldName, quoteString(query))), equalTo(rangeQuery));
                });
            }
        }
    }

    private static List<String> queryValueSamples() {
        return Stream.of(randomIdentifier(), randomTimeValue(), randomInt(), randomDouble()).map(Object::toString).toList();
    }

    private static String quoteString(String input) {
        return Strings.format("\"%s\"", input);
    }
}
