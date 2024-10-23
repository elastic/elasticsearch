/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.parser;

import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryStringQueryBuilder;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;

public class KqlParserFieldlessQueryTests extends AbstractKqlParserTestCase {

    public void testTermQueryWithNoField() {
        {
            // Single word
            MultiMatchQueryBuilder parsedQuery = asInstanceOf(MultiMatchQueryBuilder.class, parseKqlQuery("foo"));
            assertThat(parsedQuery.fields(), anEmptyMap());
            assertThat(parsedQuery.type(), equalTo(MultiMatchQueryBuilder.Type.BEST_FIELDS));
            assertThat(parsedQuery.lenient(), equalTo(true));
            assertThat(parsedQuery.value(), equalTo("foo"));
        }

        {
            // Multiple words
            MultiMatchQueryBuilder parsedQuery = asInstanceOf(MultiMatchQueryBuilder.class, parseKqlQuery("foo bar baz"));
            assertThat(parsedQuery.fields(), anEmptyMap());
            assertThat(parsedQuery.type(), equalTo(MultiMatchQueryBuilder.Type.BEST_FIELDS));
            assertThat(parsedQuery.lenient(), equalTo(true));
            assertThat(parsedQuery.value(), equalTo("foo bar baz"));
        }

        {
            // With an escaped wildcard
            MultiMatchQueryBuilder parsedQuery = asInstanceOf(MultiMatchQueryBuilder.class, parseKqlQuery("foo \\* baz"));
            assertThat(parsedQuery.fields(), anEmptyMap());
            assertThat(parsedQuery.type(), equalTo(MultiMatchQueryBuilder.Type.BEST_FIELDS));
            assertThat(parsedQuery.lenient(), equalTo(true));
            assertThat(parsedQuery.value(), equalTo("foo * baz"));
        }

        // With a wildcard
        {
            QueryStringQueryBuilder parsedQuery = asInstanceOf(QueryStringQueryBuilder.class, parseKqlQuery("foo * baz"));
            assertThat(parsedQuery.queryString(), equalTo("foo * baz"));
        }
    }
}
