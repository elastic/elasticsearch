/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.parser;

import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;

public class KqlParserTermQueryTests extends AbstractKqlParserTestCase {

    public void testTermQueryWithNoField() {
        KqlParser parser = new KqlParser();
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();

        {
            // Single word
            MultiMatchQueryBuilder parsedQuery = asInstanceOf(
                MultiMatchQueryBuilder.class,
                parser.parseKqlQuery(("foo"), searchExecutionContext)
            );
            assertThat(parsedQuery.fields(), anEmptyMap());
            assertThat(parsedQuery.type(), equalTo(MultiMatchQueryBuilder.Type.BEST_FIELDS));
            assertThat(parsedQuery.lenient(), equalTo(true));
            assertThat(parsedQuery.value(), equalTo("foo"));
        }

        {
            // Multiple words
            MultiMatchQueryBuilder parsedQuery = asInstanceOf(
                MultiMatchQueryBuilder.class,
                parser.parseKqlQuery(("foo bar baz"), searchExecutionContext)
            );
            assertThat(parsedQuery.fields(), anEmptyMap());
            assertThat(parsedQuery.type(), equalTo(MultiMatchQueryBuilder.Type.BEST_FIELDS));
            assertThat(parsedQuery.lenient(), equalTo(true));
            assertThat(parsedQuery.value(), equalTo("foo bar baz"));
        }

        {
            // With an escaped wildcard
            MultiMatchQueryBuilder parsedQuery = asInstanceOf(
                MultiMatchQueryBuilder.class,
                parser.parseKqlQuery(("foo \\* baz"), searchExecutionContext)
            );
            assertThat(parsedQuery.fields(), anEmptyMap());
            assertThat(parsedQuery.type(), equalTo(MultiMatchQueryBuilder.Type.BEST_FIELDS));
            assertThat(parsedQuery.lenient(), equalTo(true));
            assertThat(parsedQuery.value(), equalTo("foo * baz"));
        }

        // With a wildcard
        {
            QueryStringQueryBuilder parsedQuery = asInstanceOf(
                QueryStringQueryBuilder.class,
                parser.parseKqlQuery(("foo * baz"), searchExecutionContext)
            );
            assertThat(parsedQuery.queryString(), equalTo("foo * baz"));
        }
    }
}
