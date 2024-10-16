/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.parser;

import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.test.AbstractBuilderTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isA;

public class KqlParserTests extends AbstractBuilderTestCase {

    public void testEmptyQueryParsing() throws IOException {
        KqlParser parser = new KqlParser();
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        assertThat(parser.parseKqlQuery("", searchExecutionContext), isA(MatchAllQueryBuilder.class));
    }

    public void testSyntaxErrorsHandling() throws IOException {

        KqlParser parser = new KqlParser();
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();

        {
            ParsingException e = assertThrows(ParsingException.class, () -> parser.parseKqlQuery("foo: \"bar", searchExecutionContext));
            assertThat(e.getLineNumber(), equalTo(1));
            assertThat(e.getColumnNumber(), equalTo(6));
            assertThat(e.getMessage(), equalTo("line 1:6: token recognition error at: '\"bar'"));
        }

        {
            ParsingException e = assertThrows(ParsingException.class, () -> parser.parseKqlQuery("foo: (bar baz AND qux", searchExecutionContext));
            assertThat(e.getLineNumber(), equalTo(1));
            assertThat(e.getColumnNumber(), equalTo(15));
            assertThat(e.getMessage(), equalTo("line 1:15: missing ')' at 'AND'"));
        }

    }
}
