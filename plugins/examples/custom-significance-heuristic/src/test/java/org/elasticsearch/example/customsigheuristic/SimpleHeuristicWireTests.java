/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.example.customsigheuristic;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class SimpleHeuristicWireTests extends AbstractSerializingTestCase<SimpleHeuristic> {
    @Override
    protected SimpleHeuristic doParseInstance(XContentParser parser) throws IOException {
        /* Because Heuristics are XContent "fragments" we need to throw away
         * the "extra" stuff before calling the parser. */
        parser.nextToken();
        assertThat(parser.currentToken(), equalTo(Token.START_OBJECT));
        parser.nextToken();
        assertThat(parser.currentToken(), equalTo(Token.FIELD_NAME));
        assertThat(parser.currentName(), equalTo("simple"));
        parser.nextToken();
        SimpleHeuristic h = SimpleHeuristic.PARSER.apply(parser, null);
        assertThat(parser.currentToken(), equalTo(Token.END_OBJECT));
        parser.nextToken();
        return h;
    }

    @Override
    protected Reader<SimpleHeuristic> instanceReader() {
        return SimpleHeuristic::new;
    }

    @Override
    protected SimpleHeuristic createTestInstance() {
        return new SimpleHeuristic();
    }
}
