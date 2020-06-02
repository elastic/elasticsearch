/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
