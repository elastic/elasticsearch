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

package org.elasticsearch.common.xcontent;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class XContentParserUtilsTests extends ESTestCase {

    private XContentType xContentType;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        xContentType = randomFrom(XContentType.values());
    }

    public void testEnsureFieldName() throws IOException {
        ParsingException e = expectThrows(ParsingException.class, () -> {
            XContentParser parser = createParser(createBuilder().startObject().endObject().bytes());
            // Parser current token is null
            assertNull(parser.currentToken());
            XContentParserUtils.ensureFieldName(parser.currentToken(), parser::getTokenLocation);
        });
        assertThat(e.getMessage(), equalTo("Failed to parse object: expecting token of type [FIELD_NAME] but found [null]"));

        e = expectThrows(ParsingException.class, () -> {
            XContentParser parser = createParser(createBuilder().startObject().field("foo", "bar").endObject().bytes());
            // Parser next token is a start object
            XContentParserUtils.ensureFieldName(parser.nextToken(), parser::getTokenLocation);
        });
        assertThat(e.getMessage(), equalTo("Failed to parse object: expecting token of type [FIELD_NAME] but found [START_OBJECT]"));

        e = expectThrows(ParsingException.class, () -> {
            XContentParser parser = createParser(createBuilder().startObject().field("foo", "bar").endObject().bytes());
            // Moves to start object
            assertThat(parser.nextToken(), is(XContentParser.Token.START_OBJECT));
            // Expected field name is "foo", not "test"
            XContentParserUtils.ensureFieldName(parser, parser.nextToken(), "test");
        });
        assertThat(e.getMessage(), equalTo("Failed to parse object: expecting field with name [test] but found [foo]"));

        // Everything is fine
        final String randomFieldName = randomAsciiOfLength(5);
        XContentParser parser = createParser(createBuilder().startObject().field(randomFieldName, 0).endObject().bytes());
        assertThat(parser.nextToken(), is(XContentParser.Token.START_OBJECT));
        XContentParserUtils.ensureFieldName(parser, parser.nextToken(), randomFieldName);
    }

    private XContentBuilder createBuilder() throws IOException {
        return XContentBuilder.builder(xContentType.xContent());
    }

    private XContentParser createParser(BytesReference bytes) throws IOException {
        return xContentType.xContent().createParser(bytes);
    }
}
