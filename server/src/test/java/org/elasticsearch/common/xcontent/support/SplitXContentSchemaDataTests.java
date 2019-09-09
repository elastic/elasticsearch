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

package org.elasticsearch.common.xcontent.support;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentGenerator;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.StringReader;

public class SplitXContentSchemaDataTests extends ESTestCase {

    public void testEmptyObject() throws IOException {
        doRoundtrip("{}");
    }

    public void testString() throws IOException {
        doRoundtrip("{\"foo\":\"bar\"}");
    }

    public void testLong() throws IOException {
        doRoundtrip("{\"foo\":42}");
    }

    public void testDouble() throws IOException {
        doRoundtrip("{\"foo\":4.2}");
    }

    public void testNegativeLong() throws IOException {
        doRoundtrip("{\"foo\":-42}");
    }

    public void testNegativeDouble() throws IOException {
        doRoundtrip("{\"foo\":-4.2}");
    }

    public void testNull() throws IOException {
        doRoundtrip("{\"foo\":null}");
    }

    public void testInnerObject() throws IOException {
        doRoundtrip("{\"foo\":{\"bar\":\"baz\"}}");
    }

    public void testArray() throws IOException {
        doRoundtrip("{\"foo\":[\"bar\",42]}");
    }

    public void testInnerArray() throws IOException {
        doRoundtrip("{\"foo\":[[\"bar\"],[42]]}");
    }

    public void testBigInteger() throws IOException {
        doRoundtrip("{\"foo\":99999999999999999999999999999}");
    }

    public void testBigDecimal() throws IOException {
        doRoundtrip("{\"foo\":4.0000000000000000000000000002}");
    }

    private void doRoundtrip(String json) throws IOException {
        XContentParser parser = XContentType.JSON.xContent().createParser(
                NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, new StringReader(json));
        assertEquals(Token.START_OBJECT, parser.nextToken());
        BytesStreamOutput schema = new BytesStreamOutput();
        BytesStreamOutput data = new BytesStreamOutput();
        SplitXContentSchemaData.splitXContent(parser, schema, data);
        XContentParser mergedParser = SplitXContentSchemaData.mergeXContent(schema.bytes().streamInput(), data.bytes().streamInput());
        BytesStreamOutput merged = new BytesStreamOutput();
        assertEquals(Token.START_OBJECT, mergedParser.nextToken());
        try (XContentGenerator generator = XContentType.JSON.xContent().createGenerator(merged)) {
            generator.copyCurrentStructure(mergedParser);
        }
        String reconstructed = merged.bytes().utf8ToString();
        assertEquals(json, reconstructed);
    }
}
