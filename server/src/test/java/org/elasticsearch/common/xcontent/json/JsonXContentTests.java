/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.xcontent.json;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.BaseXContentTestCase;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentGenerator;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.ByteArrayOutputStream;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class JsonXContentTests extends BaseXContentTestCase {

    @Override
    public XContentType xcontentType() {
        return XContentType.JSON;
    }

    public void testBigInteger() throws Exception {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        XContentGenerator generator = JsonXContent.jsonXContent.createGenerator(os);
        doTestBigInteger(generator, os);
    }

    public void testMalformedJsonFieldThrowsXContentException() throws Exception {
        String json = "{\"test\":\"/*/}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json)) {
            parser.nextToken();
            parser.nextToken();
            parser.nextToken();
            assertThrows(XContentParseException.class, () -> parser.text());
        }
    }

    public void testOptimizedTextHasBytes() throws Exception {
        XContentBuilder builder = builder().startObject().field("text", new Text("foo")).endObject();
        XContentParserConfiguration parserConfig = parserConfig();
        if (randomBoolean()) {
            parserConfig = parserConfig.withFiltering(null, Set.of("*"), null, true);
        }
        try (XContentParser parser = createParser(parserConfig, xcontentType().xContent(), BytesReference.bytes(builder))) {
            assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertSame(XContentParser.Token.FIELD_NAME, parser.nextToken());
            assertTrue(parser.nextToken().isValue());
            Text text = (Text) parser.optimizedText();
            assertTrue(text.hasBytes());
            assertThat(text.string(), equalTo("foo"));
        }
    }
}
