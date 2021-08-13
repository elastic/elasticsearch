/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent.cbor;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentGenerator;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class JsonVsCborTests extends ESTestCase {
    public void testCompareParsingTokens() throws IOException {
        BytesStreamOutput xsonOs = new BytesStreamOutput();
        XContentGenerator xsonGen = XContentFactory.xContent(XContentType.CBOR).createGenerator(xsonOs);

        BytesStreamOutput jsonOs = new BytesStreamOutput();
        XContentGenerator jsonGen = XContentFactory.xContent(XContentType.JSON).createGenerator(jsonOs);

        xsonGen.writeStartObject();
        jsonGen.writeStartObject();

        xsonGen.writeStringField("test", "value");
        jsonGen.writeStringField("test", "value");

        xsonGen.writeFieldName("arr");
        xsonGen.writeStartArray();
        jsonGen.writeFieldName("arr");
        jsonGen.writeStartArray();
        xsonGen.writeNumber(1);
        jsonGen.writeNumber(1);
        xsonGen.writeNull();
        jsonGen.writeNull();
        xsonGen.writeEndArray();
        jsonGen.writeEndArray();

        xsonGen.writeEndObject();
        jsonGen.writeEndObject();

        xsonGen.close();
        jsonGen.close();
        try (XContentParser json0sParser = createParser(JsonXContent.jsonXContent, jsonOs.bytes());
             XContentParser xson0sParser = createParser(CborXContent.cborXContent, xsonOs.bytes())) {
            verifySameTokens(json0sParser, xson0sParser);
        }
    }

    private void verifySameTokens(XContentParser parser1, XContentParser parser2) throws IOException {
        while (true) {
            XContentParser.Token token1 = parser1.nextToken();
            XContentParser.Token token2 = parser2.nextToken();
            if (token1 == null) {
                assertThat(token2, nullValue());
                return;
            }
            assertThat(token1, equalTo(token2));
            switch (token1) {
                case FIELD_NAME:
                    assertThat(parser1.currentName(), equalTo(parser2.currentName()));
                    break;
                case VALUE_STRING:
                    assertThat(parser1.text(), equalTo(parser2.text()));
                    break;
                case VALUE_NUMBER:
                    assertThat(parser1.numberType(), equalTo(parser2.numberType()));
                    assertThat(parser1.numberValue(), equalTo(parser2.numberValue()));
                    break;
            }
        }
    }
}
