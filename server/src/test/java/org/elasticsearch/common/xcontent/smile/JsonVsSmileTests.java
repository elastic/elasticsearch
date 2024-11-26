/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.xcontent.smile;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentGenerator;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xcontent.smile.SmileXContent;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class JsonVsSmileTests extends ESTestCase {
    public void testCompareParsingTokens() throws IOException {
        BytesStreamOutput xsonOs = new BytesStreamOutput();
        XContentGenerator xsonGen = XContentFactory.xContent(XContentType.SMILE).createGenerator(xsonOs);

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

        try (
            XContentParser jsonParser = createParser(JsonXContent.jsonXContent, jsonOs.bytes());
            XContentParser smileParser = createParser(SmileXContent.smileXContent, xsonOs.bytes())
        ) {
            verifySameTokens(jsonParser, smileParser);
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
                case FIELD_NAME -> assertThat(parser1.currentName(), equalTo(parser2.currentName()));
                case VALUE_STRING -> assertThat(parser1.text(), equalTo(parser2.text()));
                case VALUE_NUMBER -> {
                    assertThat(parser1.numberType(), equalTo(parser2.numberType()));
                    assertThat(parser1.numberValue(), equalTo(parser2.numberValue()));
                }
            }
        }
    }
}
