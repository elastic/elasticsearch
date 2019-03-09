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

package org.elasticsearch.common.xcontent.smile;

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

        try (XContentParser jsonParser = createParser(JsonXContent.jsonXContent, jsonOs.bytes());
            XContentParser smileParser = createParser(SmileXContent.smileXContent, xsonOs.bytes())) {
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
