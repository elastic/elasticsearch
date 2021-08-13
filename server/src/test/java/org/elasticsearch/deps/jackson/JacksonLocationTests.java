/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.deps.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class JacksonLocationTests extends ESTestCase {
    public void testLocationExtraction() throws IOException {
        // {
        //    "index" : "test",
        //    "source" : {
        //         value : "something"
        //    }
        // }
        BytesStreamOutput os = new BytesStreamOutput();
        JsonGenerator gen = new JsonFactory().createGenerator(os);
        gen.writeStartObject();

        gen.writeStringField("index", "test");

        gen.writeFieldName("source");
        gen.writeStartObject();
        gen.writeStringField("value", "something");
        gen.writeEndObject();

        gen.writeEndObject();

        gen.close();

        JsonParser parser = new JsonFactory().createParser(os.bytes().streamInput());

        assertThat(parser.nextToken(), equalTo(JsonToken.START_OBJECT));
        assertThat(parser.nextToken(), equalTo(JsonToken.FIELD_NAME)); // "index"
        assertThat(parser.nextToken(), equalTo(JsonToken.VALUE_STRING));
        assertThat(parser.nextToken(), equalTo(JsonToken.FIELD_NAME)); // "source"
//        JsonLocation location1 = parser.getCurrentLocation();
//        parser.skipChildren();
//        JsonLocation location2 = parser.getCurrentLocation();
//
//        byte[] sourceData = new byte[(int) (location2.getByteOffset() - location1.getByteOffset())];
//        System.arraycopy(data, (int) location1.getByteOffset(), sourceData, 0, sourceData.length);
//
//        JsonParser sourceParser = new JsonFactory().createJsonParser(new FastByteArrayInputStream(sourceData));
//        assertThat(sourceParser.nextToken(), equalTo(JsonToken.START_OBJECT));
//        assertThat(sourceParser.nextToken(), equalTo(JsonToken.FIELD_NAME)); // "value"
//        assertThat(sourceParser.nextToken(), equalTo(JsonToken.VALUE_STRING));
//        assertThat(sourceParser.nextToken(), equalTo(JsonToken.END_OBJECT));
    }
}
