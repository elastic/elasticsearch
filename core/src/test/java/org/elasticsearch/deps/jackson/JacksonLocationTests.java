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

package org.elasticsearch.deps.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class JacksonLocationTests extends ElasticsearchTestCase {

    @Test
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

        byte[] data = os.bytes().toBytes();
        JsonParser parser = new JsonFactory().createParser(data);

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
