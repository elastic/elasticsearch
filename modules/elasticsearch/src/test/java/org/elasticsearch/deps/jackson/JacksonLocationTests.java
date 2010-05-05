/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.codehaus.jackson.*;
import org.elasticsearch.util.io.FastByteArrayInputStream;
import org.elasticsearch.util.io.FastByteArrayOutputStream;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
public class JacksonLocationTests {

    @Test public void testLocationExtraction() throws IOException {
        // {
        //    "index" : "test",
        //    "source" : {
        //         value : "something"
        //    }
        // }
        FastByteArrayOutputStream os = new FastByteArrayOutputStream();
        JsonGenerator gen = new JsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);
        gen.writeStartObject();

        gen.writeStringField("index", "test");

        gen.writeFieldName("source");
        gen.writeStartObject();
        gen.writeStringField("value", "something");
        gen.writeEndObject();

        gen.writeEndObject();

        gen.close();

        byte[] data = os.copiedByteArray();
        FastByteArrayInputStream is = new FastByteArrayInputStream(data);
        JsonParser parser = new JsonFactory().createJsonParser(is);

        assertThat(parser.nextToken(), equalTo(JsonToken.START_OBJECT));
        assertThat(parser.nextToken(), equalTo(JsonToken.FIELD_NAME)); // "index"
        assertThat(parser.nextToken(), equalTo(JsonToken.VALUE_STRING));
        assertThat(parser.nextToken(), equalTo(JsonToken.FIELD_NAME)); // "source"
        assertThat(parser.nextToken(), equalTo(JsonToken.START_OBJECT));
//        int location1 = is.position();
//        parser.skipChildren();
//        int location2 = is.position();
//        byte[] sourceData = new byte[location2 - location1];
//        System.arraycopy(data, location1, sourceData, 0, sourceData.length);
//        System.out.println(Unicode.fromBytes(sourceData));
//        JsonParser sourceParser = new JsonFactory().createJsonParser(new FastByteArrayInputStream(sourceData));
//        assertThat(sourceParser.nextToken(), equalTo(JsonToken.START_OBJECT));
//        assertThat(sourceParser.nextToken(), equalTo(JsonToken.FIELD_NAME)); // "value"
//        assertThat(sourceParser.nextToken(), equalTo(JsonToken.VALUE_STRING));
//        assertThat(sourceParser.nextToken(), equalTo(JsonToken.END_OBJECT));
    }
}
