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

package org.elasticsearch.util.xcontent.builder;

import org.elasticsearch.util.io.FastByteArrayOutputStream;
import org.elasticsearch.util.io.FastCharArrayWriter;
import org.elasticsearch.util.xcontent.XContentFactory;
import org.elasticsearch.util.xcontent.XContentGenerator;
import org.elasticsearch.util.xcontent.XContentType;
import org.testng.annotations.Test;

import static org.elasticsearch.util.xcontent.builder.XContentBuilder.FieldCaseConversion.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
@Test
public class XContentBuilderTests {

    @Test public void verifyReuseJsonGenerator() throws Exception {
        FastCharArrayWriter writer = new FastCharArrayWriter();
        XContentGenerator generator = XContentFactory.xContent(XContentType.JSON).createGenerator(writer);
        generator.writeStartObject();
        generator.writeStringField("test", "value");
        generator.writeEndObject();
        generator.flush();

        assertThat(writer.toStringTrim(), equalTo("{\"test\":\"value\"}"));

        // try again...
        writer.reset();
        generator.writeStartObject();
        generator.writeStringField("test", "value");
        generator.writeEndObject();
        generator.flush();
        // we get a space at the start here since it thinks we are not in the root object (fine, we will ignore it in the real code we use)
        assertThat(writer.toStringTrim(), equalTo("{\"test\":\"value\"}"));
    }

    @Test public void testSimpleGenerator() throws Exception {
        TextXContentBuilder builder = XContentFactory.contentTextBuilder(XContentType.JSON);
        builder.startObject().field("test", "value").endObject();
        assertThat(builder.string(), equalTo("{\"test\":\"value\"}"));
        builder.reset();
        builder.startObject().field("test", "value").endObject();
        assertThat(builder.string(), equalTo("{\"test\":\"value\"}"));
    }

    @Test public void testWritingBinaryToStream() throws Exception {
        FastByteArrayOutputStream bos = new FastByteArrayOutputStream();

        XContentGenerator gen = XContentFactory.xContent(XContentType.JSON).createGenerator(bos);
        gen.writeStartObject();
        gen.writeStringField("name", "something");
        gen.flush();
        bos.write(", source : { test : \"value\" }".getBytes("UTF8"));
        gen.writeStringField("name2", "something2");
        gen.writeEndObject();
        gen.close();

        byte[] data = bos.copiedByteArray();
        String sData = new String(data, "UTF8");
        System.out.println("DATA: " + sData);
    }

    @Test public void testFieldCaseConversion() throws Exception {
        TextXContentBuilder builder = XContentFactory.contentTextBuilder(XContentType.JSON).fieldCaseConversion(CAMELCASE);
        builder.startObject().field("test_name", "value").endObject();
        assertThat(builder.string(), equalTo("{\"testName\":\"value\"}"));

        builder = XContentFactory.contentTextBuilder(XContentType.JSON).fieldCaseConversion(UNDERSCORE);
        builder.startObject().field("testName", "value").endObject();
        assertThat(builder.string(), equalTo("{\"test_name\":\"value\"}"));
    }
}