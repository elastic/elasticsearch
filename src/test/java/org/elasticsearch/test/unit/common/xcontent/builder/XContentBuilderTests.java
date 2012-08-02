/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.test.unit.common.xcontent.builder;

import com.google.common.collect.Lists;
import org.elasticsearch.common.io.FastByteArrayOutputStream;
import org.elasticsearch.common.io.FastCharArrayWriter;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentGenerator;
import org.elasticsearch.common.xcontent.XContentType;
import org.testng.annotations.Test;

import static org.elasticsearch.common.xcontent.XContentBuilder.FieldCaseConversion.CAMELCASE;
import static org.elasticsearch.common.xcontent.XContentBuilder.FieldCaseConversion.UNDERSCORE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
@Test
public class XContentBuilderTests {

    @Test
    public void verifyReuseJsonGenerator() throws Exception {
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

    @Test
    public void testSimpleGenerator() throws Exception {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject().field("test", "value").endObject();
        assertThat(builder.string(), equalTo("{\"test\":\"value\"}"));

        builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject().field("test", "value").endObject();
        assertThat(builder.string(), equalTo("{\"test\":\"value\"}"));
    }

    @Test
    public void testOverloadedList() throws Exception {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject().field("test", Lists.newArrayList("1", "2")).endObject();
        assertThat(builder.string(), equalTo("{\"test\":[\"1\",\"2\"]}"));
    }

    @Test
    public void testWritingBinaryToStream() throws Exception {
        FastByteArrayOutputStream bos = new FastByteArrayOutputStream();

        XContentGenerator gen = XContentFactory.xContent(XContentType.JSON).createGenerator(bos);
        gen.writeStartObject();
        gen.writeStringField("name", "something");
        gen.flush();
        bos.write(", source : { test : \"value\" }".getBytes("UTF8"));
        gen.writeStringField("name2", "something2");
        gen.writeEndObject();
        gen.close();

        byte[] data = bos.bytes().toBytes();
        String sData = new String(data, "UTF8");
        System.out.println("DATA: " + sData);
    }

    @Test
    public void testFieldCaseConversion() throws Exception {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON).fieldCaseConversion(CAMELCASE);
        builder.startObject().field("test_name", "value").endObject();
        assertThat(builder.string(), equalTo("{\"testName\":\"value\"}"));

        builder = XContentFactory.contentBuilder(XContentType.JSON).fieldCaseConversion(UNDERSCORE);
        builder.startObject().field("testName", "value").endObject();
        assertThat(builder.string(), equalTo("{\"test_name\":\"value\"}"));
    }
}