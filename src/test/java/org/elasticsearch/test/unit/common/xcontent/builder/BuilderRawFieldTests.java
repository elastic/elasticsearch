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

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class BuilderRawFieldTests {

    @Test
    public void testJsonRawField() throws IOException {
        testRawField(XContentType.JSON);
    }

    @Test
    public void testSmileRawField() throws IOException {
        testRawField(XContentType.SMILE);
    }

    private void testRawField(XContentType type) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(type);
        builder.startObject();
        builder.field("field1", "value1");
        builder.rawField("_source", XContentFactory.contentBuilder(type).startObject().field("s_field", "s_value").endObject().bytes());
        builder.field("field2", "value2");
        builder.endObject();

        XContentParser parser = XContentFactory.xContent(type).createParser(builder.bytes());
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
        assertThat(parser.currentName(), equalTo("field1"));
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.VALUE_STRING));
        assertThat(parser.text(), equalTo("value1"));

        assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
        assertThat(parser.currentName(), equalTo("_source"));
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
        assertThat(parser.currentName(), equalTo("s_field"));
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.VALUE_STRING));
        assertThat(parser.text(), equalTo("s_value"));
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.END_OBJECT));

        assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
        assertThat(parser.currentName(), equalTo("field2"));
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.VALUE_STRING));
        assertThat(parser.text(), equalTo("value2"));
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
    }
}
