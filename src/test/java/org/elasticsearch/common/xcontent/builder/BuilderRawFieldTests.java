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

package org.elasticsearch.common.xcontent.builder;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class BuilderRawFieldTests extends ElasticsearchTestCase {

    @Test
    public void testJsonRawField() throws IOException {
        testRawField(XContentType.JSON);
    }

    @Test
    public void testSmileRawField() throws IOException {
        testRawField(XContentType.SMILE);
    }

    @Test
    public void testYamlRawField() throws IOException {
        testRawField(XContentType.YAML);
    }

    @Test
    public void testCborRawField() throws IOException {
        testRawField(XContentType.CBOR);
    }

    private void testRawField(XContentType type) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(type);
        builder.startObject();
        builder.field("field1", "value1");
        builder.rawField("_source", XContentFactory.contentBuilder(type).startObject().field("s_field", "s_value").endObject().bytes());
        builder.field("field2", "value2");
        builder.rawField("payload_i", new BytesArray(Long.toString(1)));
        builder.field("field3", "value3");
        builder.rawField("payload_d", new BytesArray(Double.toString(1.1)));
        builder.field("field4", "value4");
        builder.rawField("payload_s", new BytesArray("test"));
        builder.field("field5", "value5");
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

        assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
        assertThat(parser.currentName(), equalTo("payload_i"));
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.VALUE_NUMBER));
        assertThat(parser.numberType(), equalTo(XContentParser.NumberType.INT));
        assertThat(parser.longValue(), equalTo(1l));

        assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
        assertThat(parser.currentName(), equalTo("field3"));
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.VALUE_STRING));
        assertThat(parser.text(), equalTo("value3"));

        assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
        assertThat(parser.currentName(), equalTo("payload_d"));
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.VALUE_NUMBER));
        assertThat(parser.numberType(), equalTo(XContentParser.NumberType.DOUBLE));
        assertThat(parser.doubleValue(), equalTo(1.1d));

        assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
        assertThat(parser.currentName(), equalTo("field4"));
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.VALUE_STRING));
        assertThat(parser.text(), equalTo("value4"));

        assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
        assertThat(parser.currentName(), equalTo("payload_s"));
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.VALUE_STRING));
        assertThat(parser.text(), equalTo("test"));

        assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
        assertThat(parser.currentName(), equalTo("field5"));
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.VALUE_STRING));
        assertThat(parser.text(), equalTo("value5"));

        assertThat(parser.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
    }
}
