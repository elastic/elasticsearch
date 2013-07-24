package org.elasticsearch.test.unit.common.xcontent;
/*
 * Licensed to ElasticSearch under one
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


import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class XContentParserTests {

    @Test
    public void testDepthCounter() throws IOException {
        for (XContentType type : XContentType.values()) {
            testDepthCounterForType(type);
        }

    }


    private void testDepthCounterForType(XContentType type) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(type);
        builder.startObject();
        builder.field("field1", "value1");
        builder.startObject("obj");
        builder.field("obj_field", "v3");
        builder.startObject("obj_obj");
        builder.field("obj_obj_field", "v4");
        builder.endObject();
        builder.endObject();
        builder.field("field2", "value2");
        builder.endObject();

        XContentParser parser = XContentFactory.xContent(type).createParser(builder.bytes());
        assertThat(parser.currentDepth(), equalTo(0));
        parser.nextToken(); // start root
        assertThat(parser.currentDepth(), equalTo(0));
        parser.nextToken(); // field1
        assertThat(parser.currentDepth(), equalTo(1));
        parser.nextToken(); // field1 value
        assertThat(parser.currentDepth(), equalTo(1));
        parser.nextToken(); // obj key
        assertThat(parser.currentDepth(), equalTo(1));
        parser.nextToken(); // start obj
        assertThat(parser.currentDepth(), equalTo(1));
        parser.nextToken(); // obj_field
        assertThat(parser.currentDepth(), equalTo(2));
        parser.nextToken(); // obj_field value
        assertThat(parser.currentDepth(), equalTo(2));
        parser.nextToken(); //  obj_obj key
        assertThat(parser.currentDepth(), equalTo(2));
        parser.nextToken(); //  start obj_obj
        assertThat(parser.currentDepth(), equalTo(2));
        parser.nextToken(); // obj_obj_field
        assertThat(parser.currentDepth(), equalTo(3));
        parser.nextToken(); // obj_obj_field value
        assertThat(parser.currentDepth(), equalTo(3));
        parser.nextToken(); // end obj_obj
        assertThat(parser.currentDepth(), equalTo(2));
        parser.nextToken(); // end obj
        assertThat(parser.currentDepth(), equalTo(1));
        parser.nextToken(); // field2
        assertThat(parser.currentDepth(), equalTo(1));
        parser.nextToken(); // field2 value
        assertThat(parser.currentDepth(), equalTo(1));
        parser.nextToken(); // end root
        assertThat(parser.currentDepth(), equalTo(0));

    }
}
