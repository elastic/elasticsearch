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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.testng.annotations.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
public class ParseRoutingTimestampTests {

    @Test public void testParseRoutingAlone() throws Exception {
        MappingMetaData md = new MappingMetaData("type1", new CompressedString(""),
                new MappingMetaData.Routing(true, "routing"),
                new MappingMetaData.Timestamp(true, "timestamp", "dateOptionalTime"));
        byte[] bytes = jsonBuilder().startObject().field("field1", "value1").field("field2", "value2")
                .field("routing", "routing_value").field("timestamp", "1").endObject().copiedBytes();
        Tuple<String, String> parsed = md.parseRoutingAndTimestamp(XContentFactory.xContent(bytes).createParser(bytes), true, false);
        assertThat(parsed.v1(), equalTo("routing_value"));
        assertThat(parsed.v2(), equalTo(null));
    }

    @Test public void testParseTimestampAlone() throws Exception {
        MappingMetaData md = new MappingMetaData("type1", new CompressedString(""),
                new MappingMetaData.Routing(true, "routing"),
                new MappingMetaData.Timestamp(true, "timestamp", "dateOptionalTime"));
        byte[] bytes = jsonBuilder().startObject().field("field1", "value1").field("field2", "value2")
                .field("routing", "routing_value").field("timestamp", "1").endObject().copiedBytes();
        Tuple<String, String> parsed = md.parseRoutingAndTimestamp(XContentFactory.xContent(bytes).createParser(bytes), false, true);
        assertThat(parsed.v1(), equalTo(null));
        assertThat(parsed.v2(), equalTo("1"));
    }

    @Test public void testParseRoutingAndTimestamp() throws Exception {
        MappingMetaData md = new MappingMetaData("type1", new CompressedString(""),
                new MappingMetaData.Routing(true, "routing"),
                new MappingMetaData.Timestamp(true, "timestamp", "dateOptionalTime"));
        byte[] bytes = jsonBuilder().startObject().field("field1", "value1").field("field2", "value2")
                .field("routing", "routing_value").field("timestamp", "1").endObject().copiedBytes();
        Tuple<String, String> parsed = md.parseRoutingAndTimestamp(XContentFactory.xContent(bytes).createParser(bytes), true, true);
        assertThat(parsed.v1(), equalTo("routing_value"));
        assertThat(parsed.v2(), equalTo("1"));
    }

    @Test public void testParseRoutingAndTimestampWithPath() throws Exception {
        MappingMetaData md = new MappingMetaData("type1", new CompressedString(""),
                new MappingMetaData.Routing(true, "obj1.routing"),
                new MappingMetaData.Timestamp(true, "obj2.timestamp", "dateOptionalTime"));
        byte[] bytes = jsonBuilder().startObject().field("field1", "value1").field("field2", "value2")
                .startObject("obj0").field("field1", "value1").field("field2", "value2").endObject()
                .startObject("obj1").field("routing", "routing_value").endObject()
                .startObject("obj2").field("timestamp", "1").endObject()
                .endObject().copiedBytes();
        Tuple<String, String> parsed = md.parseRoutingAndTimestamp(XContentFactory.xContent(bytes).createParser(bytes), true, true);
        assertThat(parsed.v1(), equalTo("routing_value"));
        assertThat(parsed.v2(), equalTo("1"));
    }

    @Test public void testParseRoutingAndTimestampWithinSamePath() throws Exception {
        MappingMetaData md = new MappingMetaData("type1", new CompressedString(""),
                new MappingMetaData.Routing(true, "obj1.routing"),
                new MappingMetaData.Timestamp(true, "obj1.timestamp", "dateOptionalTime"));
        byte[] bytes = jsonBuilder().startObject().field("field1", "value1").field("field2", "value2")
                .startObject("obj0").field("field1", "value1").field("field2", "value2").endObject()
                .startObject("obj1").field("routing", "routing_value").field("timestamp", "1").endObject()
                .startObject("obj2").field("field1", "value1").endObject()
                .endObject().copiedBytes();
        Tuple<String, String> parsed = md.parseRoutingAndTimestamp(XContentFactory.xContent(bytes).createParser(bytes), true, true);
        assertThat(parsed.v1(), equalTo("routing_value"));
        assertThat(parsed.v2(), equalTo("1"));
    }

    @Test public void testParseRoutingAndTimestampWithinSamePathAndMoreLevels() throws Exception {
        MappingMetaData md = new MappingMetaData("type1", new CompressedString(""),
                new MappingMetaData.Routing(true, "obj1.obj2.routing"),
                new MappingMetaData.Timestamp(true, "obj1.obj3.timestamp", "dateOptionalTime"));
        byte[] bytes = jsonBuilder().startObject().field("field1", "value1").field("field2", "value2")
                .startObject("obj0").field("field1", "value1").field("field2", "value2").endObject()
                .startObject("obj1")
                    .startObject("obj2")
                        .field("routing", "routing_value")
                    .endObject()
                    .startObject("obj3")
                        .field("timestamp", "1")
                    .endObject()
                .endObject()
                .startObject("obj2").field("field1", "value1").endObject()
                .endObject().copiedBytes();
        Tuple<String, String> parsed = md.parseRoutingAndTimestamp(XContentFactory.xContent(bytes).createParser(bytes), true, true);
        assertThat(parsed.v1(), equalTo("routing_value"));
        assertThat(parsed.v2(), equalTo("1"));
    }


    @Test public void testParseRoutingAndTimestampWithSameRepeatedObject() throws Exception {
        MappingMetaData md = new MappingMetaData("type1", new CompressedString(""),
                new MappingMetaData.Routing(true, "obj1.routing"),
                new MappingMetaData.Timestamp(true, "obj1.timestamp", "dateOptionalTime"));
        byte[] bytes = jsonBuilder().startObject().field("field1", "value1").field("field2", "value2")
                .startObject("obj0").field("field1", "value1").field("field2", "value2").endObject()
                .startObject("obj1").field("routing", "routing_value").endObject()
                .startObject("obj1").field("timestamp", "1").endObject()
                .endObject().copiedBytes();
        Tuple<String, String> parsed = md.parseRoutingAndTimestamp(XContentFactory.xContent(bytes).createParser(bytes), true, true);
        assertThat(parsed.v1(), equalTo("routing_value"));
        assertThat(parsed.v2(), equalTo("1"));
    }

    @Test public void testParseRoutingTimestampWithRepeatedField() throws Exception {
        MappingMetaData md = new MappingMetaData("type1", new CompressedString(""),
                new MappingMetaData.Routing(true, "field1.field1"),
                new MappingMetaData.Timestamp(true, "field1", "dateOptionalTime"));

        byte[] bytes = jsonBuilder().startObject()
                .field("aaa", "wr")
                .array("arr1", "1", "2", "3")
                .field("field1", "foo")
                .field("field1", "bar")
                .field("test", "value")
                .field("zzz", "wr")
                .endObject().copiedBytes();

        Tuple<String, String> parsed = md.parseRoutingAndTimestamp(XContentFactory.xContent(bytes).createParser(bytes), true, true);
        assertThat(parsed.v1(), equalTo(null));
        assertThat(parsed.v2(), equalTo("foo"));
    }

    @Test public void testParseRoutingWithRepeatedFieldAndObject() throws Exception {
        MappingMetaData md = new MappingMetaData("type1", new CompressedString(""),
                new MappingMetaData.Routing(true, "field1.field1.field2"),
                new MappingMetaData.Timestamp(true, "field1", "dateOptionalTime"));

        byte[] bytes = jsonBuilder().startObject()
                .field("aaa", "wr")
                .array("arr1", "1", "2", "3")
                .field("field1", "foo")
                .startObject("field1").field("field2", "bar").endObject()
                .field("test", "value")
                .field("zzz", "wr")
                .endObject().copiedBytes();

        Tuple<String, String> parsed = md.parseRoutingAndTimestamp(XContentFactory.xContent(bytes).createParser(bytes), true, true);
        assertThat(parsed.v1(), equalTo(null));
        assertThat(parsed.v2(), equalTo("foo"));
    }

    @Test public void testParseRoutingWithRepeatedFieldAndValidRouting() throws Exception {
        MappingMetaData md = new MappingMetaData("type1", new CompressedString(""),
                new MappingMetaData.Routing(true, "field1.field2"),
                new MappingMetaData.Timestamp(true, "field1", "dateOptionalTime"));

        byte[] bytes = jsonBuilder().startObject()
                .field("aaa", "wr")
                .array("arr1", "1", "2", "3")
                .field("field1", "foo")
                .startObject("field1").field("field2", "bar").endObject()
                .field("test", "value")
                .field("zzz", "wr")
                .endObject().copiedBytes();

        Tuple<String, String> parsed = md.parseRoutingAndTimestamp(XContentFactory.xContent(bytes).createParser(bytes), true, true);
        assertThat(parsed.v1(), equalTo("bar"));
        assertThat(parsed.v2(), equalTo("foo"));
    }
}
