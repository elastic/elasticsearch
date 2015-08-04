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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.internal.TimestampFieldMapper;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class MappingMetaDataParserTests extends ESTestCase {

    @Test
    public void testParseIdAlone() throws Exception {
        MappingMetaData md = new MappingMetaData("type1", new CompressedXContent("{}"),
                new MappingMetaData.Id("id"),
                new MappingMetaData.Routing(true, "routing"),
                new MappingMetaData.Timestamp(true, "timestamp", "dateOptionalTime", TimestampFieldMapper.Defaults.DEFAULT_TIMESTAMP, null), false);
        byte[] bytes = jsonBuilder().startObject().field("field1", "value1").field("field2", "value2")
                .field("id", "id").field("routing", "routing_value").field("timestamp", "1").endObject().bytes().toBytes();
        MappingMetaData.ParseContext parseContext = md.createParseContext(null, "routing_value", "1");
        md.parse(XContentFactory.xContent(bytes).createParser(bytes), parseContext);
        assertThat(parseContext.id(), equalTo("id"));
        assertThat(parseContext.idResolved(), equalTo(true));
        assertThat(parseContext.routing(), equalTo("routing_value"));
        assertThat(parseContext.routingResolved(), equalTo(true));
        assertThat(parseContext.timestamp(), nullValue());
        assertThat(parseContext.timestampResolved(), equalTo(false));
    }
    
    @Test
    public void testFailIfIdIsNoValue() throws Exception {
        MappingMetaData md = new MappingMetaData("type1", new CompressedXContent("{}"),
                new MappingMetaData.Id("id"),
                new MappingMetaData.Routing(true, "routing"),
                new MappingMetaData.Timestamp(true, "timestamp", "dateOptionalTime", TimestampFieldMapper.Defaults.DEFAULT_TIMESTAMP, null), false);
        byte[] bytes = jsonBuilder().startObject().field("field1", "value1").field("field2", "value2")
                .startArray("id").value("id").endArray().field("routing", "routing_value").field("timestamp", "1").endObject().bytes().toBytes();
        MappingMetaData.ParseContext parseContext = md.createParseContext(null, "routing_value", "1");
        try {
            md.parse(XContentFactory.xContent(bytes).createParser(bytes), parseContext);
        fail();
        } catch (MapperParsingException ex) {
            // bogus its an array
        }
        
        bytes = jsonBuilder().startObject().field("field1", "value1").field("field2", "value2")
                .startObject("id").field("x", "id").endObject().field("routing", "routing_value").field("timestamp", "1").endObject().bytes().toBytes();
        parseContext = md.createParseContext(null, "routing_value", "1");
        try {
            md.parse(XContentFactory.xContent(bytes).createParser(bytes), parseContext);
        fail();
        } catch (MapperParsingException ex) {
            // bogus its an object
        }
    }

    @Test
    public void testParseRoutingAlone() throws Exception {
        MappingMetaData md = new MappingMetaData("type1", new CompressedXContent("{}"),
                new MappingMetaData.Id("id"),
                new MappingMetaData.Routing(true, "routing"),
                new MappingMetaData.Timestamp(true, "timestamp", "dateOptionalTime", TimestampFieldMapper.Defaults.DEFAULT_TIMESTAMP, null), false);
        byte[] bytes = jsonBuilder().startObject().field("field1", "value1").field("field2", "value2")
                .field("id", "id").field("routing", "routing_value").field("timestamp", "1").endObject().bytes().toBytes();
        MappingMetaData.ParseContext parseContext = md.createParseContext("id", null, "1");
        md.parse(XContentFactory.xContent(bytes).createParser(bytes), parseContext);
        assertThat(parseContext.id(), nullValue());
        assertThat(parseContext.idResolved(), equalTo(false));
        assertThat(parseContext.routing(), equalTo("routing_value"));
        assertThat(parseContext.routingResolved(), equalTo(true));
        assertThat(parseContext.timestamp(), nullValue());
        assertThat(parseContext.timestampResolved(), equalTo(false));
    }

    @Test
    public void testParseTimestampAlone() throws Exception {
        MappingMetaData md = new MappingMetaData("type1", new CompressedXContent("{}"),
                new MappingMetaData.Id("id"),
                new MappingMetaData.Routing(true, "routing"),
                new MappingMetaData.Timestamp(true, "timestamp", "dateOptionalTime", TimestampFieldMapper.Defaults.DEFAULT_TIMESTAMP, null), false);
        byte[] bytes = jsonBuilder().startObject().field("field1", "value1").field("field2", "value2")
                .field("id", "id").field("routing", "routing_value").field("timestamp", "1").endObject().bytes().toBytes();
        MappingMetaData.ParseContext parseContext = md.createParseContext("id", "routing_value1", null);
        md.parse(XContentFactory.xContent(bytes).createParser(bytes), parseContext);
        assertThat(parseContext.id(), nullValue());
        assertThat(parseContext.idResolved(), equalTo(false));
        assertThat(parseContext.routing(), equalTo("routing_value"));
        assertThat(parseContext.routingResolved(), equalTo(true));
        assertThat(parseContext.timestamp(), equalTo("1"));
        assertThat(parseContext.timestampResolved(), equalTo(true));
    }

    @Test
    public void testParseTimestampEquals() throws Exception {
        MappingMetaData md1 = new MappingMetaData("type1", new CompressedXContent("{}"),
                new MappingMetaData.Id("id"),
                new MappingMetaData.Routing(true, "routing"),
                new MappingMetaData.Timestamp(true, "timestamp", "dateOptionalTime", TimestampFieldMapper.Defaults.DEFAULT_TIMESTAMP, null), false);
        MappingMetaData md2 = new MappingMetaData("type1", new CompressedXContent("{}"),
                new MappingMetaData.Id("id"),
                new MappingMetaData.Routing(true, "routing"),
                new MappingMetaData.Timestamp(true, "timestamp", "dateOptionalTime", TimestampFieldMapper.Defaults.DEFAULT_TIMESTAMP, null), false);
        assertThat(md1, equalTo(md2));
    }

    @Test
    public void testParseIdAndRoutingAndTimestamp() throws Exception {
        MappingMetaData md = new MappingMetaData("type1", new CompressedXContent("{}"),
                new MappingMetaData.Id("id"),
                new MappingMetaData.Routing(true, "routing"),
                new MappingMetaData.Timestamp(true, "timestamp", "dateOptionalTime", TimestampFieldMapper.Defaults.DEFAULT_TIMESTAMP, null), false);
        byte[] bytes = jsonBuilder().startObject().field("field1", "value1").field("field2", "value2")
                .field("id", "id").field("routing", "routing_value").field("timestamp", "1").endObject().bytes().toBytes();
        MappingMetaData.ParseContext parseContext = md.createParseContext(null, null, null);
        md.parse(XContentFactory.xContent(bytes).createParser(bytes), parseContext);
        assertThat(parseContext.id(), equalTo("id"));
        assertThat(parseContext.routing(), equalTo("routing_value"));
        assertThat(parseContext.timestamp(), equalTo("1"));
    }

    @Test
    public void testParseIdAndRoutingAndTimestampWithPath() throws Exception {
        MappingMetaData md = new MappingMetaData("type1", new CompressedXContent("{}"),
                new MappingMetaData.Id("obj1.id"),
                new MappingMetaData.Routing(true, "obj1.routing"),
                new MappingMetaData.Timestamp(true, "obj2.timestamp", "dateOptionalTime", TimestampFieldMapper.Defaults.DEFAULT_TIMESTAMP, null), false);
        byte[] bytes = jsonBuilder().startObject().field("field1", "value1").field("field2", "value2")
                .startObject("obj0").field("field1", "value1").field("field2", "value2").endObject()
                .startObject("obj1").field("id", "id").field("routing", "routing_value").endObject()
                .startObject("obj2").field("timestamp", "1").endObject()
                .endObject().bytes().toBytes();
        MappingMetaData.ParseContext parseContext = md.createParseContext(null, null, null);
        md.parse(XContentFactory.xContent(bytes).createParser(bytes), parseContext);
        assertThat(parseContext.id(), equalTo("id"));
        assertThat(parseContext.routing(), equalTo("routing_value"));
        assertThat(parseContext.timestamp(), equalTo("1"));
    }

    @Test
    public void testParseIdWithPath() throws Exception {
        MappingMetaData md = new MappingMetaData("type1", new CompressedXContent("{}"),
                new MappingMetaData.Id("obj1.id"),
                new MappingMetaData.Routing(true, "obj1.routing"),
                new MappingMetaData.Timestamp(true, "obj2.timestamp", "dateOptionalTime", TimestampFieldMapper.Defaults.DEFAULT_TIMESTAMP, null), false);
        byte[] bytes = jsonBuilder().startObject().field("field1", "value1").field("field2", "value2")
                .startObject("obj0").field("field1", "value1").field("field2", "value2").endObject()
                .startObject("obj1").field("id", "id").field("routing", "routing_value").endObject()
                .startObject("obj2").field("timestamp", "1").endObject()
                .endObject().bytes().toBytes();
        MappingMetaData.ParseContext parseContext = md.createParseContext(null, "routing_value", "2");
        md.parse(XContentFactory.xContent(bytes).createParser(bytes), parseContext);
        assertThat(parseContext.id(), equalTo("id"));
        assertThat(parseContext.idResolved(), equalTo(true));
        assertThat(parseContext.routing(), equalTo("routing_value"));
        assertThat(parseContext.routingResolved(), equalTo(true));
        assertThat(parseContext.timestamp(), nullValue());
        assertThat(parseContext.timestampResolved(), equalTo(false));
    }

    @Test
    public void testParseRoutingWithPath() throws Exception {
        MappingMetaData md = new MappingMetaData("type1", new CompressedXContent("{}"),
                new MappingMetaData.Id("obj1.id"),
                new MappingMetaData.Routing(true, "obj1.routing"),
                new MappingMetaData.Timestamp(true, "obj2.timestamp", "dateOptionalTime", TimestampFieldMapper.Defaults.DEFAULT_TIMESTAMP, null), false);
        byte[] bytes = jsonBuilder().startObject().field("field1", "value1").field("field2", "value2")
                .startObject("obj0").field("field1", "value1").field("field2", "value2").endObject()
                .startObject("obj1").field("id", "id").field("routing", "routing_value").endObject()
                .startObject("obj2").field("timestamp", "1").endObject()
                .endObject().bytes().toBytes();
        MappingMetaData.ParseContext parseContext = md.createParseContext("id", null, "2");
        md.parse(XContentFactory.xContent(bytes).createParser(bytes), parseContext);
        assertThat(parseContext.id(), nullValue());
        assertThat(parseContext.idResolved(), equalTo(false));
        assertThat(parseContext.routing(), equalTo("routing_value"));
        assertThat(parseContext.routingResolved(), equalTo(true));
        assertThat(parseContext.timestamp(), nullValue());
        assertThat(parseContext.timestampResolved(), equalTo(false));
    }

    @Test
    public void testParseTimestampWithPath() throws Exception {
        MappingMetaData md = new MappingMetaData("type1", new CompressedXContent("{}"),
                new MappingMetaData.Id("obj1.id"),
                new MappingMetaData.Routing(true, "obj1.routing"),
                new MappingMetaData.Timestamp(true, "obj2.timestamp", "dateOptionalTime", TimestampFieldMapper.Defaults.DEFAULT_TIMESTAMP, null), false);
        byte[] bytes = jsonBuilder().startObject().field("field1", "value1").field("field2", "value2")
                .startObject("obj0").field("field1", "value1").field("field2", "value2").endObject()
                .startObject("obj1").field("routing", "routing_value").endObject()
                .startObject("obj2").field("timestamp", "1").endObject()
                .endObject().bytes().toBytes();
        MappingMetaData.ParseContext parseContext = md.createParseContext(null, "routing_value1", null);
        md.parse(XContentFactory.xContent(bytes).createParser(bytes), parseContext);
        assertThat(parseContext.id(), nullValue());
        assertThat(parseContext.idResolved(), equalTo(false));
        assertThat(parseContext.routing(), equalTo("routing_value"));
        assertThat(parseContext.routingResolved(), equalTo(true));
        assertThat(parseContext.timestamp(), equalTo("1"));
        assertThat(parseContext.timestampResolved(), equalTo(true));
    }

    @Test
    public void testParseIdAndRoutingAndTimestampWithinSamePath() throws Exception {
        MappingMetaData md = new MappingMetaData("type1", new CompressedXContent("{}"),
                new MappingMetaData.Id("obj1.id"),
                new MappingMetaData.Routing(true, "obj1.routing"),
                new MappingMetaData.Timestamp(true, "obj1.timestamp", "dateOptionalTime", TimestampFieldMapper.Defaults.DEFAULT_TIMESTAMP, null), false);
        byte[] bytes = jsonBuilder().startObject().field("field1", "value1").field("field2", "value2")
                .startObject("obj0").field("field1", "value1").field("field2", "value2").endObject()
                .startObject("obj1").field("id", "id").field("routing", "routing_value").field("timestamp", "1").endObject()
                .startObject("obj2").field("field1", "value1").endObject()
                .endObject().bytes().toBytes();
        MappingMetaData.ParseContext parseContext = md.createParseContext(null, null, null);
        md.parse(XContentFactory.xContent(bytes).createParser(bytes), parseContext);
        assertThat(parseContext.id(), equalTo("id"));
        assertThat(parseContext.routing(), equalTo("routing_value"));
        assertThat(parseContext.timestamp(), equalTo("1"));
    }

    @Test
    public void testParseIdAndRoutingAndTimestampWithinSamePathAndMoreLevels() throws Exception {
        MappingMetaData md = new MappingMetaData("type1", new CompressedXContent("{}"),
                new MappingMetaData.Id("obj1.obj0.id"),
                new MappingMetaData.Routing(true, "obj1.obj2.routing"),
                new MappingMetaData.Timestamp(true, "obj1.obj3.timestamp", "dateOptionalTime", TimestampFieldMapper.Defaults.DEFAULT_TIMESTAMP, null), false);
        byte[] bytes = jsonBuilder().startObject().field("field1", "value1").field("field2", "value2")
                .startObject("obj0").field("field1", "value1").field("field2", "value2").endObject()
                .startObject("obj1")
                .startObject("obj0")
                .field("id", "id")
                .endObject()
                .startObject("obj2")
                .field("routing", "routing_value")
                .endObject()
                .startObject("obj3")
                .field("timestamp", "1")
                .endObject()
                .endObject()
                .startObject("obj2").field("field1", "value1").endObject()
                .endObject().bytes().toBytes();
        MappingMetaData.ParseContext parseContext = md.createParseContext(null, null, null);
        md.parse(XContentFactory.xContent(bytes).createParser(bytes), parseContext);
        assertThat(parseContext.id(), equalTo("id"));
        assertThat(parseContext.routing(), equalTo("routing_value"));
        assertThat(parseContext.timestamp(), equalTo("1"));
    }


    @Test
    public void testParseIdAndRoutingAndTimestampWithSameRepeatedObject() throws Exception {
        MappingMetaData md = new MappingMetaData("type1", new CompressedXContent("{}"),
                new MappingMetaData.Id("obj1.id"),
                new MappingMetaData.Routing(true, "obj1.routing"),
                new MappingMetaData.Timestamp(true, "obj1.timestamp", "dateOptionalTime", TimestampFieldMapper.Defaults.DEFAULT_TIMESTAMP, null), false);
        byte[] bytes = jsonBuilder().startObject().field("field1", "value1").field("field2", "value2")
                .startObject("obj0").field("field1", "value1").field("field2", "value2").endObject()
                .startObject("obj1").field("id", "id").endObject()
                .startObject("obj1").field("routing", "routing_value").endObject()
                .startObject("obj1").field("timestamp", "1").endObject()
                .endObject().bytes().toBytes();
        MappingMetaData.ParseContext parseContext = md.createParseContext(null, null, null);
        md.parse(XContentFactory.xContent(bytes).createParser(bytes), parseContext);
        assertThat(parseContext.id(), equalTo("id"));
        assertThat(parseContext.routing(), equalTo("routing_value"));
        assertThat(parseContext.timestamp(), equalTo("1"));
    }

    //
    @Test
    public void testParseIdRoutingTimestampWithRepeatedField() throws Exception {
        MappingMetaData md = new MappingMetaData("type1", new CompressedXContent("{}"),
                new MappingMetaData.Id("field1"),
                new MappingMetaData.Routing(true, "field1.field1"),
                new MappingMetaData.Timestamp(true, "field1", "dateOptionalTime", TimestampFieldMapper.Defaults.DEFAULT_TIMESTAMP, null), false);

        byte[] bytes = jsonBuilder().startObject()
                .field("aaa", "wr")
                .array("arr1", "1", "2", "3")
                .field("field1", "foo")
                .field("field1", "bar")
                .field("test", "value")
                .field("zzz", "wr")
                .endObject().bytes().toBytes();

        MappingMetaData.ParseContext parseContext = md.createParseContext(null, null, null);
        md.parse(XContentFactory.xContent(bytes).createParser(bytes), parseContext);
        assertThat(parseContext.id(), equalTo("foo"));
        assertThat(parseContext.routing(), nullValue());
        assertThat(parseContext.timestamp(), equalTo("foo"));
    }

    @Test
    public void testParseNoIdRoutingWithRepeatedFieldAndObject() throws Exception {
        MappingMetaData md = new MappingMetaData("type1", new CompressedXContent("{}"),
                new MappingMetaData.Id("id"),
                new MappingMetaData.Routing(true, "field1.field1.field2"),
                new MappingMetaData.Timestamp(true, "field1", "dateOptionalTime", TimestampFieldMapper.Defaults.DEFAULT_TIMESTAMP, null), false);

        byte[] bytes = jsonBuilder().startObject()
                .field("aaa", "wr")
                .array("arr1", "1", "2", "3")
                .field("field1", "foo")
                .startObject("field1").field("field2", "bar").endObject()
                .field("test", "value")
                .field("zzz", "wr")
                .endObject().bytes().toBytes();

        MappingMetaData.ParseContext parseContext = md.createParseContext(null, null, null);
        md.parse(XContentFactory.xContent(bytes).createParser(bytes), parseContext);
        assertThat(parseContext.id(), nullValue());
        assertThat(parseContext.routing(), nullValue());
        assertThat(parseContext.timestamp(), equalTo("foo"));
    }

    @Test
    public void testParseRoutingWithRepeatedFieldAndValidRouting() throws Exception {
        MappingMetaData md = new MappingMetaData("type1", new CompressedXContent("{}"),
                new MappingMetaData.Id(null),
                new MappingMetaData.Routing(true, "field1.field2"),
                new MappingMetaData.Timestamp(true, "field1", "dateOptionalTime", TimestampFieldMapper.Defaults.DEFAULT_TIMESTAMP, null), false);

        byte[] bytes = jsonBuilder().startObject()
                .field("aaa", "wr")
                .array("arr1", "1", "2", "3")
                .field("field1", "foo")
                .startObject("field1").field("field2", "bar").endObject()
                .field("test", "value")
                .field("zzz", "wr")
                .endObject().bytes().toBytes();

        MappingMetaData.ParseContext parseContext = md.createParseContext(null, null, null);
        md.parse(XContentFactory.xContent(bytes).createParser(bytes), parseContext);
        assertThat(parseContext.id(), nullValue());
        assertThat(parseContext.routing(), equalTo("bar"));
        assertThat(parseContext.timestamp(), equalTo("foo"));
    }
}
