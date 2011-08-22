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

import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.testng.annotations.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
public class ParseRoutingTests {

    @Test public void testParseRouting() throws Exception {
        MappingMetaData md = new MappingMetaData("type1", new CompressedString(""), new MappingMetaData.Routing(true, "test"));
        byte[] bytes = jsonBuilder().startObject().field("aaa", "wr").field("test", "value").field("zzz", "wr").endObject().copiedBytes();
        assertThat(md.parseRouting(XContentFactory.xContent(bytes).createParser(bytes)), equalTo("value"));

        bytes = jsonBuilder().startObject()
                .field("aaa", "wr")
                .array("arr1", "1", "2", "3")
                .startObject("obj1").field("ob1_field", "obj1_value").endObject()
                .field("test", "value")
                .field("zzz", "wr")
                .endObject().copiedBytes();
        assertThat(md.parseRouting(XContentFactory.xContent(bytes).createParser(bytes)), equalTo("value"));
    }

    @Test public void testParseRoutingWithPath() throws Exception {
        MappingMetaData md = new MappingMetaData("type1", new CompressedString(""), new MappingMetaData.Routing(true, "obj1.field2"));

        byte[] bytes = jsonBuilder().startObject()
                .field("aaa", "wr")
                .array("arr1", "1", "2", "3")
                .startObject("obj1").field("field1", "value1").field("field2", "value2").endObject()
                .field("test", "value")
                .field("zzz", "wr")
                .endObject().copiedBytes();
        assertThat(md.parseRouting(XContentFactory.xContent(bytes).createParser(bytes)), equalTo("value2"));
    }

    @Test public void testParseRoutingWithRepeatedField() throws Exception {
        MappingMetaData md = new MappingMetaData("type1", new CompressedString(""), new MappingMetaData.Routing(true, "field1.field1"));

        byte[] bytes = jsonBuilder().startObject()
                .field("aaa", "wr")
                .array("arr1", "1", "2", "3")
                .field("field1", "foo")
                .field("field1", "bar")
                .field("test", "value")
                .field("zzz", "wr")
                .endObject().copiedBytes();
        assertThat(md.parseRouting(XContentFactory.xContent(bytes).createParser(bytes)), equalTo(null));
    }

    @Test public void testParseRoutingWithRepeatedFieldAndObject() throws Exception {
        MappingMetaData md = new MappingMetaData("type1", new CompressedString(""), new MappingMetaData.Routing(true, "field1.field1.field2"));

        byte[] bytes = jsonBuilder().startObject()
                .field("aaa", "wr")
                .array("arr1", "1", "2", "3")
                .field("field1", "foo")
                .startObject("field1").field("field2", "bar").endObject()
                .field("test", "value")
                .field("zzz", "wr")
                .endObject().copiedBytes();
        assertThat(md.parseRouting(XContentFactory.xContent(bytes).createParser(bytes)), equalTo(null));
    }

    @Test public void testParseRoutingWithRepeatedFieldAndValidRouting() throws Exception {
        MappingMetaData md = new MappingMetaData("type1", new CompressedString(""), new MappingMetaData.Routing(true, "field1.field2"));

        byte[] bytes = jsonBuilder().startObject()
                .field("aaa", "wr")
                .array("arr1", "1", "2", "3")
                .field("field1", "foo")
                .startObject("field1").field("field2", "bar").endObject()
                .field("test", "value")
                .field("zzz", "wr")
                .endObject().copiedBytes();
        assertThat(md.parseRouting(XContentFactory.xContent(bytes).createParser(bytes)), equalTo("bar"));
    }
}
