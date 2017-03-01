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

package org.elasticsearch.test;

import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;;

public class XContentTestUtilsTests extends ESTestCase {

    public void testGetInserPaths() throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        {
            builder.field("field1", "value");
            builder.startArray("list1");
            {
                builder.value(0);
                builder.value(1);
                builder.startObject();
                builder.endObject();
                builder.value(3);
                builder.startObject();
                builder.endObject();
            }
            builder.endArray();
            builder.startObject("inner1");
            {
                builder.field("inner1field1", "value");
                builder.startObject("inner2");
                {
                    builder.field("inner2field1", "value");
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();

        try (XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY, builder.bytes(), builder.contentType())) {
            List<String> insertPaths = XContentTestUtils.getInsertPaths(parser);
            assertEquals(5, insertPaths.size());
            assertThat(insertPaths, hasItem(equalTo("")));
            assertThat(insertPaths, hasItem(equalTo("list1.2")));
            assertThat(insertPaths, hasItem(equalTo("list1.4")));
            assertThat(insertPaths, hasItem(equalTo("inner1")));
            assertThat(insertPaths, hasItem(equalTo("inner1.inner2")));
        }
    }

    public void testInsertInto() throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        builder.endObject();
        try (XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY, builder.bytes(), builder.contentType())) {
            builder = XContentTestUtils.insertIntoXContent(parser, Collections.singletonList(""), () -> "inner1", () -> new HashMap<>());
        }
        try (XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY, builder.bytes(), builder.contentType())) {
            builder = XContentTestUtils.insertIntoXContent(parser, Collections.singletonList(""), () -> "field1", () -> "value1");
        }
        try (XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY, builder.bytes(), builder.contentType())) {
            builder = XContentTestUtils.insertIntoXContent(parser, Collections.singletonList("inner1"), () -> "inner2",
                    () -> new HashMap<>());
        }
        try (XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY, builder.bytes(), builder.contentType())) {
            builder = XContentTestUtils.insertIntoXContent(parser, Collections.singletonList("inner1"), () -> "field2", () -> "value2");
        }
        try (XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY, builder.bytes(), builder.contentType())) {
            Map<String, Object> map = parser.map();
            assertEquals(2, map.size());
            assertEquals("value1", map.get("field1"));
            assertThat(map.get("inner1"), instanceOf(Map.class));
            Map<String, Object> innerMap = (Map<String, Object>) map.get("inner1");
            assertEquals(2, innerMap.size());
            assertEquals("value2", innerMap.get("field2"));
            assertThat(innerMap.get("inner2"), instanceOf(Map.class));
            assertEquals(0, ((Map<String, Object>) innerMap.get("inner2")).size());
        }
    }
}
