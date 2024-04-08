/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.elasticsearch.test.XContentTestUtils.insertRandomFields;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;

public class XContentTestUtilsTests extends ESTestCase {

    public void testGetInsertPaths() throws IOException {
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
                builder.startObject("inn.er2");
                {
                    builder.field("inner2field1", "value");
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();

        try (
            XContentParser parser = XContentHelper.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                BytesReference.bytes(builder),
                builder.contentType()
            )
        ) {
            parser.nextToken();
            List<String> insertPaths = XContentTestUtils.getInsertPaths(parser, new ArrayDeque<>());
            assertEquals(5, insertPaths.size());
            assertThat(insertPaths, hasItem(equalTo("")));
            assertThat(insertPaths, hasItem(equalTo("list1.2")));
            assertThat(insertPaths, hasItem(equalTo("list1.4")));
            assertThat(insertPaths, hasItem(equalTo("inner1")));
            assertThat(insertPaths, hasItem(equalTo("inner1.inn\\.er2")));
        }
    }

    @SuppressWarnings("unchecked")
    public void testInsertIntoXContent() throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        builder.endObject();
        builder = XContentTestUtils.insertIntoXContent(
            XContentType.JSON.xContent(),
            BytesReference.bytes(builder),
            Collections.singletonList(""),
            () -> "inn.er1",
            () -> new HashMap<>()
        );
        builder = XContentTestUtils.insertIntoXContent(
            XContentType.JSON.xContent(),
            BytesReference.bytes(builder),
            Collections.singletonList(""),
            () -> "field1",
            () -> "value1"
        );
        builder = XContentTestUtils.insertIntoXContent(
            XContentType.JSON.xContent(),
            BytesReference.bytes(builder),
            Collections.singletonList("inn\\.er1"),
            () -> "inner2",
            () -> new HashMap<>()
        );
        builder = XContentTestUtils.insertIntoXContent(
            XContentType.JSON.xContent(),
            BytesReference.bytes(builder),
            Collections.singletonList("inn\\.er1"),
            () -> "field2",
            () -> "value2"
        );
        try (
            XContentParser parser = XContentHelper.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                BytesReference.bytes(builder),
                builder.contentType()
            )
        ) {
            Map<String, Object> map = parser.map();
            assertEquals(2, map.size());
            assertEquals("value1", map.get("field1"));
            assertThat(map.get("inn.er1"), instanceOf(Map.class));
            Map<String, Object> innerMap = (Map<String, Object>) map.get("inn.er1");
            assertEquals(2, innerMap.size());
            assertEquals("value2", innerMap.get("field2"));
            assertThat(innerMap.get("inner2"), instanceOf(Map.class));
            assertEquals(0, ((Map<String, Object>) innerMap.get("inner2")).size());
        }
    }

    @SuppressWarnings("unchecked")
    public void testInsertRandomXContent() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("foo");
            {
                builder.field("bar", 1);
            }
            builder.endObject();
            builder.startObject("foo1");
            {
                builder.startObject("foo2");
                {
                    builder.field("buzz", 1);
                }
                builder.endObject();
            }
            builder.endObject();
            builder.field("foo3", 2);
            builder.startArray("foo4");
            {
                builder.startObject();
                {
                    builder.field("foo5", 1);
                }
                builder.endObject();
            }
            builder.endArray();
        }
        builder.endObject();

        Map<String, Object> resultMap;

        try (
            XContentParser parser = createParser(
                XContentType.JSON.xContent(),
                insertRandomFields(builder.contentType(), BytesReference.bytes(builder), null, random())
            )
        ) {
            resultMap = parser.map();
        }
        assertEquals(5, resultMap.keySet().size());
        assertEquals(2, ((Map<String, Object>) resultMap.get("foo")).keySet().size());
        Map<String, Object> foo1 = (Map<String, Object>) resultMap.get("foo1");
        assertEquals(2, foo1.keySet().size());
        assertEquals(2, ((Map<String, Object>) foo1.get("foo2")).keySet().size());
        List<Object> foo4List = (List<Object>) resultMap.get("foo4");
        assertEquals(1, foo4List.size());
        assertEquals(2, ((Map<String, Object>) foo4List.get(0)).keySet().size());

        Predicate<String> pathsToExclude = path -> path.endsWith("foo1");
        try (
            XContentParser parser = createParser(
                XContentType.JSON.xContent(),
                insertRandomFields(builder.contentType(), BytesReference.bytes(builder), pathsToExclude, random())
            )
        ) {
            resultMap = parser.map();
        }
        assertEquals(5, resultMap.keySet().size());
        assertEquals(2, ((Map<String, Object>) resultMap.get("foo")).keySet().size());
        foo1 = (Map<String, Object>) resultMap.get("foo1");
        assertEquals(1, foo1.keySet().size());
        assertEquals(2, ((Map<String, Object>) foo1.get("foo2")).keySet().size());
        foo4List = (List<Object>) resultMap.get("foo4");
        assertEquals(1, foo4List.size());
        assertEquals(2, ((Map<String, Object>) foo4List.get(0)).keySet().size());

        pathsToExclude = path -> path.contains("foo1");
        try (
            XContentParser parser = createParser(
                XContentType.JSON.xContent(),
                insertRandomFields(builder.contentType(), BytesReference.bytes(builder), pathsToExclude, random())
            )
        ) {
            resultMap = parser.map();
        }
        assertEquals(5, resultMap.keySet().size());
        assertEquals(2, ((Map<String, Object>) resultMap.get("foo")).keySet().size());
        foo1 = (Map<String, Object>) resultMap.get("foo1");
        assertEquals(1, foo1.keySet().size());
        assertEquals(1, ((Map<String, Object>) foo1.get("foo2")).keySet().size());
        foo4List = (List<Object>) resultMap.get("foo4");
        assertEquals(1, foo4List.size());
        assertEquals(2, ((Map<String, Object>) foo4List.get(0)).keySet().size());
    }
}
