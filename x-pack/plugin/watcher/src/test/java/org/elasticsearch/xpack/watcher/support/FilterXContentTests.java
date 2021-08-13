/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.support;

import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.common.xcontent.ObjectPath;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class FilterXContentTests extends ESTestCase {
    public void testPayloadFiltering() throws Exception {
        Map<String, Object> data = new HashMap<>();
        data.put("key0", "value1");
        data.put("key1", 2);
        data.put("key2", 3.1);
        data.put("key3", true);
        data.put("key4", Arrays.asList("value5", "value5.5"));
        data.put("key5", "value6");
        data.put("key6", 7.1);
        data.put("key7", false);

        XContentBuilder builder = jsonBuilder().map(data);
        XContentParser parser = createParser(builder);

        Set<String> keys = new HashSet<>();
        int numKeys = randomInt(3);
        for (int i = 0; i < numKeys; i++) {
            boolean added;
            do {
                added = keys.add("key" + randomInt(7));
            } while (added == false);
        }

        Map<String, Object> filteredData = XContentFilterKeysUtils.filterMapOrdered(keys, parser);
        assertThat(filteredData.size(), equalTo(numKeys));
        for (String key : keys) {
            assertThat(filteredData.get(key), equalTo(data.get(key)));
        }
    }

    public void testNestedPayloadFiltering() throws Exception {
        Map<String, Object> data = new HashMap<>();
        data.put("leaf1", MapBuilder.newMapBuilder().put("key1", "value1").put("key2", true).map());
        data.put("leaf2", MapBuilder.newMapBuilder().put("key1", "value1").put("key2", "value2").put("key3", 3).map());
        Map<Object, Object> innerMap = MapBuilder.newMapBuilder().put("key1", "value1").put("key2", "value2").map();
        data.put("leaf3", MapBuilder.newMapBuilder().put("key1", "value1").put("key2", innerMap).map());

        XContentBuilder builder = jsonBuilder().map(data);
        XContentParser parser = createParser(builder);
        Set<String> keys = new HashSet<>(Arrays.asList("leaf1.key2"));
        Map<String, Object> filteredData = XContentFilterKeysUtils.filterMapOrdered(keys, parser);
        assertThat(filteredData.size(), equalTo(1));
        assertThat(selectMap(filteredData, "leaf1").size(), equalTo(1));
        assertThat(selectMap(filteredData, "leaf1").get("key2"), Matchers.<Object>equalTo(Boolean.TRUE));

        parser = createParser(builder);
        keys = new HashSet<>(Arrays.asList("leaf2"));
        filteredData = XContentFilterKeysUtils.filterMapOrdered(keys, parser);
        assertThat(filteredData.size(), equalTo(1));
        assertThat(selectMap(filteredData, "leaf2").size(), equalTo(3));
        assertThat(selectMap(filteredData, "leaf2").get("key1"), Matchers.<Object>equalTo("value1"));
        assertThat(selectMap(filteredData, "leaf2").get("key2"), Matchers.<Object>equalTo("value2"));
        assertThat(selectMap(filteredData, "leaf2").get("key3"), Matchers.<Object>equalTo(3));

        parser = createParser(builder);
        keys = new HashSet<>(Arrays.asList("leaf3.key2.key1"));
        filteredData = XContentFilterKeysUtils.filterMapOrdered(keys, parser);
        assertThat(filteredData.size(), equalTo(1));
        assertThat(selectMap(filteredData, "leaf3").size(), equalTo(1));
        assertThat(selectMap(filteredData, "leaf3", "key2").size(), equalTo(1));
        assertThat(selectMap(filteredData, "leaf3", "key2").get("key1"), Matchers.<Object>equalTo("value1"));

        parser = createParser(builder);
        keys = new HashSet<>(Arrays.asList("leaf1.key1", "leaf2.key2"));
        filteredData = XContentFilterKeysUtils.filterMapOrdered(keys, parser);
        assertThat(filteredData.size(), equalTo(2));
        assertThat(selectMap(filteredData, "leaf1").size(), equalTo(1));
        assertThat(selectMap(filteredData, "leaf2").size(), equalTo(1));
        assertThat(selectMap(filteredData, "leaf1").get("key1"), Matchers.<Object>equalTo("value1"));
        assertThat(selectMap(filteredData, "leaf2").get("key2"), Matchers.<Object>equalTo("value2"));

        parser = createParser(builder);
        keys = new HashSet<>(Arrays.asList("leaf2.key1", "leaf2.key3"));
        filteredData = XContentFilterKeysUtils.filterMapOrdered(keys, parser);
        assertThat(filteredData.size(), equalTo(1));
        assertThat(selectMap(filteredData, "leaf2").size(), equalTo(2));
        assertThat(selectMap(filteredData, "leaf2").get("key1"), Matchers.<Object>equalTo("value1"));
        assertThat(selectMap(filteredData, "leaf2").get("key3"), Matchers.<Object>equalTo(3));

        parser = createParser(builder);
        keys = new HashSet<>(Arrays.asList("leaf3.key2.key1", "leaf3.key2.key2"));
        filteredData = XContentFilterKeysUtils.filterMapOrdered(keys, parser);
        assertThat(filteredData.size(), equalTo(1));
        assertThat(selectMap(filteredData, "leaf3").size(), equalTo(1));
        assertThat(selectMap(filteredData, "leaf3", "key2").size(), equalTo(2));
        assertThat(selectMap(filteredData, "leaf3", "key2").get("key1"), Matchers.<Object>equalTo("value1"));
        assertThat(selectMap(filteredData, "leaf3", "key2").get("key2"), Matchers.<Object>equalTo("value2"));
    }

    // issue #852
    public void testArraysAreNotCutOff() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject().startArray("buckets")
                .startObject().startObject("foo").startObject("values").endObject().endObject().endObject()
                .startObject().startObject("foo").startObject("values").endObject().endObject().endObject()
                .endArray().endObject();

        XContentParser parser = createParser(builder);

        Set<String> keys = new HashSet<>();
        keys.add("buckets.foo.values");

        Map<String, Object> filteredData = XContentFilterKeysUtils.filterMapOrdered(keys, parser);
        assertThat(filteredData.get("buckets"), instanceOf(List.class));

        // both buckets have to include the following keys
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> buckets = (List<Map<String, Object>>) filteredData.get("buckets");
        assertThat(buckets, hasSize(2));
        assertThat(buckets.get(0).keySet(), containsInAnyOrder("foo"));
        assertThat(buckets.get(1).keySet(), containsInAnyOrder("foo"));
    }

    // issue #4614
    public void testNestedArraysWork() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject().startArray("buckets")
                .startObject().startObject("foo").field("spam", "eggs").endObject().endObject()
                .startObject().startObject("foo").field("spam", "eggs2").endObject().endObject()
                .startObject().startObject("foo").field("spam", "eggs3").endObject().endObject()
                .endArray().endObject();

        XContentParser parser = createParser(builder);

        assertArrayValues(parser, "buckets.foo.spam", "eggs", "eggs2", "eggs3");
    }

    private void assertArrayValues(XContentParser parser, String key, Object ... expectedValues) throws IOException {
        Set<String> keys = new HashSet<>();
        keys.add(key);
        Map<String, Object> filteredData = XContentFilterKeysUtils.filterMapOrdered(keys, parser);
        for (int i = 0; i < expectedValues.length; i++) {
            if (expectedValues[i] instanceof String) {
                String data = ObjectPath.eval("buckets." + i + ".foo.spam", filteredData);
                assertThat(data, is(expectedValues[i]));
            } else if (expectedValues[i] instanceof Integer) {
                int data = ObjectPath.eval("buckets." + i + ".foo.spam", filteredData);
                assertThat(data, is(expectedValues[i]));
            } else if (expectedValues[i] instanceof Boolean) {
                boolean data = ObjectPath.eval("buckets." + i + ".foo.spam", filteredData);
                assertThat(data, is(expectedValues[i]));
            }
        }
    }

    public void testNestedArraysWorkWithNumbers() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject().startArray("buckets")
                .startObject().startObject("foo").field("spam", 0).endObject().endObject()
                .startObject().startObject("foo").field("spam", 1).endObject().endObject()
                .startObject().startObject("foo").field("spam", 2).endObject().endObject()
                .endArray().endObject();

        XContentParser parser = createParser(builder);

        assertArrayValues(parser, "buckets.foo.spam", 0, 1, 2);
    }

    public void testNestedArraysWorkWithBooleans() throws Exception {
        boolean[] bools = new boolean[] { randomBoolean(), randomBoolean(), randomBoolean() };

        XContentBuilder builder = jsonBuilder().startObject().startArray("buckets")
                .startObject().startObject("foo").field("spam", bools[0]).endObject().endObject()
                .startObject().startObject("foo").field("spam", bools[1]).endObject().endObject()
                .startObject().startObject("foo").field("spam", bools[2]).endObject().endObject()
                .endArray().endObject();

        XContentParser parser = createParser(builder);

        assertArrayValues(parser, "buckets.foo.spam", bools);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> selectMap(Map<String, Object> data, String... path) {
        for (String element : path) {
            data = (Map<String, Object>) data.get(element);
        }
        return data;
    }

}
