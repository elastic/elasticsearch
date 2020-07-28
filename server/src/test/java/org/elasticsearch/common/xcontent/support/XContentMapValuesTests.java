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

package org.elasticsearch.common.xcontent.support;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.elasticsearch.common.xcontent.XContentHelper.convertToMap;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsEqual.equalTo;

public class XContentMapValuesTests extends AbstractFilteringTestCase {

    @Override
    protected void testFilter(Builder expected, Builder actual, Set<String> includes, Set<String> excludes) throws IOException {
        final XContentType xContentType = randomFrom(XContentType.values());
        final boolean humanReadable = randomBoolean();

        String[] sourceIncludes;
        if (includes == null) {
            sourceIncludes = randomBoolean() ? Strings.EMPTY_ARRAY : null;
        } else {
            sourceIncludes = includes.toArray(new String[includes.size()]);
        }
        String[] sourceExcludes;
        if (excludes == null) {
            sourceExcludes = randomBoolean() ? Strings.EMPTY_ARRAY : null;
        } else {
            sourceExcludes = excludes.toArray(new String[excludes.size()]);
        }

        assertEquals("Filtered map must be equal to the expected map",
                toMap(expected, xContentType, humanReadable),
                XContentMapValues.filter(toMap(actual, xContentType, humanReadable), sourceIncludes, sourceExcludes));
    }

    @SuppressWarnings({"unchecked"})
    public void testExtractValue() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                .field("test", "value")
                .endObject();

        Map<String, Object> map;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, Strings.toString(builder))) {
            map = parser.map();
        }
        assertThat(XContentMapValues.extractValue("test", map).toString(), equalTo("value"));
        assertThat(XContentMapValues.extractValue("test.me", map), nullValue());
        assertThat(XContentMapValues.extractValue("something.else.2", map), nullValue());

        builder = XContentFactory.jsonBuilder().startObject()
                .startObject("path1").startObject("path2").field("test", "value").endObject().endObject()
                .endObject();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, Strings.toString(builder))) {
            map = parser.map();
        }
        assertThat(XContentMapValues.extractValue("path1.path2.test", map).toString(), equalTo("value"));
        assertThat(XContentMapValues.extractValue("path1.path2.test_me", map), nullValue());
        assertThat(XContentMapValues.extractValue("path1.non_path2.test", map), nullValue());

        Object extValue = XContentMapValues.extractValue("path1.path2", map);
        assertThat(extValue, instanceOf(Map.class));
        Map<String, Object> extMapValue = (Map<String, Object>) extValue;
        assertThat(extMapValue, hasEntry("test", (Object) "value"));

        extValue = XContentMapValues.extractValue("path1", map);
        assertThat(extValue, instanceOf(Map.class));
        extMapValue = (Map<String, Object>) extValue;
        assertThat(extMapValue.containsKey("path2"), equalTo(true));

        // lists
        builder = XContentFactory.jsonBuilder().startObject()
                .startObject("path1").array("test", "value1", "value2").endObject()
                .endObject();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, Strings.toString(builder))) {
            map = parser.map();
        }

        extValue = XContentMapValues.extractValue("path1.test", map);
        assertThat(extValue, instanceOf(List.class));

        List<?> extListValue = (List<?>) extValue;
        assertThat(extListValue, hasSize(2));

        builder = XContentFactory.jsonBuilder().startObject()
                .startObject("path1")
                .startArray("path2")
                .startObject().field("test", "value1").endObject()
                .startObject().field("test", "value2").endObject()
                .endArray()
                .endObject()
                .endObject();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, Strings.toString(builder))) {
            map = parser.map();
        }

        extValue = XContentMapValues.extractValue("path1.path2.test", map);
        assertThat(extValue, instanceOf(List.class));

        extListValue = (List<?>) extValue;
        assertThat(extListValue, hasSize(2));
        assertThat(extListValue.get(0).toString(), equalTo("value1"));
        assertThat(extListValue.get(1).toString(), equalTo("value2"));

        // fields with . in them
        builder = XContentFactory.jsonBuilder().startObject()
                .field("xxx.yyy", "value")
                .endObject();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, Strings.toString(builder))) {
            map = parser.map();
        }
        assertThat(XContentMapValues.extractValue("xxx.yyy", map).toString(), equalTo("value"));

        builder = XContentFactory.jsonBuilder().startObject()
                .startObject("path1.xxx").startObject("path2.yyy").field("test", "value").endObject().endObject()
                .endObject();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, Strings.toString(builder))) {
            map = parser.map();
        }
        assertThat(XContentMapValues.extractValue("path1.xxx.path2.yyy.test", map).toString(), equalTo("value"));
    }

    public void testExtractValueWithNullValue() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
            .field("field", "value")
            .nullField("other_field")
            .array("array", "value1", null, "value2")
            .startObject("object1")
                .startObject("object2").nullField("field").endObject()
            .endObject()
            .startArray("object_array")
                .startObject().nullField("field").endObject()
                .startObject().field("field", "value").endObject()
            .endArray()
        .endObject();

        Map<String, Object> map;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, Strings.toString(builder))) {
            map = parser.map();
        }
        assertEquals("value", XContentMapValues.extractValue("field", map, "NULL"));
        assertNull(XContentMapValues.extractValue("missing", map, "NULL"));
        assertNull(XContentMapValues.extractValue("field.missing", map, "NULL"));
        assertNull(XContentMapValues.extractValue("object1.missing", map, "NULL"));

        assertEquals("NULL", XContentMapValues.extractValue("other_field", map, "NULL"));
        assertEquals(List.of("value1", "NULL", "value2"), XContentMapValues.extractValue("array", map, "NULL"));
        assertEquals(List.of("NULL", "value"), XContentMapValues.extractValue("object_array.field", map, "NULL"));
        assertEquals("NULL", XContentMapValues.extractValue("object1.object2.field", map, "NULL"));
    }

    public void testExtractRawValue() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                .field("test", "value")
                .endObject();

        Map<String, Object> map;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, Strings.toString(builder))) {
            map = parser.map();
        }
        assertThat(XContentMapValues.extractRawValues("test", map).get(0).toString(), equalTo("value"));

        builder = XContentFactory.jsonBuilder().startObject()
                .field("test.me", "value")
                .endObject();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, Strings.toString(builder))) {
            map = parser.map();
        }
        assertThat(XContentMapValues.extractRawValues("test.me", map).get(0).toString(), equalTo("value"));

        builder = XContentFactory.jsonBuilder().startObject()
                .startObject("path1").startObject("path2").field("test", "value").endObject().endObject()
                .endObject();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, Strings.toString(builder))) {
            map = parser.map();
        }
        assertThat(XContentMapValues.extractRawValues("path1.path2.test", map).get(0).toString(), equalTo("value"));

        builder = XContentFactory.jsonBuilder().startObject()
                .startObject("path1.xxx").startObject("path2.yyy").field("test", "value").endObject().endObject()
                .endObject();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, Strings.toString(builder))) {
            map = parser.map();
        }
        assertThat(XContentMapValues.extractRawValues("path1.xxx.path2.yyy.test", map).get(0).toString(), equalTo("value"));
    }

    public void testPrefixedNamesFilteringTest() {
        Map<String, Object> map = new HashMap<>();
        map.put("obj", "value");
        map.put("obj_name", "value_name");
        Map<String, Object> filteredMap = XContentMapValues.filter(map, new String[]{"obj_name"}, Strings.EMPTY_ARRAY);
        assertThat(filteredMap.size(), equalTo(1));
        assertThat((String) filteredMap.get("obj_name"), equalTo("value_name"));
    }


    @SuppressWarnings("unchecked")
    public void testNestedFiltering() {
        Map<String, Object> map = new HashMap<>();
        map.put("field", "value");
        map.put("array",
                Arrays.asList(
                        1,
                        new HashMap<String, Object>() {{
                            put("nested", 2);
                            put("nested_2", 3);
                        }}));
        Map<String, Object> filteredMap = XContentMapValues.filter(map, new String[]{"array.nested"}, Strings.EMPTY_ARRAY);
        assertThat(filteredMap.size(), equalTo(1));

        assertThat(((List<?>) filteredMap.get("array")), hasSize(1));
        assertThat(((Map<String, Object>) ((List<?>) filteredMap.get("array")).get(0)).size(), equalTo(1));
        assertThat((Integer) ((Map<String, Object>) ((List<?>) filteredMap.get("array")).get(0)).get("nested"), equalTo(2));

        filteredMap = XContentMapValues.filter(map, new String[]{"array.*"}, Strings.EMPTY_ARRAY);
        assertThat(filteredMap.size(), equalTo(1));
        assertThat(((List<?>) filteredMap.get("array")), hasSize(1));
        assertThat(((Map<String, Object>) ((List<?>) filteredMap.get("array")).get(0)).size(), equalTo(2));

        map.clear();
        map.put("field", "value");
        map.put("obj",
                new HashMap<String, Object>() {{
                    put("field", "value");
                    put("field2", "value2");
                }});
        filteredMap = XContentMapValues.filter(map, new String[]{"obj.field"}, Strings.EMPTY_ARRAY);
        assertThat(filteredMap.size(), equalTo(1));
        assertThat(((Map<String, Object>) filteredMap.get("obj")).size(), equalTo(1));
        assertThat((String) ((Map<String, Object>) filteredMap.get("obj")).get("field"), equalTo("value"));

        filteredMap = XContentMapValues.filter(map, new String[]{"obj.*"}, Strings.EMPTY_ARRAY);
        assertThat(filteredMap.size(), equalTo(1));
        assertThat(((Map<String, Object>) filteredMap.get("obj")).size(), equalTo(2));
        assertThat((String) ((Map<String, Object>) filteredMap.get("obj")).get("field"), equalTo("value"));
        assertThat((String) ((Map<String, Object>) filteredMap.get("obj")).get("field2"), equalTo("value2"));

    }

    @SuppressWarnings("unchecked")
    public void testCompleteObjectFiltering() {
        Map<String, Object> map = new HashMap<>();
        map.put("field", "value");
        map.put("obj",
                new HashMap<String, Object>() {{
                    put("field", "value");
                    put("field2", "value2");
                }});
        map.put("array",
                Arrays.asList(
                        1,
                        new HashMap<String, Object>() {{
                            put("field", "value");
                            put("field2", "value2");
                        }}));

        Map<String, Object> filteredMap = XContentMapValues.filter(map, new String[]{"obj"}, Strings.EMPTY_ARRAY);
        assertThat(filteredMap.size(), equalTo(1));
        assertThat(((Map<String, Object>) filteredMap.get("obj")).size(), equalTo(2));
        assertThat(((Map<String, Object>) filteredMap.get("obj")).get("field").toString(), equalTo("value"));
        assertThat(((Map<String, Object>) filteredMap.get("obj")).get("field2").toString(), equalTo("value2"));


        filteredMap = XContentMapValues.filter(map, new String[]{"obj"}, new String[]{"*.field2"});
        assertThat(filteredMap.size(), equalTo(1));
        assertThat(((Map<String, Object>) filteredMap.get("obj")).size(), equalTo(1));
        assertThat(((Map<String, Object>) filteredMap.get("obj")).get("field").toString(), equalTo("value"));


        filteredMap = XContentMapValues.filter(map, new String[]{"array"}, new String[]{});
        assertThat(filteredMap.size(), equalTo(1));
        assertThat(((List<?>) filteredMap.get("array")).size(), equalTo(2));
        assertThat((Integer) ((List<?>) filteredMap.get("array")).get(0), equalTo(1));
        assertThat(((Map<String, Object>) ((List<?>) filteredMap.get("array")).get(1)).size(), equalTo(2));

        filteredMap = XContentMapValues.filter(map, new String[]{"array"}, new String[]{"*.field2"});
        assertThat(filteredMap.size(), equalTo(1));
        assertThat(((List<?>) filteredMap.get("array")), hasSize(2));
        assertThat((Integer) ((List<?>) filteredMap.get("array")).get(0), equalTo(1));
        assertThat(((Map<String, Object>) ((List<?>) filteredMap.get("array")).get(1)).size(), equalTo(1));
        assertThat(((Map<String, Object>) ((List<?>) filteredMap.get("array")).get(1)).get("field").toString(), equalTo("value"));
    }

    @SuppressWarnings("unchecked")
    public void testFilterIncludesUsingStarPrefix() {
        Map<String, Object> map = new HashMap<>();
        map.put("field", "value");
        map.put("obj",
                new HashMap<String, Object>() {{
                    put("field", "value");
                    put("field2", "value2");
                }});
        map.put("n_obj",
                new HashMap<String, Object>() {{
                    put("n_field", "value");
                    put("n_field2", "value2");
                }});

        Map<String, Object> filteredMap = XContentMapValues.filter(map, new String[]{"*.field2"}, Strings.EMPTY_ARRAY);
        assertThat(filteredMap.size(), equalTo(1));
        assertThat(filteredMap, hasKey("obj"));
        assertThat(((Map<String, Object>) filteredMap.get("obj")).size(), equalTo(1));
        assertThat(((Map<String, Object>) filteredMap.get("obj")), hasKey("field2"));

        // only objects
        filteredMap = XContentMapValues.filter(map, new String[]{"*.*"}, Strings.EMPTY_ARRAY);
        assertThat(filteredMap.size(), equalTo(2));
        assertThat(filteredMap, hasKey("obj"));
        assertThat(((Map<String, Object>) filteredMap.get("obj")).size(), equalTo(2));
        assertThat(filteredMap, hasKey("n_obj"));
        assertThat(((Map<String, Object>) filteredMap.get("n_obj")).size(), equalTo(2));


        filteredMap = XContentMapValues.filter(map, new String[]{"*"}, new String[]{"*.*2"});
        assertThat(filteredMap.size(), equalTo(3));
        assertThat(filteredMap, hasKey("field"));
        assertThat(filteredMap, hasKey("obj"));
        assertThat(((Map<String, Object>) filteredMap.get("obj")).size(), equalTo(1));
        assertThat(((Map<String, Object>) filteredMap.get("obj")), hasKey("field"));
        assertThat(filteredMap, hasKey("n_obj"));
        assertThat(((Map<String, Object>) filteredMap.get("n_obj")).size(), equalTo(1));
        assertThat(((Map<String, Object>) filteredMap.get("n_obj")), hasKey("n_field"));

    }

    public void testFilterWithEmptyIncludesExcludes() {
        Map<String, Object> map = new HashMap<>();
        map.put("field", "value");
        Map<String, Object> filteredMap = XContentMapValues.filter(map, Strings.EMPTY_ARRAY, Strings.EMPTY_ARRAY);
        assertThat(filteredMap.size(), equalTo(1));
        assertThat(filteredMap.get("field").toString(), equalTo("value"));
    }

    public void testThatFilterIncludesEmptyObjectWhenUsingIncludes() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                .startObject("obj")
                .endObject()
                .endObject();

        Tuple<XContentType, Map<String, Object>> mapTuple = convertToMap(BytesReference.bytes(builder), true, builder.contentType());
        Map<String, Object> filteredSource = XContentMapValues.filter(mapTuple.v2(), new String[]{"obj"}, Strings.EMPTY_ARRAY);

        assertThat(mapTuple.v2(), equalTo(filteredSource));
    }

    public void testThatFilterIncludesEmptyObjectWhenUsingExcludes() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                .startObject("obj")
                .endObject()
                .endObject();

        Tuple<XContentType, Map<String, Object>> mapTuple = convertToMap(BytesReference.bytes(builder), true, builder.contentType());
        Map<String, Object> filteredSource = XContentMapValues.filter(mapTuple.v2(), Strings.EMPTY_ARRAY, new String[]{"nonExistingField"});

        assertThat(mapTuple.v2(), equalTo(filteredSource));
    }

    @SuppressWarnings("unchecked")
    public void testNotOmittingObjectsWithExcludedProperties() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                .startObject("obj")
                .field("f1", "v1")
                .endObject()
                .endObject();

        Tuple<XContentType, Map<String, Object>> mapTuple = convertToMap(BytesReference.bytes(builder), true, builder.contentType());
        Map<String, Object> filteredSource = XContentMapValues.filter(mapTuple.v2(), Strings.EMPTY_ARRAY, new String[]{"obj.f1"});

        assertThat(filteredSource.size(), equalTo(1));
        assertThat(filteredSource, hasKey("obj"));
        assertThat(((Map<String, Object>) filteredSource.get("obj")).size(), equalTo(0));
    }

    @SuppressWarnings({"unchecked"})
    public void testNotOmittingObjectWithNestedExcludedObject() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                .startObject("obj1")
                .startObject("obj2")
                .startObject("obj3")
                .endObject()
                .endObject()
                .endObject()
                .endObject();

        // implicit include
        Tuple<XContentType, Map<String, Object>> mapTuple = convertToMap(BytesReference.bytes(builder), true, builder.contentType());
        Map<String, Object> filteredSource = XContentMapValues.filter(mapTuple.v2(), Strings.EMPTY_ARRAY, new String[]{"*.obj2"});

        assertThat(filteredSource.size(), equalTo(1));
        assertThat(filteredSource, hasKey("obj1"));
        assertThat(((Map<String, Object>) filteredSource.get("obj1")).size(), equalTo(0));

        // explicit include
        filteredSource = XContentMapValues.filter(mapTuple.v2(), new String[]{"obj1"}, new String[]{"*.obj2"});
        assertThat(filteredSource.size(), equalTo(1));
        assertThat(filteredSource, hasKey("obj1"));
        assertThat(((Map<String, Object>) filteredSource.get("obj1")).size(), equalTo(0));

        // wild card include
        filteredSource = XContentMapValues.filter(mapTuple.v2(), new String[]{"*.obj2"}, new String[]{"*.obj3"});
        assertThat(filteredSource.size(), equalTo(1));
        assertThat(filteredSource, hasKey("obj1"));
        assertThat(((Map<String, Object>) filteredSource.get("obj1")), hasKey("obj2"));
        assertThat(((Map<String, Object>) ((Map<String, Object>) filteredSource.get("obj1")).get("obj2")).size(), equalTo(0));
    }

    @SuppressWarnings({"unchecked"})
    public void testIncludingObjectWithNestedIncludedObject() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                .startObject("obj1")
                .startObject("obj2")
                .endObject()
                .endObject()
                .endObject();

        Tuple<XContentType, Map<String, Object>> mapTuple = convertToMap(BytesReference.bytes(builder), true, builder.contentType());
        Map<String, Object> filteredSource = XContentMapValues.filter(mapTuple.v2(), new String[]{"*.obj2"}, Strings.EMPTY_ARRAY);

        assertThat(filteredSource.size(), equalTo(1));
        assertThat(filteredSource, hasKey("obj1"));
        assertThat(((Map<String, Object>) filteredSource.get("obj1")).size(), equalTo(1));
        assertThat(((Map<String, Object>) filteredSource.get("obj1")), hasKey("obj2"));
        assertThat(((Map<String, Object>) ((Map<String, Object>) filteredSource.get("obj1")).get("obj2")).size(), equalTo(0));
    }


    public void testDotsInFieldNames() {
        Map<String, Object> map = new HashMap<>();
        map.put("foo.bar", 2);
        Map<String, Object> sub = new HashMap<>();
        sub.put("baz", 3);
        map.put("foo", sub);
        map.put("quux", 5);

        // dots in field names in includes
        Map<String, Object> filtered = XContentMapValues.filter(map, new String[] {"foo"}, new String[0]);
        Map<String, Object> expected = new HashMap<>(map);
        expected.remove("quux");
        assertEquals(expected, filtered);

        // dots in field names in excludes
        filtered = XContentMapValues.filter(map, new String[0], new String[] {"foo"});
        expected = new HashMap<>(map);
        expected.keySet().retainAll(Collections.singleton("quux"));
        assertEquals(expected, filtered);
    }

    public void testSupplementaryCharactersInPaths() {
        Map<String, Object> map = new HashMap<>();
        map.put("搜索", 2);
        map.put("指数", 3);

        assertEquals(Collections.singletonMap("搜索", 2), XContentMapValues.filter(map, new String[] {"搜索"}, new String[0]));
        assertEquals(Collections.singletonMap("指数", 3), XContentMapValues.filter(map, new String[0], new String[] {"搜索"}));
    }

    public void testSharedPrefixes() {
        Map<String, Object> map = new HashMap<>();
        map.put("foobar", 2);
        map.put("foobaz", 3);

        assertEquals(Collections.singletonMap("foobar", 2), XContentMapValues.filter(map, new String[] {"foobar"}, new String[0]));
        assertEquals(Collections.singletonMap("foobaz", 3), XContentMapValues.filter(map, new String[0], new String[] {"foobar"}));
    }

    @Override
    public void testSimpleArrayOfObjectsExclusive() throws Exception {
        //Empty arrays are preserved by XContentMapValues, they get removed only if explicitly excluded.
        //See following tests around this specific behaviour
        testFilter(SIMPLE_ARRAY_OF_OBJECTS_EXCLUSIVE, SAMPLE, emptySet(), singleton("authors"));
    }

    public void testArraySubFieldExclusion() {
        Map<String, Object> map = new HashMap<>();
        map.put("field", "value");
        List<Map<String, String>> array = new ArrayList<>();
        Map<String, String> object = new HashMap<>();
        object.put("exclude", "bar");
        array.add(object);
        map.put("array", array);
        Map<String, Object> filtered = XContentMapValues.filter(map, new String[0], new String[]{"array.exclude"});
        assertTrue(filtered.containsKey("field"));
        assertTrue(filtered.containsKey("array"));
        List<?> filteredArray = (List<?>)filtered.get("array");
        assertThat(filteredArray, hasSize(0));
    }

    public void testEmptyArraySubFieldsExclusion() {
        Map<String, Object> map = new HashMap<>();
        map.put("field", "value");
        List<Map<String, String>> array = new ArrayList<>();
        map.put("array", array);
        Map<String, Object> filtered = XContentMapValues.filter(map, new String[0], new String[]{"array.exclude"});
        assertTrue(filtered.containsKey("field"));
        assertTrue(filtered.containsKey("array"));
        List<?> filteredArray = (List<?>)filtered.get("array");
        assertEquals(0, filteredArray.size());
    }

    public void testEmptyArraySubFieldsInclusion() {
        Map<String, Object> map = new HashMap<>();
        map.put("field", "value");
        List<Map<String, String>> array = new ArrayList<>();
        map.put("array", array);
        {
            Map<String, Object> filtered = XContentMapValues.filter(map, new String[]{"array.include"}, new String[0]);
            assertFalse(filtered.containsKey("field"));
            assertFalse(filtered.containsKey("array"));
        }
        {
            Map<String, Object> filtered = XContentMapValues.filter(map, new String[]{"array", "array.include"},
                new String[0]);
            assertFalse(filtered.containsKey("field"));
            assertTrue(filtered.containsKey("array"));
            List<?> filteredArray = (List<?>)filtered.get("array");
            assertEquals(0, filteredArray.size());
        }
    }

    public void testEmptyObjectsSubFieldsInclusion() {
        Map<String, Object> map = new HashMap<>();
        map.put("field", "value");
        map.put("object", new HashMap<>());
        {
            Map<String, Object> filtered = XContentMapValues.filter(map, new String[]{"object.include"}, new String[0]);
            assertFalse(filtered.containsKey("field"));
            assertFalse(filtered.containsKey("object"));
        }
        {
            Map<String, Object> filtered = XContentMapValues.filter(map, new String[]{"object", "object.include"},
                new String[0]);
            assertFalse(filtered.containsKey("field"));
            assertTrue(filtered.containsKey("object"));
            Map<?, ?> filteredMap = (Map<?, ?>)filtered.get("object");
            assertEquals(0, filteredMap.size());
        }
    }

    public void testPrefix() {
        Map<String, Object> map = new HashMap<>();
        map.put("photos", Arrays.asList(new String[] {"foo", "bar"}));
        map.put("photosCount", 2);

        Map<String, Object> filtered = XContentMapValues.filter(map, new String[] {"photosCount"}, new String[0]);
        Map<String, Object> expected = new HashMap<>();
        expected.put("photosCount", 2);
        assertEquals(expected, filtered);
    }

    private static Map<String, Object> toMap(Builder test, XContentType xContentType, boolean humanReadable) throws IOException {
        ToXContentObject toXContent = (builder, params) -> test.apply(builder);
        return convertToMap(toXContent(toXContent, xContentType, humanReadable), true, xContentType).v2();
    }
}
