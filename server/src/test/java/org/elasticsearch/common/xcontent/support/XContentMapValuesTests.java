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
import org.junit.Assert;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentHelper.convertToMap;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsEqual.equalTo;

public class XContentMapValuesTests extends AbstractFilteringTestCase {

    private static final String NO_INCLUDE = null;
    private static final String NO_EXCLUDE = null;
    private static final Map<String, Object> EMPTY = Collections.emptyMap();

    public void testFieldWithEmptyObject() {
        Map<String, Object> root = map("abc", new HashMap<>());

        filterAndCheck(root, NO_INCLUDE, NO_EXCLUDE, root);
    }

    public void testEmptyObjectInclude() {
        Map<String, Object> root = new HashMap<>();

        filterAndCheck(root, "qqq", NO_EXCLUDE, root);
    }

    public void testEmptyObjectExclude() {
        Map<String, Object> root = new HashMap<>();

        filterAndCheck(root, NO_INCLUDE, "qqq", root);
    }

    public void testFieldIncludeExact() {
        filterAndCheck(map("abc", 1, "xyz", 2),
            "abc",
            NO_EXCLUDE,
            map("abc", 1));
    }

    public void testFieldIncludeExactIgnoresPartialPrefix() {
        filterAndCheck(map("abc", 1, "xyz", 2),
            "a",
            NO_EXCLUDE,
            EMPTY);
    }

    public void testFieldIncludeExactUnmatched() {
        filterAndCheck(map("abc", 1, "xyz", 2),
            "qqq",
            NO_EXCLUDE,
            EMPTY);
    }

    public void testFieldIncludePrefixWildcard() {
        filterAndCheck(map("abc", 1, "xyz", 2),
            "a*",
            NO_EXCLUDE,
            map("abc", 1));
    }

    public void testFieldIncludeSuffixWildcard() {
        filterAndCheck(map("abc", 1, "xyz", 2),
            "*c",
            NO_EXCLUDE,
            map("abc", 1));
    }

    public void testFieldIncludeWildcardUnmatched() {
        filterAndCheck(map("abc", 1, "xyz", 2),
            "zz*",
            NO_EXCLUDE,
            EMPTY);
    }

    public void testFieldExcludeExact() {
        filterAndCheck(map("abc", 1, "xyz", 2),
            NO_INCLUDE,
            "xyz",
            map("abc", 1));
    }

    public void testFieldExcludeExactIgnoresPartialPrefix() {
        filterAndCheck(map("abc", 1, "xyz", 2),
            NO_INCLUDE,
            "a");
    }

    public void testFieldExcludeExactUmmatched() {
        filterAndCheck(map("abc", 1, "xyz", 2),
            NO_INCLUDE,
            "qqq");
    }

    public void testFieldExcludeSuffixWildcard() {
        filterAndCheck(map("abc", 1, "xyz", 2),
            NO_INCLUDE,
            "*z",
            map("abc", 1));
    }

    public void testFieldExcludePrefixWildcard() {
        filterAndCheck(map("abc", 1, "xyz", 2),
            NO_INCLUDE,
            "x*",
            map("abc", 1));
    }

    public void testFieldExcludeWildcardUnmatched() {
        filterAndCheck(map("abc", 1, "xyz", 2),
            NO_INCLUDE,
            "*zzz");
    }

    public void testIncludeNestedExact() {
        filterAndCheck( map("root", map("leaf", "value")),
            "root.leaf",
            NO_EXCLUDE);
    }

    public void testIncludeNestedExact2() {
        filterAndCheck(map("root", map("leaf", "value"), "lost", 123),
            "root.leaf",
            NO_EXCLUDE,
            map("root", map("leaf", "value")));
    }

    public void testIncludeNestedExactWildcard() {
        filterAndCheck(map("root", map("leaf", "value"), "lost", 123),
            "root.*",
            NO_EXCLUDE,
            map("root", map("leaf", "value")));
    }

    public void testNestedExcludeExact() {
        filterAndCheck(map("root", map("leaf", "value")),
            NO_INCLUDE,
            "root.leaf",
            EMPTY);
    }

    public void testNestedExcludeExact2() {
        filterAndCheck(map("root", map("leaf", "value", "kept", 123)),
            NO_INCLUDE,
            "root.leaf",
            map("root", map("kept", 123)));
    }

    public void testNestedExcludeWildcard() {
       filterAndCheck(map("root", map("leaf", "value")),
            NO_INCLUDE,
            "root.*",
            EMPTY);
    }

    public void testNestedExcludeWildcard2() {
        filterAndCheck(map("root", map("leaf", "value"), "kept", 123),
            NO_INCLUDE,
            "root.*",
            map("kept", 123));
    }

    public void testArrayIncludeExact() {
        filterAndCheck(map("root", list("leaf")), "root", NO_EXCLUDE);
    }

    public void testArrayIncludeExact2() {
        filterAndCheck(map("root", list("leaf"), "lost", 123),
            "root",
            NO_EXCLUDE,
            map("root", list("leaf")));
    }

    public void testArrayNestedIncludeExact() {
        filterAndCheck(map("root", list(map("leaf", "value"))),
            "root.leaf",
            NO_EXCLUDE);
    }

    public void testArrayNestedIncludeExact2() {
        filterAndCheck(map("root", list(map("leaf", "value")), "lost", 123),
            "root.leaf",
            NO_EXCLUDE,
            map("root", list(map("leaf", "value"))));
    }

    public void testArrayIncludeExactWildcard() {
        filterAndCheck(map("root", list("leaf", "2leaf2"),
            "lost", 123),
            "root*",
            NO_EXCLUDE,
            map("root", list("leaf", "2leaf2")));
    }

    public void testArrayExcludeExact() {
        filterAndCheck(map("root", list("leaf")),
            NO_INCLUDE,
            "root",
            EMPTY);
    }

    public void testArrayExcludeExact2() {
        filterAndCheck(map("root", list("leaf"),
            "lost", 123),
            NO_INCLUDE,
            "root",
            map("lost", 123));
    }

    public void testArrayExcludeExact3() {
        filterAndCheck(map("root", list("leaf", "leaf2"),
            "lost", 123),
            NO_INCLUDE,
            "root",
            map("lost", 123));
    }

    public void testArrayExcludeWildcard() {
        filterAndCheck(map("root", list("leaf")),
            NO_INCLUDE,
            "root.*");
    }

    public void testArrayExcludeWildcard2() {
        filterAndCheck(map("root", list("leaf"), "lost", 123),
            NO_INCLUDE,
            "ro*",
            map("lost", 123));
    }

    public void testArrayExcludeWildcard3() {
        filterAndCheck(map("root", list("leaf", "2leaf2"),
            "lost", 123),
            NO_INCLUDE,
            "root.*");
    }

    @Override
    protected void testFilter(Builder expected, Builder actual, Set<String> includes, Set<String> excludes) throws IOException {
        final XContentType xContentType = randomFrom(XContentType.values());

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
        filterAndCheck(toMap(actual, xContentType, true),
            sourceIncludes,
            sourceExcludes,
            toMap(expected, xContentType, true));
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

        List<?> extListValue = (List) extValue;
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

        extListValue = (List) extValue;
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

    public void testPrefixedNames() {
        Map<String, Object> map = new HashMap<>();
        map.put("obj", "value");
        map.put("obj_name", "value_name");
        filterAndCheck(map, "obj_name", NO_EXCLUDE, map("obj_name", "value_name"));
    }

    public void testIncludeNested1() {
        Map<String, Object> map = new HashMap<>();
        map.put("field", "value");
        map.put("array",
            Arrays.asList(
                1,
                new HashMap<String, Object>() {{
                    put("nested", 2);
                    put("nested_2", 3);
                }}));
        filterAndCheck(map,
            "array.nested",
            NO_EXCLUDE,
            map("array", list(map("nested", 2))));
    }

    public void testIncludeNested2() {
        Map<String, Object> map = new HashMap<>();
        map.put("field", "value");
        map.put("array",
            Arrays.asList(
                1,
                new HashMap<String, Object>() {{
                    put("nested", 2);
                    put("nested_2", 3);
                }}));
        filterAndCheck(map,
            "array.*",
            NO_EXCLUDE,
            map("array", list(map("nested", 2, "nested_2", 3))));
    }

    public void testIncludeNested3() {
        Map<String, Object> map = new HashMap<>();
        map.put("field", "value");
        map.put("obj", map("field", "value", "field2", "value2"));

        filterAndCheck(map,
            "obj.field",
            NO_EXCLUDE,
            map("obj", map("field", "value")));
    }

    public void testIncludeNested4() {
        filterAndCheck(map("field", "value",
            "obj", map("field", "value", "field2", "value2")),
            "obj.*",
            NO_EXCLUDE,
            map("obj", map("field", "value", "field2", "value2")));
    }

    public void testIncludeAndExclude() {
        Map<String, Object> map = new HashMap<>();
        map.put("field", "value");
        map.put("obj", map("field", "value", "field2", "value2"));
        map.put("array",
            list(1,
                map("field", "value", "field2", "value2")));

        filterAndCheck(map,
            "obj",
            NO_EXCLUDE,
            map("obj", map("field", "value", "field2", "value2")));
    }

     public void testIncludeAndExclude2() {
         Map<String, Object> map = new HashMap<>();
         map.put("field", "value");
         map.put("obj", map("field", "value", "field2", "value2"));
         map.put("array",
             list(1,
                 map("field", "value", "field2", "value2")));

         filterAndCheck(map,
             "obj",
             "*.field2",
             map("obj", map("field", "value")));
     }

    public void testIncludeAndExclude3() {
        Map<String, Object> map = new HashMap<>();
        map.put("field", "value");
        map.put("obj", map("field", "value", "field2", "value2"));
        map.put("array",
            list(1,
                map("field", "value", "field2", "value2")));

        filterAndCheck(map,
            "array",
            NO_EXCLUDE,
            map("array",
                list(1,
                    map("field", "value", "field2", "value2"))));
    }

    public void testIncludeAndExclude4() {
        Map<String, Object> map = new HashMap<>();
        map.put("field", "value");
        map.put("obj", map("field", "value", "field2", "value2"));
        map.put("array",
            list(1,
                map("field", "value", "field2", "value2")));

        filterAndCheck(map,
            "array",
            "*.field2",
            map("array", list(1, map("field", "value"))));
    }

    @SuppressWarnings("unchecked")
    public void testFilterIncludesUsingStarPrefix() {
        Map<String, Object> map = new HashMap<>();
        map.put("field", "value");
        map.put("obj", map("field", "value", "field2", "value2"));
        map.put("n_obj", map("n_field", "value", "n_field2", "value2"));

        filterAndCheck(map, "*.field2", NO_EXCLUDE, map("obj", map("field2", "value2")));
    }

    public void testFilterIncludesUsingStarPrefix2() {
        Map<String, Object> map = new HashMap<>();
        map.put("field", "value");
        map.put("obj", map("field", "value", "field2", "value2"));
        map.put("n_obj", map("n_field", "value", "n_field2", "value2"));

        Map<String, Object> expected = new HashMap<>();
        expected.put("obj", map("field", "value", "field2", "value2"));
        expected.put("n_obj", map("n_field", "value", "n_field2", "value2"));

        filterAndCheck(map, "*.*", NO_EXCLUDE, expected);
    }

    public void testFilterIncludesUsingStarPrefix3() {
        Map<String, Object> map = new HashMap<>();
        map.put("field", "value");
        map.put("obj", map("field", "value", "field2", "value2"));
        map.put("n_obj", map("n_field", "value", "n_field2", "value2"));

        Map<String, Object> expected = new HashMap<>();
        expected.put("field", "value");
        expected.put("obj", map("field", "value"));
        expected.put("n_obj", map("n_field", "value"));

        filterAndCheck(map, "*", "*.*2", expected);
    }

    public void testFilterWithEmptyIncludesEmptyExcludes() {
        Map<String, Object> map = new HashMap<>();
        map.put("field", "value");
        filterAndCheck(map, NO_INCLUDE, NO_EXCLUDE, map);
    }

    public void testFilterWithEmptyIncludesEmptyExcludes_EmptyString() {
        Map<String, Object> map = new HashMap<>();
        map.put("field", "");
        filterAndCheck(map, NO_INCLUDE, NO_EXCLUDE, map);
    }

    public void testThatFilterIncludesEmptyObjectWhenUsingIncludes() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                .startObject("obj")
                .endObject()
                .endObject();

        filterAndCheck(builder, "obj", NO_EXCLUDE);
    }

    public void testThatFilterIncludesEmptyObjectWhenUsingExcludes() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                .startObject("obj")
                .endObject()
                .endObject();
        filterAndCheck(builder, NO_INCLUDE, "nonExistingField");
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
        filterAndCheck(builder, NO_INCLUDE, "*.obj2", EMPTY);
    }

    @SuppressWarnings({"unchecked"})
    public void testIncludingObjectWithNestedIncludedObject() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                .startObject("obj1")
                .startObject("obj2")
                .endObject()
                .endObject()
                .endObject();

        filterAndCheck(builder, "*.obj2", NO_EXCLUDE);
    }

    public void testDotsInFieldNames() {
        Map<String, Object> map = new HashMap<>();
        map.put("foo.bar", 2);
        Map<String, Object> sub = new HashMap<>();
        sub.put("baz", 3);
        map.put("foo", sub);
        map.put("quux", 5);

        // dots in field names in includes
        Map<String, Object> expected = new HashMap<>(map);
        expected.remove("quux");
        filterAndCheck(map, "foo", NO_EXCLUDE, expected);

        // dots in field names in excludes
        expected = new HashMap<>(map);
        expected.keySet().retainAll(Collections.singleton("quux"));
        filterAndCheck(map, NO_INCLUDE, "foo", expected);
    }

    public void testSupplementaryCharactersInPaths() {
        Map<String, Object> map = new HashMap<>();
        map.put("搜索", 2);
        map.put("指数", 3);

        filterAndCheck(map, "搜索", NO_EXCLUDE, map("搜索", 2));
    }

    public void testSupplementaryCharactersInPaths2() {
        Map<String, Object> map = new HashMap<>();
        map.put("搜索", 2);
        map.put("指数", 3);

        filterAndCheck(map, NO_INCLUDE, "搜索", map("指数", 3));
    }

    public void testSharedPrefixes() {
        Map<String, Object> map = new HashMap<>();
        map.put("foobar", 2);
        map.put("foobaz", 3);

        filterAndCheck(map, "foobar", NO_EXCLUDE, map("foobar", 2));
    }

    public void testSharedPrefixes2() {
        filterAndCheck(map("foobar", 2, "foobaz", 3),
            NO_INCLUDE,
            "foobar",
            map("foobaz", 3));
    }

    public void testPrefix() {
        Map<String, Object> map = new HashMap<>();
        map.put("photos", Arrays.asList(new String[] {"foo", "bar"}));
        map.put("photosCount", 2);

        filterAndCheck(map, "photosCount", NO_EXCLUDE, map("photosCount", 2));
    }

    public void testEmptyArray() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
            .array("arrayField")
            .endObject();

        filterAndCheck(builder, NO_INCLUDE, NO_EXCLUDE);
    }

    public void testEmptyArray_UnmatchedExclude() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
            .array("myField")
            .endObject();

        filterAndCheck(builder, NO_INCLUDE, "bar");
    }

    public void testEmptyArray_UnmatchedExcludeWildcard() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
            .array("myField")
            .endObject();

        filterAndCheck(builder, NO_INCLUDE, "*bar");
    }

    public void testOneElementArray() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
            .array("arrayField", "value1")
            .endObject();

        filterAndCheck(builder, NO_INCLUDE, NO_EXCLUDE);
    }

    public void testOneElementArray_UnmatchedExclude() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
            .array("arrayField", "value1")
            .endObject();

        filterAndCheck(builder, NO_INCLUDE, "different");
    }

    public void testOneElementArray_UnmatchedExcludeWildcard() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
            .array("arrayField", "")
            .endObject();

        filterAndCheck(builder, NO_INCLUDE, "different*");
    }

    private static Map<String, Object> toMap(Builder test, XContentType xContentType, boolean humanReadable) throws IOException {
        ToXContentObject toXContent = (builder, params) -> test.apply(builder);
        return convertToMap(toXContent(toXContent, xContentType, humanReadable), true, xContentType).v2();
    }

    private Map<String, Object> toMap(final XContentBuilder builder) {
        return convertToMap(BytesReference.bytes(builder), true, builder.contentType()).v2();
    }

    private Map<String, Object> filter(final Map<String, Object> input, final String include, final String exclude) {
        return filter(input, array(include), array(exclude));
    }

    private Map<String, Object> filter(final XContentBuilder input, final String include, final String exclude) {
        return filter(input, array(include), array(exclude));
    }

    private String[] array(final String...elements) {
        return elements.length == 1 && elements[0] == null ? Strings.EMPTY_ARRAY : elements;
    }

    private Map<String, Object> filter(final Map<String, Object> input, final String[] includes, final String[] excludes) {
        return XContentMapValues.filter(input, includes, excludes);
    }

    private void filterAndCheck(final Map<String, Object> input, final String include, final String exclude) {
        filterAndCheck(input, array(include), array(exclude), Collections.unmodifiableMap(input));
    }

    private void filterAndCheck(final Map<String, Object> input,
                                final String include,
                                final String exclude,
                                final Map<String, Object> expected) {
        filterAndCheck(input, array(include), array(exclude), expected);
    }

    private void filterAndCheck(final Map<String, Object> input,
                                final String[] includes,
                                final String[] excludes,
                                final Map<String, Object> expected) {
        Map<String, Object> filteredSource = filter(input, includes, excludes);

        final StringBuilder b = new StringBuilder();
        if(null!=includes && includes.length > 0) {
            b.append("includes=")
                .append(Arrays.stream(includes).collect(Collectors.joining(", ")))
                .append(' ');
        }
        if(null!=excludes && excludes.length > 0) {
            b.append(" excludes=")
                .append(Arrays.stream(excludes).collect(Collectors.joining(", ")))
                .append(' ');
        }
        b.append("input: ").append(input);

        Assert.assertEquals(b.toString(), expected, filteredSource);
    }

    private Map<String, Object> filter(final XContentBuilder input, final String[] includes, final String[] excludes) {
        return filter(toMap(input), includes, excludes);
    }

    private void filterAndCheck(final XContentBuilder input, final String include, final String exclude) {
        filterAndCheck(toMap(input), include, exclude);
    }

    private void filterAndCheck(final XContentBuilder input,
                                final String include,
                                final String exclude,
                                final Map<String, Object> expected) {
        filterAndCheck(toMap(input), include, exclude, expected);
    }

    private <T> List<T> list(final T...elements) {
        return new ArrayList<>(Arrays.asList(elements));
    }

    private <K, V> Map<K, V> map(final K key, final V value) {
        return Collections.singletonMap(key, value);
    }

    private <K, V> Map<K, V> map(final K key1, final V value1, final K key2, final V value2) {
        final Map<K, V> map = new LinkedHashMap<>();
        map.put(key1, value1);
        map.put(key2, value2);
        return map;
    }
}
