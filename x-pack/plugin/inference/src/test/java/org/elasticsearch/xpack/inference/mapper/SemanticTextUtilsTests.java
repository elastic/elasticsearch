/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class SemanticTextUtilsTests extends ESTestCase {
    public void testInsertValueMapTraversal() throws IOException {
        {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field("test", "value").endObject();

            Map<String, Object> map = toSourceMap(Strings.toString(builder));
            SemanticTextUtils.insertValue("test", map, "value2");
            assertThat(getMapValue(map, "test"), equalTo("value2"));
            SemanticTextUtils.insertValue("something.else", map, "something_else_value");
            assertThat(getMapValue(map, "something\\.else"), equalTo("something_else_value"));
        }
        {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
            builder.startObject("path1").startObject("path2").field("test", "value").endObject().endObject();
            builder.endObject();

            Map<String, Object> map = toSourceMap(Strings.toString(builder));
            SemanticTextUtils.insertValue("path1.path2.test", map, "value2");
            assertThat(getMapValue(map, "path1.path2.test"), equalTo("value2"));
            SemanticTextUtils.insertValue("path1.path2.test_me", map, "test_me_value");
            assertThat(getMapValue(map, "path1.path2.test_me"), equalTo("test_me_value"));
            SemanticTextUtils.insertValue("path1.non_path2.test", map, "test_value");
            assertThat(getMapValue(map, "path1.non_path2\\.test"), equalTo("test_value"));

            SemanticTextUtils.insertValue("path1.path2", map, Map.of("path3", "bar"));
            assertThat(getMapValue(map, "path1.path2"), equalTo(Map.of("path3", "bar")));

            SemanticTextUtils.insertValue("path1", map, "baz");
            assertThat(getMapValue(map, "path1"), equalTo("baz"));

            SemanticTextUtils.insertValue("path3.path4", map, Map.of("test", "foo"));
            assertThat(getMapValue(map, "path3\\.path4"), equalTo(Map.of("test", "foo")));
        }
        {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
            builder.startObject("path1").array("test", "value1", "value2").endObject();
            builder.endObject();
            Map<String, Object> map = toSourceMap(Strings.toString(builder));

            SemanticTextUtils.insertValue("path1.test", map, List.of("value3", "value4", "value5"));
            assertThat(getMapValue(map, "path1.test"), equalTo(List.of("value3", "value4", "value5")));

            SemanticTextUtils.insertValue("path2.test", map, List.of("value6", "value7", "value8"));
            assertThat(getMapValue(map, "path2\\.test"), equalTo(List.of("value6", "value7", "value8")));
        }
    }

    public void testInsertValueListTraversal() throws IOException {
        {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
            {
                builder.startObject("path1");
                {
                    builder.startArray("path2");
                    builder.startObject().field("test", "value1").endObject();
                    builder.endArray();
                }
                builder.endObject();
            }
            {
                builder.startObject("path3");
                {
                    builder.startArray("path4");
                    builder.startObject().field("test", "value1").endObject();
                    builder.endArray();
                }
                builder.endObject();
            }
            builder.endObject();
            Map<String, Object> map = toSourceMap(Strings.toString(builder));

            SemanticTextUtils.insertValue("path1.path2.test", map, "value2");
            assertThat(getMapValue(map, "path1.path2.test"), equalTo("value2"));
            SemanticTextUtils.insertValue("path1.path2.test2", map, "value3");
            assertThat(getMapValue(map, "path1.path2.test2"), equalTo("value3"));
            assertThat(getMapValue(map, "path1.path2"), equalTo(List.of(Map.of("test", "value2", "test2", "value3"))));

            SemanticTextUtils.insertValue("path3.path4.test", map, "value4");
            assertThat(getMapValue(map, "path3.path4.test"), equalTo("value4"));
        }
        {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
            {
                builder.startObject("path1");
                {
                    builder.startArray("path2");
                    builder.startArray();
                    builder.startObject().field("test", "value1").endObject();
                    builder.endArray();
                    builder.endArray();
                }
                builder.endObject();
            }
            builder.endObject();
            Map<String, Object> map = toSourceMap(Strings.toString(builder));

            SemanticTextUtils.insertValue("path1.path2.test", map, "value2");
            assertThat(getMapValue(map, "path1.path2.test"), equalTo("value2"));
            SemanticTextUtils.insertValue("path1.path2.test2", map, "value3");
            assertThat(getMapValue(map, "path1.path2.test2"), equalTo("value3"));
            assertThat(getMapValue(map, "path1.path2"), equalTo(List.of(List.of(Map.of("test", "value2", "test2", "value3")))));
        }
    }

    public void testInsertValueFieldsWithDots() throws IOException {
        {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field("xxx.yyy", "value1").endObject();
            Map<String, Object> map = toSourceMap(Strings.toString(builder));

            SemanticTextUtils.insertValue("xxx.yyy", map, "value2");
            assertThat(getMapValue(map, "xxx\\.yyy"), equalTo("value2"));

            SemanticTextUtils.insertValue("xxx", map, "value3");
            assertThat(getMapValue(map, "xxx"), equalTo("value3"));
        }
        {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
            {
                builder.startObject("path1.path2");
                {
                    builder.startObject("path3.path4");
                    builder.field("test", "value1");
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            Map<String, Object> map = toSourceMap(Strings.toString(builder));

            SemanticTextUtils.insertValue("path1.path2.path3.path4.test", map, "value2");
            assertThat(getMapValue(map, "path1\\.path2.path3\\.path4.test"), equalTo("value2"));

            SemanticTextUtils.insertValue("path1.path2.path3.path4.test2", map, "value3");
            assertThat(getMapValue(map, "path1\\.path2.path3\\.path4.test2"), equalTo("value3"));
            assertThat(getMapValue(map, "path1\\.path2.path3\\.path4"), equalTo(Map.of("test", "value2", "test2", "value3")));
        }
    }

    public void testInsertValueAmbiguousPath() throws IOException {
        // Mixed dotted object notation
        {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
            {
                builder.startObject("path1.path2");
                {
                    builder.startObject("path3");
                    builder.field("test1", "value1");
                    builder.endObject();
                }
                builder.endObject();
            }
            {
                builder.startObject("path1");
                {
                    builder.startObject("path2.path3");
                    builder.field("test2", "value2");
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            Map<String, Object> map = toSourceMap(Strings.toString(builder));
            final Map<String, Object> originalMap = Collections.unmodifiableMap(toSourceMap(Strings.toString(builder)));

            IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> SemanticTextUtils.insertValue("path1.path2.path3.test1", map, "value3")
            );
            assertThat(
                ex.getMessage(),
                equalTo("Path [path1.path2.path3.test1] could be inserted in 2 distinct ways, it is ambiguous which one to use")
            );

            ex = assertThrows(
                IllegalArgumentException.class,
                () -> SemanticTextUtils.insertValue("path1.path2.path3.test3", map, "value4")
            );
            assertThat(
                ex.getMessage(),
                equalTo("Path [path1.path2.path3.test3] could be inserted in 2 distinct ways, it is ambiguous which one to use")
            );

            assertThat(map, equalTo(originalMap));
        }

        // traversal through lists
        {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
            {
                builder.startObject("path1.path2");
                {
                    builder.startArray("path3");
                    builder.startObject().field("test1", "value1").endObject();
                    builder.endArray();
                }
                builder.endObject();
            }
            {
                builder.startObject("path1");
                {
                    builder.startArray("path2.path3");
                    builder.startObject().field("test2", "value2").endObject();
                    builder.endArray();
                }
                builder.endObject();
            }
            builder.endObject();
            Map<String, Object> map = toSourceMap(Strings.toString(builder));
            final Map<String, Object> originalMap = Collections.unmodifiableMap(toSourceMap(Strings.toString(builder)));

            IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> SemanticTextUtils.insertValue("path1.path2.path3.test1", map, "value3")
            );
            assertThat(
                ex.getMessage(),
                equalTo("Path [path1.path2.path3.test1] could be inserted in 2 distinct ways, it is ambiguous which one to use")
            );

            ex = assertThrows(
                IllegalArgumentException.class,
                () -> SemanticTextUtils.insertValue("path1.path2.path3.test3", map, "value4")
            );
            assertThat(
                ex.getMessage(),
                equalTo("Path [path1.path2.path3.test3] could be inserted in 2 distinct ways, it is ambiguous which one to use")
            );

            assertThat(map, equalTo(originalMap));
        }
    }

    public void testInsertValueCannotTraversePath() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        {
            builder.startObject("path1");
            {
                builder.startArray("path2");
                builder.startArray();
                builder.startObject().field("test", "value1").endObject();
                builder.endArray();
                builder.endArray();
            }
            builder.endObject();
        }
        builder.endObject();
        Map<String, Object> map = toSourceMap(Strings.toString(builder));
        final Map<String, Object> originalMap = Collections.unmodifiableMap(toSourceMap(Strings.toString(builder)));

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> SemanticTextUtils.insertValue("path1.path2.test.test2", map, "value2")
        );
        assertThat(
            ex.getMessage(),
            equalTo("Path [path1.path2.test] has value [value1] of type [String], which cannot be traversed into further")
        );

        assertThat(map, equalTo(originalMap));
    }

    private Map<String, Object> toSourceMap(String source) throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            return parser.map();
        }
    }

    private static Object getMapValue(Map<String, Object> map, String key) {
        // Split the path on unescaped "." chars and then unescape the escaped "." chars
        final String[] pathElements = Arrays.stream(key.split("(?<!\\\\)\\.")).map(k -> k.replace("\\.", ".")).toArray(String[]::new);

        Object value = null;
        Object nextLayer = map;
        for (int i = 0; i < pathElements.length; i++) {
            if (nextLayer instanceof Map<?, ?> nextMap) {
                value = nextMap.get(pathElements[i]);
            } else if (nextLayer instanceof List<?> nextList) {
                final String pathElement = pathElements[i];
                List<?> values = nextList.stream().flatMap(v -> {
                    Stream.Builder<Object> streamBuilder = Stream.builder();
                    if (v instanceof List<?> innerList) {
                        traverseList(innerList, streamBuilder);
                    } else {
                        streamBuilder.add(v);
                    }
                    return streamBuilder.build();
                }).filter(v -> v instanceof Map<?, ?>).map(v -> ((Map<?, ?>) v).get(pathElement)).filter(Objects::nonNull).toList();

                if (values.isEmpty()) {
                    return null;
                } else if (values.size() > 1) {
                    throw new AssertionError("List " + nextList + " contains multiple values for [" + pathElement + "]");
                } else {
                    value = values.getFirst();
                }
            } else if (nextLayer == null) {
                break;
            } else {
                throw new AssertionError(
                    "Path ["
                        + String.join(".", Arrays.copyOfRange(pathElements, 0, i))
                        + "] has value ["
                        + value
                        + "] of type ["
                        + value.getClass().getSimpleName()
                        + "], which cannot be traversed into further"
                );
            }

            nextLayer = value;
        }

        return value;
    }

    private static void traverseList(List<?> list, Stream.Builder<Object> streamBuilder) {
        for (Object value : list) {
            if (value instanceof List<?> innerList) {
                traverseList(innerList, streamBuilder);
            } else {
                streamBuilder.add(value);
            }
        }
    }
}
