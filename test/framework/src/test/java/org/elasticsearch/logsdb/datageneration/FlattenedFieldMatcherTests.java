/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.logsdb.datageneration;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.datageneration.matchers.source.FlattenedFieldMatcher;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class FlattenedFieldMatcherTests extends ESTestCase {
    public void testSingleMatch() throws IOException {
        List<Object> values = List.of(Map.of("foo", "bar"));

        Map<String, Object> mapping = Map.of("type", "flattened");

        var mappingXContent = XContentFactory.jsonBuilder().map(mapping);

        var matcher = new FlattenedFieldMatcher(mappingXContent, Settings.builder(), mappingXContent, Settings.builder());

        assertTrue(matcher.match(values, values, mapping, mapping).isMatch());
    }

    public void testSingleMismatch() throws IOException {
        List<Object> actual = List.of(Map.of("foo", "baz"));
        List<Object> expected = List.of(Map.of("foo", "bar"));

        Map<String, Object> mapping = Map.of("type", "flattened");

        var mappingXContent = XContentFactory.jsonBuilder().map(mapping);

        var matcher = new FlattenedFieldMatcher(mappingXContent, Settings.builder(), mappingXContent, Settings.builder());

        assertFalse(matcher.match(actual, expected, mapping, mapping).isMatch());
    }

    public void testArrayMatchLossy() throws IOException {
        List<Object> expected = List.of(Map.of("foo", nullableList("a", "a", null, "c", null, "a", "d", "b")));
        List<Object> actual = List.of(Map.of("foo", List.of("a", "b", "c", "d")));

        Map<String, Object> mapping = Map.of("type", "flattened", "preserve_leaf_arrays", "lossy");
        var mappingXContent = XContentFactory.jsonBuilder().map(mapping);

        var matcher = new FlattenedFieldMatcher(mappingXContent, Settings.builder(), mappingXContent, Settings.builder());

        assertTrue(matcher.match(actual, expected, mapping, mapping).isMatch());

    }

    public void testArrayMismatchLossy() throws IOException {
        List<Object> expected = List.of(Map.of("foo", nullableList("a", "a", null, "c", null, "a", "d", "b")));
        List<Object> actual = List.of(Map.of("foo", List.of("a", "b", "c")));

        Map<String, Object> mapping = Map.of("type", "flattened", "preserve_leaf_arrays", "lossy");
        var mappingXContent = XContentFactory.jsonBuilder().map(mapping);

        var matcher = new FlattenedFieldMatcher(mappingXContent, Settings.builder(), mappingXContent, Settings.builder());

        assertFalse(matcher.match(actual, expected, mapping, mapping).isMatch());
    }

    public void testArrayMatchExact() throws IOException {
        List<Object> values = List.of(Map.of("foo", nullableList("a", "a", null, "c", null, "a", "d", "b")));

        Map<String, Object> mapping = Map.of("type", "flattened", "preserve_leaf_arrays", "exact");
        var mappingXContent = XContentFactory.jsonBuilder().map(mapping);

        var matcher = new FlattenedFieldMatcher(mappingXContent, Settings.builder(), mappingXContent, Settings.builder());

        assertTrue(matcher.match(values, values, mapping, mapping).isMatch());
    }

    public void testArrayMismatchExact() throws IOException {
        List<Object> expected = List.of(Map.of("foo", nullableList("a", "a", null, "c", null, "a", "d", "b")));
        List<Object> actual = List.of(Map.of("foo", List.of("a", "b", "c", "d")));

        Map<String, Object> mapping = Map.of("type", "flattened", "preserve_leaf_arrays", "exact");
        var mappingXContent = XContentFactory.jsonBuilder().map(mapping);

        var matcher = new FlattenedFieldMatcher(mappingXContent, Settings.builder(), mappingXContent, Settings.builder());

        assertFalse(matcher.match(actual, expected, mapping, mapping).isMatch());
    }

    private static List<Object> nullableList(Object... values) {
        return Arrays.stream(values).toList();
    }
}
