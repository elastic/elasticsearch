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
import org.elasticsearch.datageneration.matchers.source.SourceMatcher;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class SourceMatcherTests extends ESTestCase {
    public void testDynamicMatch() throws IOException {
        List<Map<String, Object>> values = List.of(
            Map.of("aaa", 124, "bbb", false, "ccc", 12.34),
            Map.of("aaa", 124, "bbb", false, "ccc", 12.34)
        );

        var sut = new SourceMatcher(
            Map.of(),
            XContentBuilder.builder(XContentType.JSON.xContent()).startObject().endObject(),
            Settings.builder(),
            XContentBuilder.builder(XContentType.JSON.xContent()).startObject().endObject(),
            Settings.builder(),
            values,
            values,
            false
        );
        assertTrue(sut.match().isMatch());
    }

    public void testDynamicMismatch() throws IOException {
        List<Map<String, Object>> actual = List.of(
            Map.of("aaa", 124, "bbb", false, "ccc", 12.34),
            Map.of("aaa", 124, "bbb", false, "ccc", 12.34)
        );
        List<Map<String, Object>> expected = List.of(
            Map.of("aaa", 124, "bbb", false, "ccc", 12.34),
            Map.of("aaa", 125, "bbb", false, "ccc", 12.34)
        );

        var sut = new SourceMatcher(
            Map.of(),
            XContentBuilder.builder(XContentType.JSON.xContent()).startObject().endObject(),
            Settings.builder(),
            XContentBuilder.builder(XContentType.JSON.xContent()).startObject().endObject(),
            Settings.builder(),
            actual,
            expected,
            false
        );
        assertFalse(sut.match().isMatch());
    }

    public void testMappedMatch() throws IOException {
        List<Map<String, Object>> values = List.of(
            Map.of("aaa", 124, "bbb", "hey", "ccc", 12.34),
            Map.of("aaa", 124, "bbb", "yeh", "ccc", 12.34)
        );

        var mapping = XContentBuilder.builder(XContentType.JSON.xContent());
        mapping.startObject();
        mapping.startObject("_doc");
        {
            mapping.startObject("aaa").field("type", "long").endObject();
            mapping.startObject("bbb").field("type", "keyword").endObject();
            mapping.startObject("ccc").field("type", "half_float").endObject();
        }
        mapping.endObject();
        mapping.endObject();

        var sut = new SourceMatcher(Map.of(), mapping, Settings.builder(), mapping, Settings.builder(), values, values, false);
        assertTrue(sut.match().isMatch());
    }

    public void testMappedMismatch() throws IOException {
        List<Map<String, Object>> actual = List.of(
            Map.of("aaa", 124, "bbb", "hey", "ccc", 12.34),
            Map.of("aaa", 124, "bbb", "yeh", "ccc", 12.34)
        );
        List<Map<String, Object>> expected = List.of(
            Map.of("aaa", 124, "bbb", "hey", "ccc", 12.34),
            Map.of("aaa", 124, "bbb", "yeh", "ccc", 12.35)
        );

        var mapping = XContentBuilder.builder(XContentType.JSON.xContent());
        mapping.startObject();
        mapping.startObject("_doc");
        {
            mapping.startObject("aaa").field("type", "long").endObject();
            mapping.startObject("bbb").field("type", "keyword").endObject();
            mapping.startObject("ccc").field("type", "half_float").endObject();
        }
        mapping.endObject();
        mapping.endObject();

        var sut = new SourceMatcher(Map.of(), mapping, Settings.builder(), mapping, Settings.builder(), actual, expected, false);
        assertFalse(sut.match().isMatch());
    }

    public void testCountedKeywordMatch() throws IOException {
        List<Map<String, Object>> actual = List.of(Map.of("field", List.of("a", "b", "a", "c", "b", "a")));
        List<Map<String, Object>> expected = List.of(Map.of("field", List.of("a", "b", "a", "c", "b", "a")));

        var mapping = XContentBuilder.builder(XContentType.JSON.xContent());
        mapping.startObject();
        mapping.startObject("_doc");
        {
            mapping.startObject("field").field("type", "counted_keyword").endObject();
        }
        mapping.endObject();
        mapping.endObject();

        var sut = new SourceMatcher(Map.of(), mapping, Settings.builder(), mapping, Settings.builder(), actual, expected, false);
        assertTrue(sut.match().isMatch());
    }

    public void testCountedKeywordMismatch() throws IOException {
        List<Map<String, Object>> actual = List.of(Map.of("field", List.of("a", "b", "a", "c", "b", "a")));
        List<Map<String, Object>> expected = List.of(Map.of("field", List.of("a", "b", "c", "a")));

        var mapping = XContentBuilder.builder(XContentType.JSON.xContent());
        mapping.startObject();
        mapping.startObject("_doc");
        {
            mapping.startObject("field").field("type", "counted_keyword").endObject();
        }
        mapping.endObject();
        mapping.endObject();

        var sut = new SourceMatcher(Map.of(), mapping, Settings.builder(), mapping, Settings.builder(), actual, expected, false);
        assertFalse(sut.match().isMatch());
    }
}
