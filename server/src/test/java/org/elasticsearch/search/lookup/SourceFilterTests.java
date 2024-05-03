/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.lookup;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.util.List;
import java.util.Map;

public class SourceFilterTests extends ESTestCase {

    public void testEmptyFiltering() {
        Source s = Source.fromMap(Map.of("field", "value"), XContentType.JSON);
        Source filtered = s.filter(new SourceFilter(new String[] {}, new String[] {}));
        assertSame(s, filtered);
    }

    public void testSimpleInclude() {
        Source s = Source.fromBytes(new BytesArray("""
            { "field1" : "value1", "field2" : "value2" }"""));
        Source filtered = s.filter(new SourceFilter(new String[] { "field2" }, new String[] {}));
        assertTrue(filtered.source().containsKey("field2"));
        assertEquals("value2", filtered.source().get("field2"));
        assertFalse(filtered.source().containsKey("field1"));
    }

    public void testSimpleExclude() {
        Source s = Source.fromBytes(new BytesArray("""
            { "field1" : "value1", "field2" : "value2" }"""));
        Source filtered = s.filter(new SourceFilter(new String[] {}, new String[] { "field1" }));
        assertTrue(filtered.source().containsKey("field2"));
        assertEquals("value2", filtered.source().get("field2"));
        assertFalse(filtered.source().containsKey("field1"));
    }

    public void testCombinedIncludesAndExcludes() {
        Source s = Source.fromBytes(new BytesArray("""
            {
              "requests": {
                "count": 10,
                "foo": "bar"
              },
              "meta": {
                "name": "Some metric",
                "description": "Some metric description",
                "other": {
                  "foo": "one",
                  "baz": "two"
                }
              }
            }
            """));
        SourceFilter sourceFilter = new SourceFilter(
            new String[] { "*.count", "meta.*" },
            new String[] { "meta.description", "meta.other.*" }
        );

        s = s.filter(sourceFilter);
        Map<String, Object> expected = Map.of("requests", Map.of("count", 10), "meta", Map.of("name", "Some metric", "other", Map.of()));
        assertEquals(expected, s.source());
    }

    public void testExcludeWithWildcards() {
        Source s = Source.fromBytes(new BytesArray("""
            { "field1" : "value1", "array_field" : [ "value2" ] }"""));
        Source filtered = s.filter(new SourceFilter(new String[] {}, new String[] { "array*" }));
        assertTrue(filtered.source().containsKey("field1"));
        assertEquals("value1", filtered.source().get("field1"));
        assertFalse(filtered.source().containsKey("array_field"));
    }

    public void testExcludeWithWildcardsUsesMap() {

        Source s = new Source() {
            @Override
            public XContentType sourceContentType() {
                return XContentType.JSON;
            }

            @Override
            public Map<String, Object> source() {
                return Map.of("field", "value", "array_field", List.of("value1", "value2"));
            }

            @Override
            public BytesReference internalSourceRef() {
                throw new AssertionError("SourceFilter with '*' in excludes list should filter on map");
            }

            @Override
            public Source filter(SourceFilter sourceFilter) {
                // We call filterBytes explicitly here but the filter should re-route to
                // using filterMap because it contains an exclude filter with a wildcard
                return sourceFilter.filterBytes(this);
            }
        };

        Source filtered = s.filter(new SourceFilter(new String[] {}, new String[] { "array*" }));
        assertTrue(filtered.source().containsKey("field"));
        assertEquals("value", filtered.source().get("field"));
        assertFalse(filtered.source().containsKey("array_field"));

    }

}
