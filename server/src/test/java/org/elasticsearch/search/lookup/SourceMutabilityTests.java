/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.lookup;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Tests documenting the current mutability characteristics of Map objects returned by Source.fromBytes()
 * and Source.fromMap()
 *
 */
public class SourceMutabilityTests extends ESTestCase {

    public void testFromEmpty() {
        Source s1 = Source.fromMap(null, XContentType.JSON);
        assertTrue("Should be empty", s1.source().isEmpty());
        assertMapIsImmutable(s1.source());
        Source s2 = s1.withMutations(map -> map.put("key", "value"));
        assertEquals("value", s2.source().get("key"));

        Source s3 = s2.withMutations(map -> map.remove("key"));
        assertNull(s3.source().get("key"));
        assertTrue(s3.source().isEmpty());
        // note that the returned map does not have to be mutable
        assertMapIsImmutable(s3.source());
    }

    public void testWithMutationsFromImmutableMap() {
        Source original = Source.fromMap(Map.of("field1", "value1"), XContentType.JSON);
        assertMapIsImmutable(original.source());

        Source modified = original.withMutations(map -> map.put("field2", "value2"));

        assertEquals(1, original.source().size());
        assertEquals("value1", original.source().get("field1"));
        assertNull(original.source().get("field2"));

        assertEquals(2, modified.source().size());
        assertEquals("value1", modified.source().get("field1"));
        assertEquals("value2", modified.source().get("field2"));
    }

    public void testWithMutationsFromMutableMap() {
        Map<String, Object> mutableMap = new LinkedHashMap<>();
        mutableMap.put("field1", "value1");
        Source original = Source.fromMap(mutableMap, XContentType.JSON);
        assertMapIsMutable("fromMap(LinkedHashMap)", original.source());

        Source modified = original.withMutations(map -> map.put("field2", "value2"));

        assertEquals(1, original.source().size());
        assertEquals("value1", original.source().get("field1"));

        assertEquals(2, modified.source().size());
        assertEquals("value1", modified.source().get("field1"));
        assertEquals("value2", modified.source().get("field2"));
    }

    public void testWithMutationsFromBytes() {
        Source original = Source.fromBytes(new BytesArray("{\"field1\":\"value1\"}"));
        assertMapIsMutable("fromBytes", original.source());

        Source modified = original.withMutations(map -> map.put("field2", "value2"));

        assertEquals(1, original.source().size());
        assertEquals("value1", original.source().get("field1"));

        assertEquals(2, modified.source().size());
        assertEquals("value1", modified.source().get("field1"));
        assertEquals("value2", modified.source().get("field2"));
    }

    public void testWithMutationsRemoveField() {
        Source original = Source.fromMap(Map.of("field1", "value1", "field2", "value2"), XContentType.JSON);

        Source modified = original.withMutations(map -> map.remove("field1"));

        // Original should be unchanged
        assertEquals(2, original.source().size());
        assertEquals("value1", original.source().get("field1"));

        // Modified should only have field2
        assertEquals(1, modified.source().size());
        assertNull(modified.source().get("field1"));
        assertEquals("value2", modified.source().get("field2"));
    }

    public void testWithMutationsUpdateField() {
        Source original = Source.fromMap(Map.of("field1", "value1"), XContentType.JSON);

        Source modified = original.withMutations(map -> map.put("field1", "updated"));

        // Original should be unchanged
        assertEquals("value1", original.source().get("field1"));

        // Modified should have updated value
        assertEquals("updated", modified.source().get("field1"));
    }

    public void testWithMutationsChaining() {
        Source original = Source.fromMap(Map.of("field1", "value1"), XContentType.JSON);

        Source modified = original.withMutations(map -> map.put("field2", "value2"))
            .withMutations(map -> map.put("field3", "value3"))
            .withMutations(map -> map.remove("field1"));

        // Original should be unchanged
        assertEquals(1, original.source().size());
        assertEquals("value1", original.source().get("field1"));

        // Modified should reflect all mutations
        assertEquals(2, modified.source().size());
        assertNull(modified.source().get("field1"));
        assertEquals("value2", modified.source().get("field2"));
        assertEquals("value3", modified.source().get("field3"));
    }

    public void testWithMutationsPreservesContentType() {
        Source jsonSource = Source.fromMap(Map.of("field", "value"), XContentType.JSON);
        Source modifiedJson = jsonSource.withMutations(map -> map.put("new", "field"));
        assertEquals(XContentType.JSON, modifiedJson.sourceContentType());

        Source smileSource = Source.fromMap(Map.of("field", "value"), XContentType.SMILE);
        Source modifiedSmile = smileSource.withMutations(map -> map.put("new", "field"));
        assertEquals(XContentType.SMILE, modifiedSmile.sourceContentType());
    }

    public void testWithMutationsMultipleOperationsInSingleCall() {
        Source original = Source.fromMap(Map.of("a", "1", "b", "2"), XContentType.JSON);

        Source modified = original.withMutations(map -> {
            map.put("c", "3");
            map.put("d", "4");
            map.remove("a");
            map.put("b", "updated");
        });

        // Original unchanged
        assertEquals(2, original.source().size());
        assertEquals("1", original.source().get("a"));
        assertEquals("2", original.source().get("b"));

        // Modified has all changes
        assertEquals(3, modified.source().size());
        assertNull(modified.source().get("a"));
        assertEquals("updated", modified.source().get("b"));
        assertEquals("3", modified.source().get("c"));
        assertEquals("4", modified.source().get("d"));
    }

    public void testWithMutationsHandlesNullSource() {
        Source nullSource = new NullReturningSource();
        assertNull(nullSource.source());

        Source modified = nullSource.withMutations(map -> map.put("field", "value"));

        assertEquals(1, modified.source().size());
        assertEquals("value", modified.source().get("field"));
    }

    /**
     * A bad Source implementation that returns null from source().
     * This is used to test that withMutations handles null gracefully.
     */
    private static class NullReturningSource implements Source {
        @Override
        public XContentType sourceContentType() {
            return XContentType.JSON;
        }

        @Override
        public Map<String, Object> source() {
            return null;
        }

        @Override
        public BytesReference internalSourceRef() {
            return null;
        }

        @Override
        public Source filter(SourceFilter sourceFilter) {
            return this;
        }
    }

    private void assertMapIsImmutable(Map<String, Object> map) {
        expectThrows(UnsupportedOperationException.class, () -> map.put("test_key", "test_value"));
    }

    private void assertMapIsMutable(String scenario, Map<String, Object> map) {
        try {
            map.put("test_key", "test_value");
            assertEquals("test put in mutable map", "test_value", map.get("test_key"));
            map.remove("test_key");
            assertNull("test remove from mutable map", map.get("test_key"));
        } catch (UnsupportedOperationException e) {
            fail(scenario + " should return a mutable map but got: " + e.getMessage());
        }
    }
}
