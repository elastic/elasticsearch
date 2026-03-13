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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Tests {@link Source#withMutations(Consumer)}.
 *
 * <p><b>Important behavior note</b>: When the source map is already a {@link java.util.HashMap}
 * (including {@link LinkedHashMap}), {@code withMutations} will mutate the original map directly
 * rather than creating a copy. This is intentional for performance. Immutable maps (like those
 * from {@link Map#of}) are copied before mutation.
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

        // Immutable maps are copied, so original is unchanged
        assertEquals(1, original.source().size());
        assertEquals("value1", original.source().get("field1"));
        assertNull(original.source().get("field2"));

        assertEquals(2, modified.source().size());
        assertEquals("value1", modified.source().get("field1"));
        assertEquals("value2", modified.source().get("field2"));

        // Original and modified have different underlying maps
        assertNotSame(original.source(), modified.source());
    }

    public void testMutableVsImmutableSourceBehavior() {
        // Immutable map: original is NOT mutated, maps are different
        Source immutableSource = Source.fromMap(Map.of("field", "value"), XContentType.JSON);
        Source modifiedImmutable = immutableSource.withMutations(map -> map.put("new", "field"));
        assertNotSame("Immutable source should be copied", immutableSource.source(), modifiedImmutable.source());
        assertEquals(1, immutableSource.source().size());
        assertEquals(2, modifiedImmutable.source().size());

        // Mutable map (LinkedHashMap): original IS mutated, maps are same
        Map<String, Object> mutableMap = new LinkedHashMap<>();
        mutableMap.put("field", "value");
        Source mutableSource = Source.fromMap(mutableMap, XContentType.JSON);
        Source modifiedMutable = mutableSource.withMutations(map -> map.put("new", "field"));
        assertSame("Mutable source should NOT be copied", mutableSource.source(), modifiedMutable.source());
        assertEquals(2, mutableSource.source().size());
        assertEquals(2, modifiedMutable.source().size());
    }

    public void testWithMutationsFromMutableMap() {
        Map<String, Object> mutableMap = new LinkedHashMap<>();
        mutableMap.put("field1", "value1");
        Source original = Source.fromMap(mutableMap, XContentType.JSON);
        assertMapIsMutable("fromMap(LinkedHashMap)", original.source());

        Source modified = original.withMutations(map -> map.put("field2", "value2"));

        // When source map is a HashMap/LinkedHashMap, the original is mutated directly (no copy)
        assertEquals(2, original.source().size());
        assertEquals("value1", original.source().get("field1"));
        assertEquals("value2", original.source().get("field2"));

        // Modified and original share the same underlying map
        assertEquals(2, modified.source().size());
        assertEquals("value1", modified.source().get("field1"));
        assertEquals("value2", modified.source().get("field2"));
        assertSame(original.source(), modified.source());
    }

    public void testWithMutationsFromBytes() {
        Source original = Source.fromBytes(new BytesArray("{\"field1\":\"value1\"}"));
        assertMapIsMutable("fromBytes", original.source());

        Source modified = original.withMutations(map -> map.put("field2", "value2"));

        // fromBytes parses to a LinkedHashMap, so the original is mutated directly (no copy)
        assertEquals(2, original.source().size());
        assertEquals("value1", original.source().get("field1"));
        assertEquals("value2", original.source().get("field2"));

        // Modified and original share the same underlying map
        assertEquals(2, modified.source().size());
        assertEquals("value1", modified.source().get("field1"));
        assertEquals("value2", modified.source().get("field2"));
        assertSame(original.source(), modified.source());
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

    public void testInsertionOrder() {
        int numFields = randomIntBetween(5, 20);
        List<String> fieldNames = new ArrayList<>(numFields);
        Map<String, Object> fieldValues = new LinkedHashMap<>();
        for (int i = 0; i < numFields; i++) {
            String fieldName = "field_" + i + "_" + randomAlphaOfLength(5);
            fieldNames.add(fieldName);
            fieldValues.put(fieldName, randomAlphaOfLength(10));
        }

        // Helper to create a fresh source with the same field order
        // (since LinkedHashMap sources are mutated directly, we need fresh copies for each scenario)
        java.util.function.Supplier<Source> freshSource = () -> Source.fromMap(new LinkedHashMap<>(fieldValues), XContentType.JSON);

        // Test 1: Verify original preserves insertion order
        Source original = freshSource.get();
        List<String> originalKeys = new ArrayList<>(original.source().keySet());
        assertEquals("Original should preserve insertion order", fieldNames, originalKeys);

        // Test 2: Updating an existing field preserves order
        int updateIndex = randomIntBetween(0, numFields - 1);
        String fieldToUpdate = fieldNames.get(updateIndex);
        String newValue = "updated_" + randomAlphaOfLength(8);

        Source forUpdate = freshSource.get();
        Source modified = forUpdate.withMutations(map -> map.put(fieldToUpdate, newValue));

        assertEquals(newValue, modified.source().get(fieldToUpdate));
        List<String> modifiedKeys = new ArrayList<>(modified.source().keySet());
        assertEquals("Mutation should preserve insertion order", fieldNames, modifiedKeys);

        // Test 3: Adding a new field appends at end
        String newFieldName = "new_field_" + randomAlphaOfLength(5);
        Source forAdd = freshSource.get();
        Source withNewField = forAdd.withMutations(map -> map.put(newFieldName, "new_value"));

        List<String> expectedKeysWithNew = new ArrayList<>(fieldNames);
        expectedKeysWithNew.add(newFieldName);
        List<String> actualKeysWithNew = new ArrayList<>(withNewField.source().keySet());
        assertEquals("New field should be appended at end", expectedKeysWithNew, actualKeysWithNew);

        // Test 4: Removing a field preserves relative order of remaining fields
        int removeIndex = randomIntBetween(0, numFields - 1);
        String fieldToRemove = fieldNames.get(removeIndex);
        Source forRemoval = freshSource.get();
        Source withRemoval = forRemoval.withMutations(map -> map.remove(fieldToRemove));

        List<String> expectedAfterRemoval = new ArrayList<>(fieldNames);
        expectedAfterRemoval.remove(removeIndex);
        List<String> actualAfterRemoval = new ArrayList<>(withRemoval.source().keySet());
        assertEquals("Removal should preserve relative order of remaining fields", expectedAfterRemoval, actualAfterRemoval);
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
