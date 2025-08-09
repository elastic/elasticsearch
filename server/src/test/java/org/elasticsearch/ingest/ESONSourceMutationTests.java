/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ESONSourceMutationTests extends ESTestCase {

    public void testInPlaceFixedValueMutations() throws Exception {
        String jsonString = """
            {
                "intField": 42,
                "longField": 9223372036854775807,
                "floatField": 3.14,
                "doubleField": 2.718281828459045,
                "booleanField": true
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
            ESONSource.Builder builder = new ESONSource.Builder();
            ESONSource.ESONObject root = builder.parse(parser);

            // Test in-place int modification
            Object oldIntValue = root.put("intField", 100);
            assertThat(oldIntValue, equalTo(42));
            assertThat(root.get("intField"), equalTo(100));
            assertThat(root.get("intField"), instanceOf(Integer.class));

            // Test in-place long modification
            Object oldLongValue = root.put("longField", 1000L);
            assertThat(oldLongValue, equalTo(9223372036854775807L));
            assertThat(root.get("longField"), equalTo(1000L));
            assertThat(root.get("longField"), instanceOf(Long.class));

            // Test in-place float modification
            Object oldFloatValue = root.put("floatField", 2.5f);
            assertThat(oldFloatValue, equalTo(3.14d));
            assertThat((Float) root.get("floatField"), equalTo(2.5f));
            assertThat(root.get("floatField"), instanceOf(Float.class));

            // Test in-place double modification
            Object oldDoubleValue = root.put("doubleField", 1.23456789);
            assertThat((Double) oldDoubleValue, equalTo(2.718281828459045));
            assertThat((Double) root.get("doubleField"), equalTo(1.23456789));
            assertThat(root.get("doubleField"), instanceOf(Double.class));

            // Test in-place boolean modification
            Object oldBooleanValue = root.put("booleanField", false);
            assertThat(oldBooleanValue, equalTo(true));
            assertThat(root.get("booleanField"), equalTo(false));
            assertThat(root.get("booleanField"), instanceOf(Boolean.class));

            // Verify other fields remain unchanged
            assertThat(root.get("intField"), equalTo(100));
            assertThat(root.get("longField"), equalTo(1000L));
        }
    }

    public void testTrackedModifications() throws Exception {
        String jsonString = """
            {
                "intField": 42,
                "stringField": "original",
                "booleanField": true
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
            ESONSource.Builder builder = new ESONSource.Builder();
            ESONSource.ESONObject root = builder.parse(parser);

            // Test type change modifications (should be tracked, not in-place)
            Object oldIntValue = root.put("intField", "forty-two");
            assertThat(oldIntValue, equalTo(42));
            assertThat(root.get("intField"), equalTo("forty-two"));
            assertThat(root.get("intField"), instanceOf(String.class));

            // Test string modification (variable length, should be tracked)
            Object oldStringValue = root.put("stringField", "modified");
            assertThat(oldStringValue, equalTo("original"));
            assertThat(root.get("stringField"), equalTo("modified"));
            assertThat(root.get("stringField"), instanceOf(String.class));

            // Test new field addition
            Object oldNewField = root.put("newField", "new value");
            assertThat(oldNewField, nullValue());
            assertThat(root.get("newField"), equalTo("new value"));

            // Verify boolean field remains unchanged
            assertThat(root.get("booleanField"), equalTo(true));
        }
    }

    public void testMixedInPlaceAndTrackedModifications() throws Exception {
        String jsonString = """
            {
                "intField": 42,
                "stringField": "original",
                "floatField": 3.14,
                "booleanField": true
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
            ESONSource.Builder builder = new ESONSource.Builder();
            ESONSource.ESONObject root = builder.parse(parser);

            // Mix of in-place and tracked modifications
            root.put("intField", 100);              // In-place
            root.put("stringField", "modified");    // Tracked
            root.put("floatField", 2.5f);          // In-place
            root.put("booleanField", "not boolean"); // Tracked (type change)

            // Verify all modifications
            assertThat(root.get("intField"), equalTo(100));
            assertThat(root.get("stringField"), equalTo("modified"));
            assertThat((Float) root.get("floatField"), equalTo(2.5f));
            assertThat(root.get("booleanField"), equalTo("not boolean"));

            // Test overwriting modifications
            root.put("intField", 200);              // In-place overwrite
            root.put("stringField", "modified again"); // Tracked overwrite

            assertThat(root.get("intField"), equalTo(200));
            assertThat(root.get("stringField"), equalTo("modified again"));
        }
    }

    public void testFieldRemoval() throws Exception {
        String jsonString = """
            {
                "field1": 42,
                "field2": "value",
                "field3": true
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
            ESONSource.Builder builder = new ESONSource.Builder();
            ESONSource.ESONObject root = builder.parse(parser);

            // Test removal
            Object removedValue = root.remove("field2");
            assertThat(removedValue, equalTo("value"));
            assertThat(root.get("field2"), nullValue());

            // Verify other fields remain
            assertThat(root.get("field1"), equalTo(42));
            assertThat(root.get("field3"), equalTo(true));

            // Test removing non-existent field
            Object nonExistent = root.remove("nonExistent");
            assertThat(nonExistent, nullValue());
        }
    }

    public void testArrayMutations() throws Exception {
        String jsonString = """
            {
                "numbers": [1, 2, 3],
                "mixed": [42, "string", true, 3.14],
                "booleans": [true, false, true]
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
            ESONSource.Builder builder = new ESONSource.Builder();
            ESONSource.ESONObject root = builder.parse(parser);

            // Test array in-place modifications
            ESONSource.ESONArray numbers = (ESONSource.ESONArray) root.get("numbers");
            Object oldValue = numbers.set(0, 10);
            assertThat(oldValue, equalTo(1));
            assertThat(numbers.get(0), equalTo(10));
            assertThat(numbers.get(1), equalTo(2));
            assertThat(numbers.get(2), equalTo(3));

            // Test array tracked modifications (type change)
            ESONSource.ESONArray mixed = (ESONSource.ESONArray) root.get("mixed");
            Object oldMixedValue = mixed.set(0, "forty-two");
            assertThat(oldMixedValue, equalTo(42));
            assertThat(mixed.get(0), equalTo("forty-two"));
            assertThat(mixed.get(1), equalTo("string"));
            assertThat(mixed.get(2), equalTo(true));
            assertThat((Double) mixed.get(3), equalTo(3.14d));

            // Test boolean array in-place modifications
            ESONSource.ESONArray booleans = (ESONSource.ESONArray) root.get("booleans");
            booleans.set(1, true);
            assertThat(booleans.get(0), equalTo(true));
            assertThat(booleans.get(1), equalTo(true));
            assertThat(booleans.get(2), equalTo(true));
        }
    }

    public void testNestedObjectMutations() throws Exception {
        String jsonString = """
            {
                "user": {
                    "name": "John",
                    "age": 30,
                    "active": true
                },
                "settings": {
                    "theme": "dark",
                    "notifications": true
                }
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
            ESONSource.Builder builder = new ESONSource.Builder();
            ESONSource.ESONObject root = builder.parse(parser);

            // Test nested object modifications
            ESONSource.ESONObject user = (ESONSource.ESONObject) root.get("user");
            user.put("name", "Jane");           // Tracked (variable length)
            user.put("age", 25);                // In-place
            user.put("active", false);          // In-place

            assertThat(user.get("name"), equalTo("Jane"));
            assertThat(user.get("age"), equalTo(25));
            assertThat(user.get("active"), equalTo(false));

            // Test adding new field to nested object
            user.put("email", "jane@example.com");
            assertThat(user.get("email"), equalTo("jane@example.com"));

            // Verify other nested object is unchanged
            ESONSource.ESONObject settings = (ESONSource.ESONObject) root.get("settings");
            assertThat(settings.get("theme"), equalTo("dark"));
            assertThat(settings.get("notifications"), equalTo(true));
        }
    }

    public void testComplexNestedMutations() throws Exception {
        String jsonString = """
            {
                "data": {
                    "users": [
                        {"id": 1, "name": "Alice", "score": 95.5},
                        {"id": 2, "name": "Bob", "score": 87.2}
                    ],
                    "config": {
                        "maxUsers": 100,
                        "enabled": true
                    }
                }
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
            ESONSource.Builder builder = new ESONSource.Builder();
            ESONSource.ESONObject root = builder.parse(parser);

            // Navigate to nested structures
            ESONSource.ESONObject data = (ESONSource.ESONObject) root.get("data");
            ESONSource.ESONArray users = (ESONSource.ESONArray) data.get("users");
            ESONSource.ESONObject user1 = (ESONSource.ESONObject) users.get(0);
            ESONSource.ESONObject config = (ESONSource.ESONObject) data.get("config");

            // Test deep mutations
            user1.put("name", "Alice Smith");   // Tracked
            user1.put("score", 98.0);          // In-place (double)
            user1.put("active", true);         // New field

            config.put("maxUsers", 200);       // In-place
            config.put("enabled", false);     // In-place

            // Verify deep changes
            assertThat(user1.get("name"), equalTo("Alice Smith"));
            assertThat((Double) user1.get("score"), equalTo(98.0));
            assertThat(user1.get("active"), equalTo(true));
            assertThat(config.get("maxUsers"), equalTo(200));
            assertThat(config.get("enabled"), equalTo(false));

            // Verify other user is unchanged
            ESONSource.ESONObject user2 = (ESONSource.ESONObject) users.get(1);
            assertThat(user2.get("name"), equalTo("Bob"));
            assertThat((Double) user2.get("score"), equalTo(87.2));
        }
    }

    public void testMapInterfaceMethods() throws Exception {
        String jsonString = """
            {
                "field1": 42,
                "field2": "value",
                "field3": true
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
            ESONSource.Builder builder = new ESONSource.Builder();
            ESONSource.ESONObject root = builder.parse(parser);

            // Test putAll
            Map<String, Object> updates = new HashMap<>();
            updates.put("field1", 100);
            updates.put("field4", "new field");
            root.putAll(updates);

            assertThat(root.get("field1"), equalTo(100));
            assertThat(root.get("field4"), equalTo("new field"));

            // Test keySet after modifications
            Set<String> keys = root.keySet();
            assertThat(keys.size(), equalTo(4));
            assertThat(keys.contains("field1"), equalTo(true));
            assertThat(keys.contains("field2"), equalTo(true));
            assertThat(keys.contains("field3"), equalTo(true));
            assertThat(keys.contains("field4"), equalTo(true));

            // Test values collection
            Collection<Object> values = root.values();
            assertThat(values.size(), equalTo(4));

            // Test entrySet
            Set<Map.Entry<String, Object>> entries = root.entrySet();
            assertThat(entries.size(), equalTo(4));

            // Verify entrySet reflects modifications
            boolean foundModifiedField1 = false;
            for (Map.Entry<String, Object> entry : entries) {
                if ("field1".equals(entry.getKey())) {
                    assertThat(entry.getValue(), equalTo(100));
                    foundModifiedField1 = true;
                }
            }
            assertThat(foundModifiedField1, equalTo(true));
        }
    }

    public void testClearOperation() throws Exception {
        String jsonString = """
            {
                "field1": 42,
                "field2": "value",
                "field3": true
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
            ESONSource.Builder builder = new ESONSource.Builder();
            ESONSource.ESONObject root = builder.parse(parser);

            // Verify initial state
            assertThat(root.get("field1"), equalTo(42));
            assertThat(root.get("field2"), equalTo("value"));
            assertThat(root.get("field3"), equalTo(true));

            root.put("field4", "new field");

            // Clear all fields
            root.clear();

            // Verify all fields are now null
            assertThat(root.get("field1"), nullValue());
            assertThat(root.get("field2"), nullValue());
            assertThat(root.get("field3"), nullValue());
            assertThat(root.get("field4"), nullValue());
        }
    }

    public void testReadAfterMultipleWrites() throws Exception {
        String jsonString = """
            {
                "counter": 0,
                "message": "start"
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
            ESONSource.Builder builder = new ESONSource.Builder();
            ESONSource.ESONObject root = builder.parse(parser);

            // Multiple writes to same field
            root.put("counter", 1);
            assertThat(root.get("counter"), equalTo(1));

            root.put("counter", 2);
            assertThat(root.get("counter"), equalTo(2));

            root.put("counter", 3);
            assertThat(root.get("counter"), equalTo(3));

            // Type change
            root.put("counter", "three");
            assertThat(root.get("counter"), equalTo("three"));

            // Back to number
            root.put("counter", 4);
            assertThat(root.get("counter"), equalTo(4));

            // Multiple string modifications
            root.put("message", "step1");
            assertThat(root.get("message"), equalTo("step1"));

            root.put("message", "step2");
            assertThat(root.get("message"), equalTo("step2"));

            root.put("message", "final");
            assertThat(root.get("message"), equalTo("final"));
        }
    }

    /*
     * Additional test methods for ESONSourceMutationTests to improve coverage
     */

    // Array mutation tests - missing from current test suite
    public void testArrayAddOperations() throws Exception {
        String jsonString = """
            {
                "numbers": [1, 2, 3],
                "empty": []
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
            ESONSource.Builder builder = new ESONSource.Builder();
            ESONSource.ESONObject root = builder.parse(parser);

            ESONSource.ESONArray numbers = (ESONSource.ESONArray) root.get("numbers");
            ESONSource.ESONArray empty = (ESONSource.ESONArray) root.get("empty");

            // Test add() method
            numbers.add(4);
            assertThat(numbers.size(), equalTo(4));
            assertThat(numbers.get(3), equalTo(4));

            // Test add(index, element) method
            numbers.add(1, 10);
            assertThat(numbers.size(), equalTo(5));
            assertThat(numbers.get(1), equalTo(10));
            assertThat(numbers.get(2), equalTo(2)); // shifted

            // Test adding to empty array
            empty.add("first");
            assertThat(empty.size(), equalTo(1));
            assertThat(empty.get(0), equalTo("first"));

            // Test adding different types
            numbers.add("string");
            numbers.add(true);
            numbers.add(3.14);
            assertThat(numbers.get(5), equalTo("string"));
            assertThat(numbers.get(6), equalTo(true));
            assertThat((Double) numbers.get(7), equalTo(3.14));
        }
    }

    public void testArrayRemoveOperations() throws Exception {
        String jsonString = """
            {
                "items": [1, 2, "three", 4.0, true]
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
            ESONSource.Builder builder = new ESONSource.Builder();
            ESONSource.ESONObject root = builder.parse(parser);

            ESONSource.ESONArray items = (ESONSource.ESONArray) root.get("items");

            // Test remove by index
            Object removed = items.remove(2);
            assertThat(removed, equalTo("three"));
            assertThat(items.size(), equalTo(4));
            assertThat((Double) items.get(2), equalTo(4.0)); // shifted

            // Test remove by object
            boolean removedBoolean = items.remove((Object) true);
            assertThat(removedBoolean, equalTo(true));
            assertThat(items.size(), equalTo(3));

            // Test remove non-existent object
            boolean removedNonExistent = items.remove((Object) "nonexistent");
            assertThat(removedNonExistent, equalTo(false));
            assertThat(items.size(), equalTo(3));

            // Test removing first and last elements
            items.remove(0);
            assertThat(items.size(), equalTo(2));
            assertThat(items.get(0), equalTo(2));

            items.remove(items.size() - 1);
            assertThat(items.size(), equalTo(1));
            assertThat(items.get(0), equalTo(2));
        }
    }

    public void testArrayIteratorOperations() throws Exception {
        String jsonString = """
            {
                "items": [1, 2, 3, 4, 5]
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
            ESONSource.Builder builder = new ESONSource.Builder();
            ESONSource.ESONObject root = builder.parse(parser);

            ESONSource.ESONArray items = (ESONSource.ESONArray) root.get("items");

            // Test iterator basic functionality
            Iterator<Object> iterator = items.iterator();
            int count = 0;
            while (iterator.hasNext()) {
                Object item = iterator.next();
                assertThat(item, equalTo(count + 1));
                count++;
            }
            assertThat(count, equalTo(5));

            // Test iterator remove functionality
            iterator = items.iterator();
            iterator.next(); // 1
            iterator.next(); // 2
            iterator.remove(); // remove 2
            assertThat(items.size(), equalTo(4));
            assertThat(items.get(0), equalTo(1));
            assertThat(items.get(1), equalTo(3)); // 3 shifted to index 1

            // Test enhanced for loop
            int sum = 0;
            for (Object item : items) {
                sum += (Integer) item;
            }
            assertThat(sum, equalTo(13)); // 1 + 3 + 4 + 5

            // Test iterator remove at different positions
            iterator = items.iterator();
            iterator.next(); // 1
            iterator.remove(); // remove first
            assertThat(items.get(0), equalTo(3));
            assertThat(items.size(), equalTo(3));
        }
    }

    public void testObjectEntrySetIteratorRemove() throws Exception {
        String jsonString = """
            {
                "field1": 42,
                "field2": "value",
                "field3": true,
                "field4": 3.14
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
            ESONSource.Builder builder = new ESONSource.Builder();
            ESONSource.ESONObject root = builder.parse(parser);

            Set<Map.Entry<String, Object>> entrySet = root.entrySet();
            assertThat(entrySet.size(), equalTo(4));

            // Test iterator remove
            Iterator<Map.Entry<String, Object>> iterator = entrySet.iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Object> entry = iterator.next();
                if ("field2".equals(entry.getKey())) {
                    iterator.remove();
                    break;
                }
            }

            assertThat(root.size(), equalTo(3));
            assertThat(root.get("field2"), nullValue());
            assertThat(root.get("field1"), equalTo(42));
            assertThat(root.get("field3"), equalTo(true));
            assertThat((Double) root.get("field4"), equalTo(3.14));
        }
    }

    public void testObjectEntrySetRemoveByEntry() throws Exception {
        String jsonString = """
            {
                "field1": 42,
                "field2": "value",
                "field3": true
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
            ESONSource.Builder builder = new ESONSource.Builder();
            ESONSource.ESONObject root = builder.parse(parser);

            Set<Map.Entry<String, Object>> entrySet = root.entrySet();

            // Test remove by entry (matching key and value)
            Map.Entry<String, Object> entryToRemove = new AbstractMap.SimpleEntry<>("field2", "value");
            boolean removed = entrySet.remove(entryToRemove);
            assertThat(removed, equalTo(true));
            assertThat(root.get("field2"), nullValue());

            // Test remove by entry (non-matching value)
            Map.Entry<String, Object> nonMatchingEntry = new AbstractMap.SimpleEntry<>("field1", 99);
            boolean notRemoved = entrySet.remove(nonMatchingEntry);
            assertThat(notRemoved, equalTo(false));
            assertThat(root.get("field1"), equalTo(42));

            // Test remove by entry (non-existent key)
            Map.Entry<String, Object> nonExistentEntry = new AbstractMap.SimpleEntry<>("nonexistent", "value");
            boolean notRemovedNonExistent = entrySet.remove(nonExistentEntry);
            assertThat(notRemovedNonExistent, equalTo(false));
        }
    }

    public void testObjectEntrySetContains() throws Exception {
        String jsonString = """
            {
                "field1": 42,
                "field2": "value",
                "field3": true
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
            ESONSource.Builder builder = new ESONSource.Builder();
            ESONSource.ESONObject root = builder.parse(parser);

            Set<Map.Entry<String, Object>> entrySet = root.entrySet();

            // Test contains with matching entry
            Map.Entry<String, Object> matchingEntry = new AbstractMap.SimpleEntry<>("field1", 42);
            assertThat(entrySet.contains(matchingEntry), equalTo(true));

            // Test contains with non-matching value
            Map.Entry<String, Object> nonMatchingEntry = new AbstractMap.SimpleEntry<>("field1", 99);
            assertThat(entrySet.contains(nonMatchingEntry), equalTo(false));

            // Test contains with non-existent key
            Map.Entry<String, Object> nonExistentEntry = new AbstractMap.SimpleEntry<>("nonexistent", "value");
            assertThat(entrySet.contains(nonExistentEntry), equalTo(false));

            // Test contains with non-Entry object
            assertThat(entrySet.contains("not an entry"), equalTo(false));
            assertThat(entrySet.contains(null), equalTo(false));

            // Test contains with non-String key
            Map.Entry<Integer, Object> intKeyEntry = new AbstractMap.SimpleEntry<>(42, "value");
            assertThat(entrySet.contains(intKeyEntry), equalTo(false));
        }
    }

    public void testObjectValuesCollection() throws Exception {
        String jsonString = """
            {
                "field1": 42,
                "field2": "value",
                "field3": true
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
            ESONSource.Builder builder = new ESONSource.Builder();
            ESONSource.ESONObject root = builder.parse(parser);

            Collection<Object> values = root.values();

            // Test values collection properties
            assertThat(values.size(), equalTo(3));
            assertThat(values.contains(42), equalTo(true));
            assertThat(values.contains("value"), equalTo(true));
            assertThat(values.contains(true), equalTo(true));
            assertThat(values.contains("nonexistent"), equalTo(false));

            // Test values iterator
            Iterator<Object> iterator = values.iterator();
            int count = 0;
            Set<Object> expected = Set.of(42, "value", true);
            HashSet<Object> actual = new HashSet<>();
            while (iterator.hasNext()) {
                Object value = iterator.next();
                actual.add(value);
                count++;
            }
            assertThat(count, equalTo(3));
            assertThat(actual, equalTo(expected));

            // Test values collection after modification
            root.put("field4", "new value");
            assertThat(values.size(), equalTo(4));
            assertThat(values.contains("new value"), equalTo(true));
        }
    }

    public void testArraySubListOperations() throws Exception {
        String jsonString = """
            {
                "items": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
            ESONSource.Builder builder = new ESONSource.Builder();
            ESONSource.ESONObject root = builder.parse(parser);

            ESONSource.ESONArray items = (ESONSource.ESONArray) root.get("items");

            // Test subList functionality
            List<Object> subList = items.subList(2, 5);
            assertThat(subList.size(), equalTo(3));
            assertThat(subList.get(0), equalTo(3));
            assertThat(subList.get(1), equalTo(4));
            assertThat(subList.get(2), equalTo(5));

            // Test subList modifications affect original
            subList.set(1, 99);
            assertThat(items.get(3), equalTo(99));

            // Test subList clear
            subList.clear();
            assertThat(items.size(), equalTo(7));
            assertThat(items.get(2), equalTo(6)); // element shifted
        }
    }

    // public void testNestedArrayMutations() throws Exception {
    // String jsonString = """
    // {
    // "matrix": [
    // [1, 2, 3],
    // [4, 5, 6],
    // [7, 8, 9]
    // ]
    // }
    // """;
    //
    // try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
    // ESONSource.Builder builder = new ESONSource.Builder();
    // ESONSource.ESONObject root = builder.parse(parser);
    //
    // ESONSource.ESONArray matrix = (ESONSource.ESONArray) root.get("matrix");
    // ESONSource.ESONArray row1 = (ESONSource.ESONArray) matrix.get(0);
    // ESONSource.ESONArray row2 = (ESONSource.ESONArray) matrix.get(1);
    //
    // // Test nested array mutations
    // row1.set(1, 99);
    // assertThat(row1.get(1), equalTo(99));
    //
    // row2.add(100);
    // assertThat(row2.size(), equalTo(4));
    // assertThat(row2.get(3), equalTo(100));
    //
    // // Test adding new row
    // ESONSource.ESONArray newRow = new ESONSource.ESONArray(
    // List.of(new ESONSource.Mutation(10), new ESONSource.Mutation(11), new ESONSource.Mutation(12)),
    // root.objectValues()
    // );
    // matrix.add(newRow);
    // assertThat(matrix.size(), equalTo(4));
    //
    // ESONSource.ESONArray addedRow = (ESONSource.ESONArray) matrix.get(3);
    // assertThat(addedRow.get(0), equalTo(10));
    // assertThat(addedRow.get(1), equalTo(11));
    // assertThat(addedRow.get(2), equalTo(12));
    // }
    // }

    public void testMutationEqualsAndHashCode() throws Exception {
        String jsonString = """
            {
                "field1": 42,
                "field2": "value"
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
            ESONSource.Builder builder = new ESONSource.Builder();
            ESONSource.ESONObject root = builder.parse(parser);

            // Test entry equals and hashCode after mutations
            root.put("field1", 100);
            root.put("field3", "new field");

            Set<Map.Entry<String, Object>> entrySet = root.entrySet();

            // Find the modified entry
            Map.Entry<String, Object> modifiedEntry = null;
            for (Map.Entry<String, Object> entry : entrySet) {
                if ("field1".equals(entry.getKey())) {
                    modifiedEntry = entry;
                    break;
                }
            }

            assertThat(modifiedEntry, notNullValue());
            assertThat(modifiedEntry.getValue(), equalTo(100));

            // Test equals
            Map.Entry<String, Object> equivalentEntry = new AbstractMap.SimpleEntry<>("field1", 100);
            assertThat(modifiedEntry.equals(equivalentEntry), equalTo(true));
            assertThat(modifiedEntry.hashCode(), equalTo(equivalentEntry.hashCode()));

            // Test setValue on entry
            Object oldValue = modifiedEntry.setValue(200);
            assertThat(oldValue, equalTo(100));
            assertThat(root.get("field1"), equalTo(200));
            assertThat(modifiedEntry.getValue(), equalTo(200));
        }
    }

    public void testArrayBoundaryConditions() throws Exception {
        String jsonString = """
            {
                "items": [1, 2, 3]
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
            ESONSource.Builder builder = new ESONSource.Builder();
            ESONSource.ESONObject root = builder.parse(parser);

            ESONSource.ESONArray items = (ESONSource.ESONArray) root.get("items");

            // Test boundary conditions
            assertThrows(IndexOutOfBoundsException.class, () -> items.get(3));
            assertThrows(IndexOutOfBoundsException.class, () -> items.get(-1));
            assertThrows(IndexOutOfBoundsException.class, () -> items.set(3, "value"));
            assertThrows(IndexOutOfBoundsException.class, () -> items.remove(3));

            // Test add at boundary indices
            items.add(0, "first");
            assertThat(items.get(0), equalTo("first"));
            assertThat(items.size(), equalTo(4));

            items.add(items.size(), "last");
            assertThat(items.get(items.size() - 1), equalTo("last"));
            assertThat(items.size(), equalTo(5));

            // Test remove at boundaries
            items.remove(0);
            assertThat(items.get(0), equalTo(1));
            assertThat(items.size(), equalTo(4));

            items.remove(items.size() - 1);
            assertThat(items.size(), equalTo(3));
        }
    }

    public void testObjectKeySetModifications() throws Exception {
        String jsonString = """
            {
                "field1": 42,
                "field2": "value",
                "field3": true
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
            ESONSource.Builder builder = new ESONSource.Builder();
            ESONSource.ESONObject root = builder.parse(parser);

            Set<String> keySet = root.keySet();
            assertThat(keySet.size(), equalTo(3));
            assertThat(keySet.contains("field1"), equalTo(true));
            assertThat(keySet.contains("field2"), equalTo(true));
            assertThat(keySet.contains("field3"), equalTo(true));

            // Test keySet reflects modifications
            root.put("field4", "new");
            assertThat(keySet.size(), equalTo(4));
            assertThat(keySet.contains("field4"), equalTo(true));

            root.remove("field2");
            assertThat(keySet.size(), equalTo(3));
            assertThat(keySet.contains("field2"), equalTo(false));

            // Test keySet iterator
            Iterator<String> keyIterator = keySet.iterator();
            int count = 0;
            while (keyIterator.hasNext()) {
                String key = keyIterator.next();
                assertThat(key, anyOf(equalTo("field1"), equalTo("field3"), equalTo("field4")));
                count++;
            }
            assertThat(count, equalTo(3));
        }
    }

    public void testObjectIsEmpty() throws Exception {
        String jsonString = """
            {
                "field1": 42
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
            ESONSource.Builder builder = new ESONSource.Builder();
            ESONSource.ESONObject root = builder.parse(parser);

            assertThat(root.isEmpty(), equalTo(false));

            root.remove("field1");
            assertThat(root.isEmpty(), equalTo(true));

            root.put("newField", "value");
            assertThat(root.isEmpty(), equalTo(false));

            root.clear();
            assertThat(root.isEmpty(), equalTo(true));
        }
    }

    public void testArrayIsEmpty() throws Exception {
        String jsonString = """
            {
                "items": [1, 2, 3],
                "empty": []
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
            ESONSource.Builder builder = new ESONSource.Builder();
            ESONSource.ESONObject root = builder.parse(parser);

            ESONSource.ESONArray items = (ESONSource.ESONArray) root.get("items");
            ESONSource.ESONArray empty = (ESONSource.ESONArray) root.get("empty");

            assertThat(items.isEmpty(), equalTo(false));
            assertThat(empty.isEmpty(), equalTo(true));

            items.clear();
            assertThat(items.isEmpty(), equalTo(true));

            empty.add("item");
            assertThat(empty.isEmpty(), equalTo(false));
        }
    }
}
