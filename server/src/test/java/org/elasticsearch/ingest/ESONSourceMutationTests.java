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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
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
            ESONSource.Builder builder = new ESONSource.Builder(false);
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
            ESONSource.Builder builder = new ESONSource.Builder(false);
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
            ESONSource.Builder builder = new ESONSource.Builder(false);
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
            ESONSource.Builder builder = new ESONSource.Builder(false);
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
            ESONSource.Builder builder = new ESONSource.Builder(false);
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
            ESONSource.Builder builder = new ESONSource.Builder(false);
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
            ESONSource.Builder builder = new ESONSource.Builder(false);
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
            ESONSource.Builder builder = new ESONSource.Builder(false);
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
            ESONSource.Builder builder = new ESONSource.Builder(false);
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
            ESONSource.Builder builder = new ESONSource.Builder(false);
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
}
