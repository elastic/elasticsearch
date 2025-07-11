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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.instanceOf;

public class ESONSourceTests extends ESTestCase {

    public void testParseBasicTypes() throws Exception {
        String jsonString = """
            {
                "intField": 42,
                "longField": 9223372036854775807,
                "floatField": 3.14,
                "doubleField": 2.718281828459045,
                "stringField": "hello world",
                "booleanField": true,
                "nullField": null
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
            ESONSourceClaude.Builder builder = new ESONSourceClaude.Builder(false);
            ESONSourceClaude source = builder.parse(parser);
            ESONSourceClaude.ESONObject root = source.root();

            // Test integer
            assertThat(root.get("intField"), equalTo(42));
            assertThat(root.get("intField"), instanceOf(Integer.class));

            // Test long
            assertThat(root.get("longField"), equalTo(9223372036854775807L));
            assertThat(root.get("longField"), instanceOf(Long.class));

            // Test float
            // Jackson parses as double without custom parser
            assertThat((Double) root.get("floatField"), equalTo(3.14));
            assertThat(root.get("floatField"), instanceOf(Double.class));

            // Test double
            assertThat((Double) root.get("doubleField"), equalTo(2.718281828459045));
            assertThat(root.get("doubleField"), instanceOf(Double.class));

            // Test string
            assertThat(root.get("stringField"), equalTo("hello world"));
            assertThat(root.get("stringField"), instanceOf(String.class));

            // Test boolean
            assertThat(root.get("booleanField"), equalTo(true));
            assertThat(root.get("booleanField"), instanceOf(Boolean.class));

            // Test null
            assertThat(root.get("nullField"), nullValue());
        }
    }

//    public void testParseArray() throws Exception {
//        String jsonString = """
//            {
//                "mixedArray": [1, "string", true, null, 3.14],
//                "numberArray": [10, 20, 30],
//                "stringArray": ["a", "b", "c"],
//                "emptyArray": []
//            }
//            """;
//
//        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
//            ESONSourceClaude.Builder builder = new ESONSourceClaude.Builder(false);
//            ESONSourceClaude source = builder.parse(parser);
//            ESONSourceClaude.ESONObject root = source.root();
//
//            // Test mixed array
//            ESONSourceClaude.ESONArray mixedArray = (ESONSourceClaude.ESONArray) root.get("mixedArray");
//            assertThat(mixedArray.size(), equalTo(5));
//
//            // Access array elements through the type system for verification
//            ESONSourceClaude.Value intVal = (ESONSourceClaude.Value) mixedArray.get(0);
//            assertThat(intVal.getValue(new ESONSourceClaude.Values(source.data)), equalTo(1));
//
//            ESONSourceClaude.Value stringVal = (ESONSourceClaude.Value) mixedArray.get(1);
//            assertThat(stringVal.getValue(new ESONSourceClaude.Values(source.data)), equalTo("string"));
//
//            ESONSourceClaude.Value boolVal = (ESONSourceClaude.Value) mixedArray.get(2);
//            assertThat(boolVal.getValue(new ESONSourceClaude.Values(source.data)), equalTo(true));
//
//            ESONSourceClaude.NullValue nullVal = (ESONSourceClaude.NullValue) mixedArray.get(3);
//            assertThat(nullVal, instanceOf(ESONSourceClaude.NullValue.class));
//
//            ESONSourceClaude.Value floatVal = (ESONSourceClaude.Value) mixedArray.get(4);
//            assertThat((Float) floatVal.getValue(new ESONSourceClaude.Values(source.data)), equalTo(3.14f));
//
//            // Test number array
//            ESONSourceClaude.ESONArray numberArray = (ESONSourceClaude.ESONArray) root.get("numberArray");
//            assertThat(numberArray.size(), equalTo(3));
//
//            // Test string array
//            ESONSourceClaude.ESONArray stringArray = (ESONSourceClaude.ESONArray) root.get("stringArray");
//            assertThat(stringArray.size(), equalTo(3));
//
//            // Test empty array
//            ESONSourceClaude.ESONArray emptyArray = (ESONSourceClaude.ESONArray) root.get("emptyArray");
//            assertThat(emptyArray.size(), equalTo(0));
//        }
//    }

    public void testParseNestedObject() throws Exception {
        String jsonString = """
            {
                "user": {
                    "name": "John Doe",
                    "age": 30,
                    "active": true,
                    "address": {
                        "street": "123 Main St",
                        "city": "Springfield",
                        "zipcode": "12345"
                    }
                },
                "emptyObject": {}
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
            ESONSourceClaude.Builder builder = new ESONSourceClaude.Builder(false);
            ESONSourceClaude source = builder.parse(parser);
            ESONSourceClaude.ESONObject root = source.root();

            // Test nested object
            ESONSourceClaude.ESONObject user = (ESONSourceClaude.ESONObject) root.get("user");
            assertThat(user.get("name"), equalTo("John Doe"));
            assertThat(user.get("age"), equalTo(30));
            assertThat(user.get("active"), equalTo(true));

            // Test deeply nested object
            ESONSourceClaude.ESONObject address = (ESONSourceClaude.ESONObject) user.get("address");
            assertThat(address.get("street"), equalTo("123 Main St"));
            assertThat(address.get("city"), equalTo("Springfield"));
            assertThat(address.get("zipcode"), equalTo("12345"));

            // Test empty object
            ESONSourceClaude.ESONObject emptyObject = (ESONSourceClaude.ESONObject) root.get("emptyObject");
            assertThat(emptyObject.size(), equalTo(0));
            assertThat(emptyObject.isEmpty(), equalTo(true));
        }
    }

    public void testParseComplexStructure() throws Exception {
        String jsonString = """
            {
                "metadata": {
                    "version": 1,
                    "created": "2024-01-01"
                },
                "users": [
                    {
                        "id": 1,
                        "name": "Alice",
                        "scores": [95.5, 87.2, 92.1]
                    },
                    {
                        "id": 2,
                        "name": "Bob",
                        "scores": [88.0, 91.5, 89.7]
                    }
                ],
                "config": {
                    "enabled": true,
                    "maxUsers": 100,
                    "features": ["feature1", "feature2", "feature3"]
                }
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
            ESONSourceClaude.Builder builder = new ESONSourceClaude.Builder(false);
            ESONSourceClaude source = builder.parse(parser);
            ESONSourceClaude.ESONObject root = source.root();

            // Test metadata object
            ESONSourceClaude.ESONObject metadata = (ESONSourceClaude.ESONObject) root.get("metadata");
            assertThat(metadata.get("version"), equalTo(1));
            assertThat(metadata.get("created"), equalTo("2024-01-01"));

            // Test users array
            ESONSourceClaude.ESONArray users = (ESONSourceClaude.ESONArray) root.get("users");
            assertThat(users.size(), equalTo(2));

            // Test first user
            ESONSourceClaude.ESONObject user1 = (ESONSourceClaude.ESONObject) users.get(0);
            assertThat(user1.get("id"), equalTo(1));
            assertThat(user1.get("name"), equalTo("Alice"));

            ESONSourceClaude.ESONArray scores1 = (ESONSourceClaude.ESONArray) user1.get("scores");
            assertThat(scores1.size(), equalTo(3));

            // Test config object
            ESONSourceClaude.ESONObject config = (ESONSourceClaude.ESONObject) root.get("config");
            assertThat(config.get("enabled"), equalTo(true));
            assertThat(config.get("maxUsers"), equalTo(100));

            ESONSourceClaude.ESONArray features = (ESONSourceClaude.ESONArray) config.get("features");
            assertThat(features.size(), equalTo(3));
        }
    }

    public void testMapInterface() throws Exception {
        String jsonString = """
            {
                "field1": "value1",
                "field2": 42,
                "field3": true,
                "field4": null
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
            ESONSourceClaude.Builder builder = new ESONSourceClaude.Builder(false);
            ESONSourceClaude source = builder.parse(parser);
            ESONSourceClaude.ESONObject root = source.root();

            // Test Map interface methods
            assertThat(root.size(), equalTo(4));
            assertThat(root.isEmpty(), equalTo(false));
            assertThat(root.containsKey("field1"), equalTo(true));
            assertThat(root.containsKey("nonexistent"), equalTo(false));

            // Test keySet
            assertThat(root.keySet().size(), equalTo(4));
            assertThat(root.keySet().contains("field1"), equalTo(true));
            assertThat(root.keySet().contains("field2"), equalTo(true));
            assertThat(root.keySet().contains("field3"), equalTo(true));
            assertThat(root.keySet().contains("field4"), equalTo(true));

            // Test values
            assertThat(root.values().size(), equalTo(4));

            // Test entrySet
            assertThat(root.entrySet().size(), equalTo(4));
        }
    }

    public void testSpecialNumbers() throws Exception {
        String jsonString = """
            {
                "maxInt": 2147483647,
                "minInt": -2147483648,
                "maxLong": 9223372036854775807,
                "minLong": -9223372036854775808,
                "zero": 0,
                "negativeFloat": -3.14,
                "scientificNotation": 1.23e-4
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
            ESONSourceClaude.Builder builder = new ESONSourceClaude.Builder(false);
            ESONSourceClaude source = builder.parse(parser);
            ESONSourceClaude.ESONObject root = source.root();

            assertThat(root.get("maxInt"), equalTo(Integer.MAX_VALUE));
            assertThat(root.get("minInt"), equalTo(Integer.MIN_VALUE));
            assertThat(root.get("maxLong"), equalTo(Long.MAX_VALUE));
            assertThat(root.get("minLong"), equalTo(Long.MIN_VALUE));
            assertThat(root.get("zero"), equalTo(0));
            // Jackson parses as double
            assertThat((Double) root.get("negativeFloat"), equalTo(-3.14));
            assertThat((Double) root.get("scientificNotation"), equalTo(1.23e-4));
        }
    }

    public void testEmptyAndNullCases() throws Exception {
        String jsonString = """
            {
                "emptyString": "",
                "nullValue": null,
                "emptyArray": [],
                "emptyObject": {},
                "arrayWithNulls": [null, "value", null]
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
            ESONSourceClaude.Builder builder = new ESONSourceClaude.Builder(false);
            ESONSourceClaude source = builder.parse(parser);
            ESONSourceClaude.ESONObject root = source.root();

            assertThat(root.get("emptyString"), equalTo(""));
            assertThat(root.get("nullValue"), nullValue());

            ESONSourceClaude.ESONArray emptyArray = (ESONSourceClaude.ESONArray) root.get("emptyArray");
            assertThat(emptyArray.size(), equalTo(0));

            ESONSourceClaude.ESONObject emptyObject = (ESONSourceClaude.ESONObject) root.get("emptyObject");
            assertThat(emptyObject.size(), equalTo(0));

            ESONSourceClaude.ESONArray arrayWithNulls = (ESONSourceClaude.ESONArray) root.get("arrayWithNulls");
            assertThat(arrayWithNulls.size(), equalTo(3));
            assertThat(arrayWithNulls.get(0), instanceOf(ESONSourceClaude.NullValue.class));
            assertThat(arrayWithNulls.get(2), instanceOf(ESONSourceClaude.NullValue.class));
        }
    }

    public void testUnicodeStrings() throws Exception {
        String jsonString = """
            {
                "unicode": "Hello 世界! 🌍",
                "emoji": "🚀✨🎉",
                "accents": "café naïve résumé"
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
            ESONSourceClaude.Builder builder = new ESONSourceClaude.Builder(false);
            ESONSourceClaude source = builder.parse(parser);
            ESONSourceClaude.ESONObject root = source.root();

            assertThat(root.get("unicode"), equalTo("Hello 世界! 🌍"));
            assertThat(root.get("emoji"), equalTo("🚀✨🎉"));
            assertThat(root.get("accents"), equalTo("café naïve résumé"));
        }
    }
}
