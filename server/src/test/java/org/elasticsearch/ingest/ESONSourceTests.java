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
            // Jackson always parses as Double
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

    public void testParseArray() throws Exception {
        String jsonString = """
            {
                "mixedArray": [1, "string", true, null, 3.14],
                "numberArray": [10, 20, 30],
                "stringArray": ["a", "b", "c"],
                "emptyArray": []
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
            ESONSourceClaude.Builder builder = new ESONSourceClaude.Builder(false);
            ESONSourceClaude source = builder.parse(parser);
            ESONSourceClaude.ESONObject root = source.root();

            // Test mixed array using List<Object> interface
            ESONSourceClaude.ESONArray mixedArray = (ESONSourceClaude.ESONArray) root.get("mixedArray");
            assertThat(mixedArray.size(), equalTo(5));
            assertThat(mixedArray.get(0), equalTo(1));
            assertThat(mixedArray.get(1), equalTo("string"));
            assertThat(mixedArray.get(2), equalTo(true));
            assertThat(mixedArray.get(3), nullValue());
            // Jackson always parses as Double
            assertThat((Double) mixedArray.get(4), equalTo(3.14));

            // Test number array
            ESONSourceClaude.ESONArray numberArray = (ESONSourceClaude.ESONArray) root.get("numberArray");
            assertThat(numberArray.size(), equalTo(3));
            assertThat(numberArray.get(0), equalTo(10));
            assertThat(numberArray.get(1), equalTo(20));
            assertThat(numberArray.get(2), equalTo(30));

            // Test string array
            ESONSourceClaude.ESONArray stringArray = (ESONSourceClaude.ESONArray) root.get("stringArray");
            assertThat(stringArray.size(), equalTo(3));
            assertThat(stringArray.get(0), equalTo("a"));
            assertThat(stringArray.get(1), equalTo("b"));
            assertThat(stringArray.get(2), equalTo("c"));

            // Test empty array
            ESONSourceClaude.ESONArray emptyArray = (ESONSourceClaude.ESONArray) root.get("emptyArray");
            assertThat(emptyArray.size(), equalTo(0));
            assertThat(emptyArray.isEmpty(), equalTo(true));
        }
    }

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
            // Jackson always parses as Double
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
            assertThat(arrayWithNulls.get(0), nullValue());
            assertThat(arrayWithNulls.get(2), nullValue());
        }
    }

    public void testArrayListInterface() throws Exception {
        String jsonString = """
            {
                "testArray": [1, 2, 3, "hello", true, null]
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
            ESONSourceClaude.Builder builder = new ESONSourceClaude.Builder(false);
            ESONSourceClaude source = builder.parse(parser);
            ESONSourceClaude.ESONObject root = source.root();

            ESONSourceClaude.ESONArray array = (ESONSourceClaude.ESONArray) root.get("testArray");

            // Test List interface methods
            assertThat(array.size(), equalTo(6));
            assertThat(array.isEmpty(), equalTo(false));
            assertThat(array.contains(1), equalTo(true));
            assertThat(array.contains("hello"), equalTo(true));
            assertThat(array.contains(null), equalTo(true));
            assertThat(array.contains("not found"), equalTo(false));

            // Test indexOf
            assertThat(array.indexOf(1), equalTo(0));
            assertThat(array.indexOf("hello"), equalTo(3));
            assertThat(array.indexOf(null), equalTo(5));
            assertThat(array.indexOf("not found"), equalTo(-1));

            // Test iteration
            int count = 0;
            for (Object item : array) {
                count++;
            }
            assertThat(count, equalTo(6));

            // Test toArray
            Object[] arrayContents = array.toArray();
            assertThat(arrayContents.length, equalTo(6));
            assertThat(arrayContents[0], equalTo(1));
            assertThat(arrayContents[3], equalTo("hello"));
            assertThat(arrayContents[5], nullValue());
        }
    }

    public void testNestedArrays() throws Exception {
        String jsonString = """
            {
                "matrix": [
                    [1, 2, 3],
                    [4, 5, 6],
                    [7, 8, 9]
                ],
                "jagged": [
                    [1],
                    [2, 3],
                    [4, 5, 6, 7]
                ]
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
            ESONSourceClaude.Builder builder = new ESONSourceClaude.Builder(false);
            ESONSourceClaude source = builder.parse(parser);
            ESONSourceClaude.ESONObject root = source.root();

            // Test matrix (regular 2D array)
            ESONSourceClaude.ESONArray matrix = (ESONSourceClaude.ESONArray) root.get("matrix");
            assertThat(matrix.size(), equalTo(3));

            ESONSourceClaude.ESONArray row1 = (ESONSourceClaude.ESONArray) matrix.get(0);
            assertThat(row1.get(0), equalTo(1));
            assertThat(row1.get(1), equalTo(2));
            assertThat(row1.get(2), equalTo(3));

            ESONSourceClaude.ESONArray row2 = (ESONSourceClaude.ESONArray) matrix.get(1);
            assertThat(row2.get(0), equalTo(4));
            assertThat(row2.get(1), equalTo(5));
            assertThat(row2.get(2), equalTo(6));

            // Test jagged array (different row sizes)
            ESONSourceClaude.ESONArray jagged = (ESONSourceClaude.ESONArray) root.get("jagged");
            assertThat(jagged.size(), equalTo(3));

            ESONSourceClaude.ESONArray jaggedRow1 = (ESONSourceClaude.ESONArray) jagged.get(0);
            assertThat(jaggedRow1.size(), equalTo(1));
            assertThat(jaggedRow1.get(0), equalTo(1));

            ESONSourceClaude.ESONArray jaggedRow2 = (ESONSourceClaude.ESONArray) jagged.get(1);
            assertThat(jaggedRow2.size(), equalTo(2));
            assertThat(jaggedRow2.get(0), equalTo(2));
            assertThat(jaggedRow2.get(1), equalTo(3));

            ESONSourceClaude.ESONArray jaggedRow3 = (ESONSourceClaude.ESONArray) jagged.get(2);
            assertThat(jaggedRow3.size(), equalTo(4));
            assertThat(jaggedRow3.get(3), equalTo(7));
        }
    }

    public void testArrayOfObjects() throws Exception {
        String jsonString = """
            {
                "items": [
                    {"id": 1, "name": "item1"},
                    {"id": 2, "name": "item2"},
                    {"id": 3, "name": "item3"}
                ]
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
            ESONSourceClaude.Builder builder = new ESONSourceClaude.Builder(false);
            ESONSourceClaude source = builder.parse(parser);
            ESONSourceClaude.ESONObject root = source.root();

            ESONSourceClaude.ESONArray items = (ESONSourceClaude.ESONArray) root.get("items");
            assertThat(items.size(), equalTo(3));

            // Test first item
            ESONSourceClaude.ESONObject item1 = (ESONSourceClaude.ESONObject) items.get(0);
            assertThat(item1.get("id"), equalTo(1));
            assertThat(item1.get("name"), equalTo("item1"));

            // Test second item
            ESONSourceClaude.ESONObject item2 = (ESONSourceClaude.ESONObject) items.get(1);
            assertThat(item2.get("id"), equalTo(2));
            assertThat(item2.get("name"), equalTo("item2"));

            // Test third item
            ESONSourceClaude.ESONObject item3 = (ESONSourceClaude.ESONObject) items.get(2);
            assertThat(item3.get("id"), equalTo(3));
            assertThat(item3.get("name"), equalTo("item3"));
        }
    }

    public void testArrayWithMixedNesting() throws Exception {
        String jsonString = """
            {
                "complex": [
                    1,
                    "string",
                    [1, 2, 3],
                    {"nested": "object"},
                    [
                        {"deep": "nesting"},
                        [4, 5, 6]
                    ],
                    null
                ]
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
            ESONSourceClaude.Builder builder = new ESONSourceClaude.Builder(false);
            ESONSourceClaude source = builder.parse(parser);
            ESONSourceClaude.ESONObject root = source.root();

            ESONSourceClaude.ESONArray complex = (ESONSourceClaude.ESONArray) root.get("complex");
            assertThat(complex.size(), equalTo(6));

            // Test primitive values
            assertThat(complex.get(0), equalTo(1));
            assertThat(complex.get(1), equalTo("string"));

            // Test nested array
            ESONSourceClaude.ESONArray nestedArray = (ESONSourceClaude.ESONArray) complex.get(2);
            assertThat(nestedArray.size(), equalTo(3));
            assertThat(nestedArray.get(0), equalTo(1));
            assertThat(nestedArray.get(1), equalTo(2));
            assertThat(nestedArray.get(2), equalTo(3));

            // Test nested object
            ESONSourceClaude.ESONObject nestedObject = (ESONSourceClaude.ESONObject) complex.get(3);
            assertThat(nestedObject.get("nested"), equalTo("object"));

            // Test deeply nested array
            ESONSourceClaude.ESONArray deepArray = (ESONSourceClaude.ESONArray) complex.get(4);
            assertThat(deepArray.size(), equalTo(2));

            ESONSourceClaude.ESONObject deepObject = (ESONSourceClaude.ESONObject) deepArray.get(0);
            assertThat(deepObject.get("deep"), equalTo("nesting"));

            ESONSourceClaude.ESONArray deepNestedArray = (ESONSourceClaude.ESONArray) deepArray.get(1);
            assertThat(deepNestedArray.get(0), equalTo(4));
            assertThat(deepNestedArray.get(1), equalTo(5));
            assertThat(deepNestedArray.get(2), equalTo(6));

            // Test null
            assertThat(complex.get(5), nullValue());
        }
    }

    public void testArrayEdgeCases() throws Exception {
        String jsonString = """
            {
                "onlyNulls": [null, null, null],
                "singleElement": [42],
                "alternatingNulls": [1, null, 2, null, 3],
                "emptyStrings": ["", "", ""],
                "booleans": [true, false, true, false]
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
            ESONSourceClaude.Builder builder = new ESONSourceClaude.Builder(false);
            ESONSourceClaude source = builder.parse(parser);
            ESONSourceClaude.ESONObject root = source.root();

            // Test array of only nulls
            ESONSourceClaude.ESONArray onlyNulls = (ESONSourceClaude.ESONArray) root.get("onlyNulls");
            assertThat(onlyNulls.size(), equalTo(3));
            assertThat(onlyNulls.get(0), nullValue());
            assertThat(onlyNulls.get(1), nullValue());
            assertThat(onlyNulls.get(2), nullValue());

            // Test single element array
            ESONSourceClaude.ESONArray singleElement = (ESONSourceClaude.ESONArray) root.get("singleElement");
            assertThat(singleElement.size(), equalTo(1));
            assertThat(singleElement.get(0), equalTo(42));

            // Test alternating nulls
            ESONSourceClaude.ESONArray alternating = (ESONSourceClaude.ESONArray) root.get("alternatingNulls");
            assertThat(alternating.size(), equalTo(5));
            assertThat(alternating.get(0), equalTo(1));
            assertThat(alternating.get(1), nullValue());
            assertThat(alternating.get(2), equalTo(2));
            assertThat(alternating.get(3), nullValue());
            assertThat(alternating.get(4), equalTo(3));

            // Test empty strings
            ESONSourceClaude.ESONArray emptyStrings = (ESONSourceClaude.ESONArray) root.get("emptyStrings");
            assertThat(emptyStrings.size(), equalTo(3));
            assertThat(emptyStrings.get(0), equalTo(""));
            assertThat(emptyStrings.get(1), equalTo(""));
            assertThat(emptyStrings.get(2), equalTo(""));

            // Test booleans
            ESONSourceClaude.ESONArray booleans = (ESONSourceClaude.ESONArray) root.get("booleans");
            assertThat(booleans.size(), equalTo(4));
            assertThat(booleans.get(0), equalTo(true));
            assertThat(booleans.get(1), equalTo(false));
            assertThat(booleans.get(2), equalTo(true));
            assertThat(booleans.get(3), equalTo(false));
        }
    }
}
