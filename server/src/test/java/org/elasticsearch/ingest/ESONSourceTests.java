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
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

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
            ESONSource.Builder builder = new ESONSource.Builder();
            ESONSource.ESONObject source = builder.parse(parser);

            // Test integer
            assertThat(source.get("intField"), equalTo(42));
            assertThat(source.get("intField"), instanceOf(Integer.class));

            // Test long
            assertThat(source.get("longField"), equalTo(9223372036854775807L));
            assertThat(source.get("longField"), instanceOf(Long.class));

            // Test float
            // Jackson always parses as Double
            assertThat((Double) source.get("floatField"), equalTo(3.14));
            assertThat(source.get("floatField"), instanceOf(Double.class));

            // Test double
            assertThat((Double) source.get("doubleField"), equalTo(2.718281828459045));
            assertThat(source.get("doubleField"), instanceOf(Double.class));

            // Test string
            assertThat(source.get("stringField"), equalTo("hello world"));
            assertThat(source.get("stringField"), instanceOf(String.class));

            // Test boolean
            assertThat(source.get("booleanField"), equalTo(true));
            assertThat(source.get("booleanField"), instanceOf(Boolean.class));

            // Test null
            assertThat(source.get("nullField"), nullValue());
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
            ESONSource.Builder builder = new ESONSource.Builder();
            ESONSource.ESONObject source = builder.parse(parser);

            // Test mixed array using List<Object> interface
            ESONSource.ESONArray mixedArray = (ESONSource.ESONArray) source.get("mixedArray");
            assertThat(mixedArray.size(), equalTo(5));
            assertThat(mixedArray.get(0), equalTo(1));
            assertThat(mixedArray.get(1), equalTo("string"));
            assertThat(mixedArray.get(2), equalTo(true));
            assertThat(mixedArray.get(3), nullValue());
            // Jackson always parses as Double
            assertThat((Double) mixedArray.get(4), equalTo(3.14));

            // Test number array
            ESONSource.ESONArray numberArray = (ESONSource.ESONArray) source.get("numberArray");
            assertThat(numberArray.size(), equalTo(3));
            assertThat(numberArray.get(0), equalTo(10));
            assertThat(numberArray.get(1), equalTo(20));
            assertThat(numberArray.get(2), equalTo(30));

            // Test string array
            ESONSource.ESONArray stringArray = (ESONSource.ESONArray) source.get("stringArray");
            assertThat(stringArray.size(), equalTo(3));
            assertThat(stringArray.get(0), equalTo("a"));
            assertThat(stringArray.get(1), equalTo("b"));
            assertThat(stringArray.get(2), equalTo("c"));

            // Test empty array
            ESONSource.ESONArray emptyArray = (ESONSource.ESONArray) source.get("emptyArray");
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
            ESONSource.Builder builder = new ESONSource.Builder();
            ESONSource.ESONObject source = builder.parse(parser);

            // Test nested object
            ESONSource.ESONObject user = (ESONSource.ESONObject) source.get("user");
            assertThat(user.get("name"), equalTo("John Doe"));
            assertThat(user.get("age"), equalTo(30));
            assertThat(user.get("active"), equalTo(true));

            // Test deeply nested object
            ESONSource.ESONObject address = (ESONSource.ESONObject) user.get("address");
            assertThat(address.get("street"), equalTo("123 Main St"));
            assertThat(address.get("city"), equalTo("Springfield"));
            assertThat(address.get("zipcode"), equalTo("12345"));

            // Test empty object
            ESONSource.ESONObject emptyObject = (ESONSource.ESONObject) source.get("emptyObject");
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
            ESONSource.Builder builder = new ESONSource.Builder();
            ESONSource.ESONObject source = builder.parse(parser);

            // Test metadata object
            ESONSource.ESONObject metadata = (ESONSource.ESONObject) source.get("metadata");
            assertThat(metadata.get("version"), equalTo(1));
            assertThat(metadata.get("created"), equalTo("2024-01-01"));

            // Test users array
            ESONSource.ESONArray users = (ESONSource.ESONArray) source.get("users");
            assertThat(users.size(), equalTo(2));

            // Test first user
            ESONSource.ESONObject user1 = (ESONSource.ESONObject) users.get(0);
            assertThat(user1.get("id"), equalTo(1));
            assertThat(user1.get("name"), equalTo("Alice"));

            ESONSource.ESONArray scores1 = (ESONSource.ESONArray) user1.get("scores");
            assertThat(scores1.size(), equalTo(3));

            // Test config object
            ESONSource.ESONObject config = (ESONSource.ESONObject) source.get("config");
            assertThat(config.get("enabled"), equalTo(true));
            assertThat(config.get("maxUsers"), equalTo(100));

            ESONSource.ESONArray features = (ESONSource.ESONArray) config.get("features");
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
            ESONSource.Builder builder = new ESONSource.Builder();
            ESONSource.ESONObject source = builder.parse(parser);

            // Test Map interface methods
            assertThat(source.size(), equalTo(4));
            assertThat(source.isEmpty(), equalTo(false));
            assertThat(source.containsKey("field1"), equalTo(true));
            assertThat(source.containsKey("nonexistent"), equalTo(false));

            // Test keySet
            assertThat(source.keySet().size(), equalTo(4));
            assertThat(source.keySet().contains("field1"), equalTo(true));
            assertThat(source.keySet().contains("field2"), equalTo(true));
            assertThat(source.keySet().contains("field3"), equalTo(true));
            assertThat(source.keySet().contains("field4"), equalTo(true));

            // Test values
            assertThat(source.values().size(), equalTo(4));

            // Test entrySet
            assertThat(source.entrySet().size(), equalTo(4));
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
            ESONSource.Builder builder = new ESONSource.Builder();
            ESONSource.ESONObject source = builder.parse(parser);

            assertThat(source.get("maxInt"), equalTo(Integer.MAX_VALUE));
            assertThat(source.get("minInt"), equalTo(Integer.MIN_VALUE));
            assertThat(source.get("maxLong"), equalTo(Long.MAX_VALUE));
            assertThat(source.get("minLong"), equalTo(Long.MIN_VALUE));
            assertThat(source.get("zero"), equalTo(0));
            // Jackson always parses as Double
            assertThat((Double) source.get("negativeFloat"), equalTo(-3.14));
            assertThat((Double) source.get("scientificNotation"), equalTo(1.23e-4));
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
            ESONSource.Builder builder = new ESONSource.Builder();
            ESONSource.ESONObject source = builder.parse(parser);

            assertThat(source.get("emptyString"), equalTo(""));
            assertThat(source.get("nullValue"), nullValue());

            ESONSource.ESONArray emptyArray = (ESONSource.ESONArray) source.get("emptyArray");
            assertThat(emptyArray.size(), equalTo(0));

            ESONSource.ESONObject emptyObject = (ESONSource.ESONObject) source.get("emptyObject");
            assertThat(emptyObject.size(), equalTo(0));

            ESONSource.ESONArray arrayWithNulls = (ESONSource.ESONArray) source.get("arrayWithNulls");
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
            ESONSource.Builder builder = new ESONSource.Builder();
            ESONSource.ESONObject source = builder.parse(parser);

            ESONSource.ESONArray array = (ESONSource.ESONArray) source.get("testArray");

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
            ESONSource.Builder builder = new ESONSource.Builder();
            ESONSource.ESONObject source = builder.parse(parser);

            // Test matrix (regular 2D array)
            ESONSource.ESONArray matrix = (ESONSource.ESONArray) source.get("matrix");
            assertThat(matrix.size(), equalTo(3));

            ESONSource.ESONArray row1 = (ESONSource.ESONArray) matrix.get(0);
            assertThat(row1.get(0), equalTo(1));
            assertThat(row1.get(1), equalTo(2));
            assertThat(row1.get(2), equalTo(3));

            ESONSource.ESONArray row2 = (ESONSource.ESONArray) matrix.get(1);
            assertThat(row2.get(0), equalTo(4));
            assertThat(row2.get(1), equalTo(5));
            assertThat(row2.get(2), equalTo(6));

            // Test jagged array (different row sizes)
            ESONSource.ESONArray jagged = (ESONSource.ESONArray) source.get("jagged");
            assertThat(jagged.size(), equalTo(3));

            ESONSource.ESONArray jaggedRow1 = (ESONSource.ESONArray) jagged.get(0);
            assertThat(jaggedRow1.size(), equalTo(1));
            assertThat(jaggedRow1.get(0), equalTo(1));

            ESONSource.ESONArray jaggedRow2 = (ESONSource.ESONArray) jagged.get(1);
            assertThat(jaggedRow2.size(), equalTo(2));
            assertThat(jaggedRow2.get(0), equalTo(2));
            assertThat(jaggedRow2.get(1), equalTo(3));

            ESONSource.ESONArray jaggedRow3 = (ESONSource.ESONArray) jagged.get(2);
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
            ESONSource.Builder builder = new ESONSource.Builder();
            ESONSource.ESONObject source = builder.parse(parser);

            ESONSource.ESONArray items = (ESONSource.ESONArray) source.get("items");
            assertThat(items.size(), equalTo(3));

            // Test first item
            ESONSource.ESONObject item1 = (ESONSource.ESONObject) items.get(0);
            assertThat(item1.get("id"), equalTo(1));
            assertThat(item1.get("name"), equalTo("item1"));

            // Test second item
            ESONSource.ESONObject item2 = (ESONSource.ESONObject) items.get(1);
            assertThat(item2.get("id"), equalTo(2));
            assertThat(item2.get("name"), equalTo("item2"));

            // Test third item
            ESONSource.ESONObject item3 = (ESONSource.ESONObject) items.get(2);
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
            ESONSource.Builder builder = new ESONSource.Builder();
            ESONSource.ESONObject source = builder.parse(parser);

            ESONSource.ESONArray complex = (ESONSource.ESONArray) source.get("complex");
            assertThat(complex.size(), equalTo(6));

            // Test primitive values
            assertThat(complex.get(0), equalTo(1));
            assertThat(complex.get(1), equalTo("string"));

            // Test nested array
            ESONSource.ESONArray nestedArray = (ESONSource.ESONArray) complex.get(2);
            assertThat(nestedArray.size(), equalTo(3));
            assertThat(nestedArray.get(0), equalTo(1));
            assertThat(nestedArray.get(1), equalTo(2));
            assertThat(nestedArray.get(2), equalTo(3));

            // Test nested object
            ESONSource.ESONObject nestedObject = (ESONSource.ESONObject) complex.get(3);
            assertThat(nestedObject.get("nested"), equalTo("object"));

            // Test deeply nested array
            ESONSource.ESONArray deepArray = (ESONSource.ESONArray) complex.get(4);
            assertThat(deepArray.size(), equalTo(2));

            ESONSource.ESONObject deepObject = (ESONSource.ESONObject) deepArray.get(0);
            assertThat(deepObject.get("deep"), equalTo("nesting"));

            ESONSource.ESONArray deepNestedArray = (ESONSource.ESONArray) deepArray.get(1);
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
            ESONSource.Builder builder = new ESONSource.Builder();
            ESONSource.ESONObject source = builder.parse(parser);

            // Test array of only nulls
            ESONSource.ESONArray onlyNulls = (ESONSource.ESONArray) source.get("onlyNulls");
            assertThat(onlyNulls.size(), equalTo(3));
            assertThat(onlyNulls.get(0), nullValue());
            assertThat(onlyNulls.get(1), nullValue());
            assertThat(onlyNulls.get(2), nullValue());

            // Test single element array
            ESONSource.ESONArray singleElement = (ESONSource.ESONArray) source.get("singleElement");
            assertThat(singleElement.size(), equalTo(1));
            assertThat(singleElement.get(0), equalTo(42));

            // Test alternating nulls
            ESONSource.ESONArray alternating = (ESONSource.ESONArray) source.get("alternatingNulls");
            assertThat(alternating.size(), equalTo(5));
            assertThat(alternating.get(0), equalTo(1));
            assertThat(alternating.get(1), nullValue());
            assertThat(alternating.get(2), equalTo(2));
            assertThat(alternating.get(3), nullValue());
            assertThat(alternating.get(4), equalTo(3));

            // Test empty strings
            ESONSource.ESONArray emptyStrings = (ESONSource.ESONArray) source.get("emptyStrings");
            assertThat(emptyStrings.size(), equalTo(3));
            assertThat(emptyStrings.get(0), equalTo(""));
            assertThat(emptyStrings.get(1), equalTo(""));
            assertThat(emptyStrings.get(2), equalTo(""));

            // Test booleans
            ESONSource.ESONArray booleans = (ESONSource.ESONArray) source.get("booleans");
            assertThat(booleans.size(), equalTo(4));
            assertThat(booleans.get(0), equalTo(true));
            assertThat(booleans.get(1), equalTo(false));
            assertThat(booleans.get(2), equalTo(true));
            assertThat(booleans.get(3), equalTo(false));
        }
    }
}
