/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class NdJsonSchemaInferrerTests extends ESTestCase {

    private Attribute field(String name, DataType type, boolean nullable) {
        return new ReferenceAttribute(Source.EMPTY, null, name, type, nullable ? Nullability.TRUE : Nullability.UNKNOWN, null, false);
    }

    private Attribute field(String name, DataType type) {
        return field(name, type, false);
    }

    /**
     * Test case: Verifies the correct schema inference for lines containing valid flat JSON objects.
     */
    public void testInferSchemaForFlatJson() throws IOException {
        check("""
            {"name": "John", "age": 30}
            {"name": "Jane", "age": 25}
            """, field("name", DataType.KEYWORD), field("age", DataType.INTEGER));
    }

    /**
     * Test case: Verifies the schema inference properly handles nested JSON objects.
     */
    public void testInferSchemaForNestedJson() throws IOException {
        check("""
            {"user": {"name": "John", "age": 30, "long_value": 12345678901234}}
            {"user": {"name": "Jane", "age": 25}}
            """, field("user.name", DataType.KEYWORD), field("user.age", DataType.INTEGER), field("user.long_value", DataType.LONG, true));
    }

    /**
     * Test case: Ensures the method ignores empty lines and invalid JSON lines.
     */
    public void testIgnoreEmptyAndInvalidLines() throws IOException {
        check("""
            {"name": "John", "age": 30}
            not_json

            {"name": "Jane", "age": null}
            """, field("name", DataType.KEYWORD), field("age", DataType.INTEGER, true));
    }

    /**
     * Test case: check line ending variations
     */
    public void testLineEndingVariations() throws IOException {
        check(
            "{\"name\": \"John\", \"age\": 30}\nnot_json\n\n{\"name\": \"Jane\", \"age\": null}",
            field("name", DataType.KEYWORD),
            field("age", DataType.INTEGER, true)
        );

        check(
            "{\"name\": \"John\", \"age\": 30}\nnot_json\r\r{\"name\": \"Jane\", \"age\": null}",
            field("name", DataType.KEYWORD),
            field("age", DataType.INTEGER, true)
        );

        check(
            "{\"name\": \"John\", \"age\": 30}\nnot_json\r\n\n\r{\"name\": \"Jane\", \"age\": null}",
            field("name", DataType.KEYWORD),
            field("age", DataType.INTEGER, true)
        );
    }

    /**
     * Test case: Verifies the inference correctly handles arrays in JSON objects.
     */
    public void testInferSchemaForJsonWithArrays() throws IOException {
        check("""
            {"scores": [85, 90, 95]}
            {"scores": [70, null]}
            """, field("scores", DataType.INTEGER, true));
    }

    /**
     * Test case: Ensures correct schema inference when all values of a field are null.
     */
    public void testInferSchemaForNullFields() throws IOException {
        // "age" field ignored as it has no non-null value.
        check("""
            {"name": "John", "age": null}
            {"name": "Jane", "age": null}
            """, field("name", DataType.KEYWORD));
    }

    /**
     * Test case: Verifies schema inference respects the maxLines parameter.
     */
    public void testInferSchemaWithMaxLinesLimit() throws IOException {
        check("""
            {"name": "John", "age": 30}
            {"name": "Jane", "age": 25}
            {"name": "Smith", "age": 40}
            """, field("name", DataType.KEYWORD), field("age", DataType.INTEGER));
    }

    /**
     * Test case: Verifies the correct handling of mixed field types.
     */
    public void testInferSchemaForMixedTypeFields() throws IOException {
        check("""
            {"mixed": 42}
            {"mixed": "text"}
            {"mixed": 3.14}
            """, field("mixed", DataType.KEYWORD));
    }

    /**
     * Reproduces the exact repro from elastic/esql-planning#1028: a field that is a scalar in some sampled
     * records and a JSON object in others must resolve to exactly one shape (mirroring core ES dynamic
     * mapping's first-writer-wins), never both a scalar attribute and its object's nested children. Here the
     * scalar shape is observed first, so the later object record's shape is ignored for schema purposes (the
     * decoder applies {@code ErrorPolicy} to the actual conflicting value at read time).
     */
    public void testScalarThenObjectConflictResolvesToScalarShape() throws IOException {
        check("""
            {"event":1,"user":"alice"}
            {"event":2,"user":{"id":"bob","tier":"gold"}}
            {"event":3,"user":"carol"}
            """, field("event", DataType.INTEGER), field("user", DataType.KEYWORD, true));
    }

    /**
     * Mirror of {@link #testScalarThenObjectConflictResolvesToScalarShape}: when the object shape is observed
     * first, a later scalar value for the same field name must not resurrect a duplicate scalar attribute
     * alongside the already-committed nested children.
     */
    public void testObjectThenScalarConflictResolvesToObjectShape() throws IOException {
        check("""
            {"event":1,"user":{"id":"bob","tier":"gold"}}
            {"event":2,"user":"alice"}
            {"event":3,"user":{"id":"carol","tier":"silver"}}
            """, field("event", DataType.INTEGER), field("user.id", DataType.KEYWORD, true), field("user.tier", DataType.KEYWORD, true));
    }

    public void testDateTime() throws Exception {
        check("""
            {"timestamp": "2025-03-26T18:12:34Z"}
            {"timestamp": "2023-03-26"}
            """, field("timestamp", DataType.DATETIME));

        // Numbers aren't implicitly interpreted as timestamps.
        check("""
            {"timestamp": "2025-03-26T18:12:34Z"}
            {"timestamp": 1679854354000}
            """, field("timestamp", DataType.KEYWORD));
    }

    private void check(String ndjson, Attribute... expected) throws IOException {
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(ndjson.getBytes(StandardCharsets.UTF_8))) {
            List<Attribute> result = NdJsonSchemaInferrer.inferSchema(inputStream, 100, null);

            assertEquals(expected.length, result.size());
            for (int i = 0; i < expected.length; i++) {
                String name = result.get(i).name();
                assertEquals(name + " name", expected[i].name(), name);
                assertEquals(name + " type", expected[i].dataType(), result.get(i).dataType());
                assertEquals(name + " nullable", expected[i].nullable(), result.get(i).nullable());
            }
        }
    }
}
