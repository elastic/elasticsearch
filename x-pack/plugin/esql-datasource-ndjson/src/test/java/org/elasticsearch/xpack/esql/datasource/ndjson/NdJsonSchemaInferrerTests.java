/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.common.time.DateFormatter;
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

    /**
     * A line that trips one of Jackson's {@code StreamReadConstraints} limits is skipped by the sampling pass
     * exactly as a malformed line is, and the lines around it still shape the schema. Inference is best-effort
     * and policy-independent: failing it would kill the query before {@code error_mode} could decide anything,
     * even under {@code skip_row}. The bad line here carries a field the good lines do not, so the assertion
     * fails if the sampler had actually consumed it.
     */
    public void testStreamConstraintViolationSkippedDuringInference() throws IOException {
        String ndjson = "{\"name\": \"John\", \"age\": 30}\n"
            + "{\"name\": \"Bad\", \"age\": "
            + "1".repeat(1200)
            + ", \"only_on_bad_line\": true}\n"
            + "{\"name\": \"Jane\", \"age\": 25}\n";
        // `age` comes back nullable because the abandoned line had already contributed `name` before the
        // scanner threw, so `age` counts as unseen for that round. That is the pre-existing consequence of a
        // partially-consumed line and is identical for an ordinary malformed line — the point here is that
        // inference completes at all, and that `only_on_bad_line` never enters the schema.
        check(ndjson, field("name", DataType.KEYWORD), field("age", DataType.INTEGER, true));
    }

    /**
     * The sampling loop guards two call sites, and the two tests around this one both land on
     * {@code inferObjectSchema}. A bare oversized token on its own line is scanned by the {@code nextToken}
     * that opens a record, which is the other one. That arm {@code continue}s before the mark-unseen-nullable
     * sweep, so unlike its siblings the surviving columns stay non-nullable — which is also what proves the
     * skipped line was abandoned at the top of the loop rather than part-way through a record.
     */
    public void testConstraintViolationOnRecordOpeningTokenSkippedDuringInference() throws IOException {
        String ndjson = "{\"name\": \"John\", \"age\": 30}\n" + "1".repeat(1200) + "\n{\"name\": \"Jane\", \"age\": 25}\n";
        check(ndjson, field("name", DataType.KEYWORD), field("age", DataType.INTEGER));
    }

    /** The same skip for the name-length limit, which trips in a different scanner call than the number limit. */
    public void testOversizedFieldNameSkippedDuringInference() throws IOException {
        String ndjson = "{\"name\": \"John\", \"age\": 30}\n"
            + "{\""
            + "n".repeat(60_000)
            + "\": 1}\n"
            + "{\"name\": \"Jane\", \"age\": 25}\n";
        // The bad line contributes no field at all before throwing, so both columns are unseen for that round
        // and come back nullable — again the pre-existing partially-consumed-line behavior, not a new effect.
        check(ndjson, field("name", DataType.KEYWORD, true), field("age", DataType.INTEGER, true));
    }

    public void testNanosecondTimestampInfersDateNanos() throws IOException {
        check("""
            {"ts": "2023-10-23T12:15:03.360103847Z"}
            """, field("ts", DataType.DATE_NANOS));
    }

    public void testMixedPrecisionWidensToDateNanos() throws IOException {
        // Either order: the field accumulates both types and resolution widens to the one that can
        // hold both. Reading a millisecond string on the nanos rail is lossless.
        check("""
            {"ts": "2023-10-23T12:15:03.360Z"}
            {"ts": "2023-10-23T12:15:03.360103847Z"}
            """, field("ts", DataType.DATE_NANOS));
        check("""
            {"ts": "2023-10-23T12:15:03.360103847Z"}
            {"ts": "2023-10-23T12:15:03.360Z"}
            """, field("ts", DataType.DATE_NANOS));
    }

    public void testTrailingZeroFractionStaysDatetime() throws IOException {
        // Nine digits of text, but millisecond-exact as a value: datetime reads it without loss, so
        // there is no reason to retype the column.
        check("""
            {"ts": "2023-10-23T12:15:03.360000000Z"}
            """, field("ts", DataType.DATETIME));
    }

    public void testPreEpochNanosecondStaysDatetime() throws IOException {
        // date_nanos cannot represent anything before the epoch at all.
        check("""
            {"ts": "1969-12-31T23:59:59.999999999Z"}
            """, field("ts", DataType.DATETIME));
    }

    public void testPostWindowNanosecondStaysDatetime() throws IOException {
        check("""
            {"ts": "2263-01-01T00:00:00.123456789Z"}
            """, field("ts", DataType.DATETIME));
    }

    public void testNanosMixedWithNonTemporalStringResolvesKeyword() throws IOException {
        check("""
            {"ts": "2023-10-23T12:15:03.360103847Z"}
            {"ts": "not a date"}
            """, field("ts", DataType.KEYWORD));
    }

    public void testCustomDatetimeFormatNeverInfersDateNanos() throws IOException {
        // The pattern below happily parses the nanosecond fraction, so this is not about parse
        // failure: a declared dialect means the user has said how their timestamps are written, and
        // declaring the schema is the way to ask for nanoseconds.
        DateFormatter custom = DateFormatter.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
        String ndjson = """
            {"ts": "2023-10-23 12:15:03.360103847"}
            """;
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(ndjson.getBytes(StandardCharsets.UTF_8))) {
            List<Attribute> result = NdJsonSchemaInferrer.inferSchema(inputStream, 100, custom);
            assertEquals(1, result.size());
            assertEquals(DataType.DATETIME, result.get(0).dataType());
        }
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
