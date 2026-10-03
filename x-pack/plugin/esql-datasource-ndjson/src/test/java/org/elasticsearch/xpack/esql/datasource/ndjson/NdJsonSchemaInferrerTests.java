/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.datasources.spi.HeapEstimates;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

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
     * A record {@code depth} levels deep whose innermost object holds {@code leaves} keys. Inference flattens it into
     * {@code leaves} columns each named by the whole dotted path, so the schema is roughly {@code leaves * depth * 2}
     * characters from an input of only {@code depth * 5 + leaves * 10} bytes.
     */
    private static String deeplyNestedRecord(int depth, int leaves) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"a\":".repeat(depth));
        sb.append('{');
        for (int i = 0; i < leaves; i++) {
            sb.append(i == 0 ? "" : ",").append("\"k").append(i).append("\":1");
        }
        sb.append('}');
        sb.append("}".repeat(depth));
        return sb.append('\n').toString();
    }

    private static String wideFlatRecord(int columns) {
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < columns; i++) {
            sb.append(i == 0 ? "" : ",").append("\"column_").append(i).append("\":1");
        }
        return sb.append("}\n").toString();
    }

    /** No field cap, so only the breaker can refuse: these records are deliberately far wider than the default cap. */
    private static List<Attribute> infer(String ndjson, LimitedBreaker breaker) throws IOException {
        return infer(ndjson, Integer.MAX_VALUE, breaker);
    }

    private static List<Attribute> infer(String ndjson, int maxFields, LimitedBreaker breaker) throws IOException {
        try (ByteArrayInputStream in = new ByteArrayInputStream(ndjson.getBytes(StandardCharsets.UTF_8))) {
            return NdJsonSchemaInferrer.inferSchema(in, 100, maxFields, null, breaker);
        }
    }

    /** Counts refusals so a test can tell one charge-and-refuse from a retry per record. */
    private static class CountingLimitedBreaker extends LimitedBreaker {
        final AtomicInteger trips = new AtomicInteger();

        CountingLimitedBreaker(long maxBytes) {
            super("test", ByteSizeValue.ofBytes(maxBytes));
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            try {
                super.addEstimateBytesAndMaybeBreak(bytes, label);
            } catch (CircuitBreakingException e) {
                trips.incrementAndGet();
                throw e;
            }
        }
    }

    /**
     * Esql-planning#2143: a flat record with many distinct keys is refused rather than inferred without a charge.
     */
    public void testWideFlatRecordTripsBreaker() {
        LimitedBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofKb(100));
        expectThrows(CircuitBreakingException.class, () -> infer(wideFlatRecord(5_000), breaker));
        assertThat("a refused inference releases everything it reserved", breaker.getUsed(), equalTo(0L));
    }

    /**
     * Esql-planning#2143: the repro shape. Only about a thousand nodes are alive, so the field tree is cheap and this
     * trips on the dotted names built from it. 900 levels and 100 leaves is ~380 KB of names against a tree of ~300 KB:
     * a limit between the two admits the tree and refuses the columns, so the column charge alone is what trips.
     */
    public void testDeeplyNestedRecordTripsOnColumnNamesNotOnTheFieldTree() throws IOException {
        int depth = 900;
        int leaves = 100;
        String record = deeplyNestedRecord(depth, leaves);
        ByteSizeValue limit = ByteSizeValue.ofKb(500);
        // The root, one node per level and one per leaf, each with its name.
        long tree = (1L + depth + leaves) * NdJsonSchemaInferrer.FIELD_INFO_BYTES + HeapEstimates.stringBytes((String) null) + depth
            * HeapEstimates.stringBytes("a");
        for (int i = 0; i < leaves; i++) {
            tree += HeapEstimates.stringBytes("k" + i);
        }
        assertThat("the field tree alone must fit, so only the column charge can trip", tree, lessThan(limit.getBytes()));

        LimitedBreaker breaker = new LimitedBreaker("test", limit);
        expectThrows(CircuitBreakingException.class, () -> infer(record, breaker));
        assertThat(breaker.getUsed(), equalTo(0L));

        // The same record fits when the breaker has headroom for the names, so the refusal above was theirs.
        LimitedBreaker roomy = new LimitedBreaker("test", ByteSizeValue.ofMb(4));
        assertThat(infer(record, roomy).size(), equalTo(leaves));
        assertThat(roomy.getUsed(), equalTo(0L));
    }

    public void testNothingIsLeftReservedAfterSuccess() throws IOException {
        LimitedBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofMb(64));
        assertThat(infer(wideFlatRecord(1_000), breaker).size(), equalTo(1_000));
        assertThat("the returned schema belongs to the caller, not to inference", breaker.getUsed(), equalTo(0L));
    }

    /**
     * A refusal is not a malformed line. It must stop inference at once instead of being skipped like a bad record
     * and retried on the next one, which would charge and refuse once per remaining record.
     */
    public void testRefusalStopsInferenceWithoutRetryingTheNextRecord() {
        String malformed = "not_json\n";
        String ndjson = malformed + wideFlatRecord(5_000) + wideFlatRecord(5_000) + wideFlatRecord(5_000);
        CountingLimitedBreaker breaker = new CountingLimitedBreaker(ByteSizeValue.ofKb(100).getBytes());
        expectThrows(CircuitBreakingException.class, () -> infer(ndjson, breaker));
        assertThat(breaker.trips.get(), equalTo(1));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /**
     * An empty first segment is still a parent, so its children keep the separator. {@code ".b"} is a column of its own
     * and must not merge into, or share a name with, {@code "b"}.
     */
    public void testEmptyFirstSegmentKeepsLeadingDot() throws IOException {
        check("{\".b\":1,\"b\":\"x\"}\n", field(".b", DataType.INTEGER), field("b", DataType.KEYWORD));
        check("{\"\":{\"b\":1}}\n", field(".b", DataType.INTEGER));
    }

    /** Records the most it held at once, so a test can read what an inference charged before releasing it. */
    private static class PeakTrackingLimitedBreaker extends LimitedBreaker {
        long peak;

        PeakTrackingLimitedBreaker(ByteSizeValue max) {
            super("test", max);
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            super.addEstimateBytesAndMaybeBreak(bytes, label);
            peak = Math.max(peak, getUsed());
        }
    }

    /**
     * Same column count and same leaf keys; only the one parent key differs, so the field-tree charge differs by a
     * single node's name while every column's dotted name grows by 499 characters. The difference in what was held
     * must therefore include the column-name charge, not just the one longer node name.
     */
    public void testChargesGrowWithColumnNameLength() throws IOException {
        int columns = 200;
        String[] parents = { "p", "p".repeat(500) };
        long[] peak = new long[parents.length];
        for (int i = 0; i < parents.length; i++) {
            StringBuilder sb = new StringBuilder("{\"").append(parents[i]).append("\":{");
            for (int c = 0; c < columns; c++) {
                sb.append(c == 0 ? "" : ",").append("\"c").append(c).append("\":1");
            }
            PeakTrackingLimitedBreaker breaker = new PeakTrackingLimitedBreaker(ByteSizeValue.ofMb(16));
            assertThat(infer(sb.append("}}\n").toString(), breaker).size(), equalTo(columns));
            peak[i] = breaker.peak;
        }
        assertThat(peak[1] - peak[0], greaterThanOrEqualTo(columns * 499L * Character.BYTES));
    }

    /**
     * A dotted key is split into one node per segment, which Jackson's nesting cap does not bound. A ~50 KB key of
     * 25,000 segments must infer its one column without overflowing the stack, and every byte is released after.
     */
    public void testVeryDeepDottedKeyDoesNotOverflowTheStack() throws IOException {
        String key = "a.".repeat(24_999) + "b";
        LimitedBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofMb(64));
        List<Attribute> schema = infer("{\"" + key + "\":1}\n", breaker);
        assertThat(schema.size(), equalTo(1));
        assertThat(schema.get(0).name(), equalTo(key));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /**
     * The shared path buffer is charged by the longest path it spells, on top of the field tree and the column, so a
     * deep chain of objects with one leaf is held against the breaker twice for its path: once as the buffer and once
     * as the column name built from it.
     */
    public void testObjectPathIsChargedWhileSpelled() throws IOException {
        String key = "a.".repeat(2_000) + "b";
        PeakTrackingLimitedBreaker breaker = new PeakTrackingLimitedBreaker(ByteSizeValue.ofMb(64));
        infer("{\"" + key + "\":1}\n", breaker);
        long nodes = 2_001L + 1; // one per segment, plus the root
        long treeAndColumn = nodes * NdJsonSchemaInferrer.FIELD_INFO_BYTES + 2_000 * HeapEstimates.stringBytes("a") + HeapEstimates
            .stringBytes("b") + HeapEstimates.stringBytes((String) null) + HeapEstimates.columnBytes(key.length());
        assertThat(breaker.peak - treeAndColumn, equalTo((long) key.length() * Character.BYTES));
    }

    /** The cap admits exactly {@code maxFields} fields and refuses the next one as a client error, releasing everything. */
    public void testFieldCapAdmitsExactlyTheLimit() throws IOException {
        int maxFields = between(1, 50);
        LimitedBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofMb(16));
        assertThat(infer(wideFlatRecord(maxFields), maxFields, breaker).size(), equalTo(maxFields));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> infer(wideFlatRecord(maxFields + 1), maxFields, breaker)
        );
        assertThat(e.getMessage(), containsString("more than [" + maxFields + "] fields"));
        assertThat(e.getMessage(), containsString(NdJsonFormatReader.CONFIG_SCHEMA_MAX_FIELDS));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /**
     * Objects count like leaves, as in {@code index.mapping.total_fields.limit}, and so does every segment of a dotted
     * key: {@code {"a":{"b":1}}} and {@code {"a.b":1}} are both two fields.
     */
    public void testFieldCapCountsObjectsAndDottedSegments() throws IOException {
        LimitedBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofMb(16));
        for (String record : List.of("{\"a\":{\"b\":1}}\n", "{\"a.b\":1}\n")) {
            assertThat(infer(record, 2, breaker).size(), equalTo(1));
            expectThrows(IllegalArgumentException.class, () -> infer(record, 1, breaker));
        }
        // The esql-planning#2143 shape (36,000 leaves under deep nesting) is refused by the default cap at its 101st
        // leaf, long before the dotted names are built.
        expectThrows(
            IllegalArgumentException.class,
            () -> infer(deeplyNestedRecord(900, 36_000), ExternalSourceSettings.DEFAULT_SCHEMA_MAX_FIELDS, breaker)
        );
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /**
     * The cap is not a malformed line: a later record that pushes the schema over it fails inference rather than
     * being skipped, so the earlier records' narrower schema is never returned as if it were complete.
     */
    public void testFieldCapIsNotSkippedAsAMalformedLine() {
        String ndjson = "not_json\n" + wideFlatRecord(3) + wideFlatRecord(5);
        expectThrows(IllegalArgumentException.class, () -> infer(ndjson, 4, new LimitedBreaker("test", ByteSizeValue.ofMb(16))));
    }

    /**
     * A malformed line is deferred to the slice read, so the fields it created before its error are discarded: they
     * neither become columns nor count toward the cap, even when that line alone crosses it.
     */
    public void testMalformedLineFieldsAreDiscarded() throws IOException {
        LimitedBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofMb(16));
        String duplicateKey = "{\"k0\":1,\"k1\":1,\"k2\":1,\"k3\":1,\"k4\":1,\"k5\":1,\"k0\":2}\n";
        String ndjson = "{\"a\":1}\n" + duplicateKey + "{\"a\":2}\n";
        for (int maxFields : List.of(Integer.MAX_VALUE, 5)) {
            List<Attribute> schema = infer(ndjson, maxFields, breaker);
            assertThat(schema.stream().map(Attribute::name).toList(), equalTo(List.of("a")));
            assertThat(breaker.getUsed(), equalTo(0L));
        }
        // A nested object created by the malformed line goes too, leaving its pre-existing parent a leaf.
        assertThat(
            infer("{\"a\":1}\n{\"a\":{\"b\":1,\"b\":2}}\n", 5, breaker).stream().map(Attribute::name).toList(),
            equalTo(List.of("a"))
        );
        // A record cut short by the end of the stream is malformed too.
        assertThat(
            infer("{\"a\":1}\n{\"k0\":1,\"k1\":1,\"k2\":1,\"k3\":1,\"k4\":1,\"k5\":1", 5, breaker).stream().map(Attribute::name).toList(),
            equalTo(List.of("a"))
        );
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /** A well-formed line that crosses the cap still fails inference, however much of it follows the crossing. */
    public void testWellFormedLineCrossingTheCapMidRecordFails() {
        String ndjson = "{\"a\":1}\n{\"k0\":1,\"k1\":{\"x\":[1,{\"y\":2}]},\"k2\":1,\"k3\":1,\"k4\":1,\"k5\":1}\n";
        LimitedBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofMb(16));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> infer(ndjson, 3, breaker));
        assertThat(e.getMessage(), containsString("more than [3] fields"));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    private void check(String ndjson, Attribute... expected) throws IOException {
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(ndjson.getBytes(StandardCharsets.UTF_8))) {
            List<Attribute> result = NdJsonSchemaInferrer.inferSchema(
                inputStream,
                100,
                ExternalSourceSettings.DEFAULT_SCHEMA_MAX_FIELDS,
                null,
                new NoopCircuitBreaker("test")
            );

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
