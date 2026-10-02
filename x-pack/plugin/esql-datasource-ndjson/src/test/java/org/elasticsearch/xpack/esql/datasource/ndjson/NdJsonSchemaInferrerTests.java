/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.HeapEstimates;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

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

    public void testInferredOrderFollowsFirstAppearance() throws IOException {
        check("""
            {"ts": "2026-01-01T00:00:00Z", "error_code": 7, "level": "INFO"}
            {"ts": "2026-01-01T00:00:01Z", "error_code": 3, "level": "WARN"}
            """, field("ts", DataType.DATETIME), field("error_code", DataType.INTEGER), field("level", DataType.KEYWORD));
        check("""
            {"ts": "2026-01-01T00:00:02Z", "level": "INFO"}
            {"ts": "2026-01-01T00:00:03Z", "error_code": 5, "level": "ERROR"}
            """, field("ts", DataType.DATETIME), field("level", DataType.KEYWORD), field("error_code", DataType.INTEGER, true));
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
     * A dot is a literal character in a column name, so a scalar {@code user} and an object {@code user} are not the
     * same field: the object flattens to {@code user.id}/{@code user.tier} and coexists with the scalar. Both are
     * nullable because neither shape appears in every record.
     */
    public void testScalarAndObjectCoexist() throws IOException {
        check(
            """
                {"event":1,"user":"alice"}
                {"event":2,"user":{"id":"bob","tier":"gold"}}
                {"event":3,"user":"carol"}
                """,
            field("event", DataType.INTEGER),
            field("user", DataType.KEYWORD, true),
            field("user.id", DataType.KEYWORD, true),
            field("user.tier", DataType.KEYWORD, true)
        );
    }

    /**
     * Shape order does not matter: object first still coexists with the later scalar. {@code user} precedes its dotted
     * siblings because the object record already claimed that name as a node, and the scalar observed later fills that
     * same slot rather than appending a new one.
     */
    public void testObjectAndScalarCoexist() throws IOException {
        check(
            """
                {"event":1,"user":{"id":"bob","tier":"gold"}}
                {"event":2,"user":"alice"}
                {"event":3,"user":{"id":"carol","tier":"silver"}}
                """,
            field("event", DataType.INTEGER),
            field("user", DataType.KEYWORD, true),
            field("user.id", DataType.KEYWORD, true),
            field("user.tier", DataType.KEYWORD, true)
        );
    }

    /** Both spellings of a dotted column ({@code "user.id"} flat and {@code {"user":{"id":...}}} nested) are one column. */
    public void testDottedKeyAndNestedObjectAreOneColumn() throws IOException {
        check("""
            {"user.id":"alice"}
            {"user":{"id":"bob"}}
            """, field("user.id", DataType.KEYWORD));
    }

    /**
     * An empty field name is a legal JSON name, so it is an ordinary segment: it composes to a column name with an
     * empty segment, and the flat spelling of that name unifies onto the same column the way any dotted name does.
     * {@link NdJsonIngestParityTests} pins that these columns are then actually filled.
     */
    public void testEmptyFieldNameIsAnOrdinarySegment() throws IOException {
        check("""
            {"a":{"":1},"b":{"":{"c":2}},"keep":3}
            """, field("a.", DataType.INTEGER), field("b..c", DataType.INTEGER), field("keep", DataType.INTEGER));
        check("""
            {"":1,"x":2}
            """, field("", DataType.INTEGER), field("x", DataType.INTEGER));
        // The flat spelling of the same column, so one column rather than two attributes with one name.
        check("""
            {"a":{"":1}}
            {"a.":2}
            """, field("a.", DataType.INTEGER));
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

    /**
     * A bare JSON number on its own line must not cause the following record to be dropped from the
     * inference sample (elastic/esql-planning#1731). The record after the bare number is the only
     * one that carries {@code email}, so if it is silently skipped the inferred schema omits it.
     * <p>
     * The bug: {@code nextToken()} succeeds (returning {@code VALUE_NUMBER_INT}), then
     * {@code inferObjectSchema} throws because the token is not {@code START_OBJECT}. By that point
     * Jackson has consumed the line terminator as a lookahead byte, leaving the parser positioned at
     * the start of the following record. The old {@code moveToNextLine} call then scanned forward and
     * consumed the following record through its own terminator, silently dropping it.
     */
    public void testBareNumberDropsSelfDuringInference() throws IOException {
        String ndjson = "{\"name\":\"John\"}\n42\n{\"email\":\"jane@x.com\"}\n";
        check(ndjson, field("name", DataType.KEYWORD, true), field("email", DataType.KEYWORD, true));
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
            List<Attribute> result = NdJsonSchemaInferrer.inferSchema(inputStream, 100, custom, new NoopCircuitBreaker("test"));
            assertEquals(1, result.size());
            assertEquals(DataType.DATETIME, result.get(0).dataType());
        }
    }

    public void testFourDigitStringIsNotADatetime() throws IOException {
        // strict_date_optional_time accepts a bare 4-digit year, which would make any all-4-digit
        // string column look temporal. The filter that prevents that lives on the same path the
        // nanosecond discriminator now sits on, so it is pinned here too.
        check("""
            {"code": "5327"}
            {"code": "4536"}
            """, field("code", DataType.KEYWORD));
    }

    public void testBooleanDetection() throws IOException {
        check("""
            {"active": true}
            {"active": false}
            """, field("active", DataType.BOOLEAN));
    }

    public void testNullMarksFieldNullableWithoutContributingAType() throws IOException {
        check("""
            {"v": 1}
            {"v": null}
            """, field("v", DataType.INTEGER, true));
    }

    public void testIntegerTooLargeForLongFallsBackToDouble() throws IOException {
        check("""
            {"v": 99999999999999999999999999}
            """, field("v", DataType.DOUBLE));
    }

    public void testNonObjectLineIsSkipped() throws IOException {
        // A line that is not a JSON object is a whole-line scanner failure: inference skips it and
        // leaves the decision to the read's error policy, rather than failing the whole plan.
        check("""
            {"v": 1}
            [1, 2]
            {"v": 2}
            """, field("v", DataType.INTEGER, true));
    }

    /**
     * Every type set this rail can produce, resolved by the lattice fold, must land exactly where the
     * hand-written rules landed it. The old rules are reproduced verbatim below rather than described,
     * so this compares implementations instead of comparing an implementation to a summary of itself.
     * <p>
     * All 127 non-empty subsets of the seven types the inferrer can observe, not a sample: the whole
     * point of moving the rule is that no set quietly changes answer.
     */
    public void testLatticeFoldMatchesTheReplacedRulesOnEverySubset() {
        DataType[] observable = {
            DataType.KEYWORD,
            DataType.INTEGER,
            DataType.LONG,
            DataType.DOUBLE,
            DataType.BOOLEAN,
            DataType.DATETIME,
            DataType.DATE_NANOS };
        int checked = 0;
        for (int mask = 1; mask < (1 << observable.length); mask++) {
            EnumSet<DataType> set = EnumSet.noneOf(DataType.class);
            for (int bit = 0; bit < observable.length; bit++) {
                if ((mask & (1 << bit)) != 0) {
                    set.add(observable[bit]);
                }
            }
            assertEquals(set.toString(), replacedRules(set), NdJsonSchemaInferrer.resolveObservedTypes(set));
            checked++;
        }
        assertEquals("every non-empty subset of the observable types", 127, checked);
    }

    public void testEmptyObservedSetIsNotAScalarColumn() {
        // A field only ever seen as an object or an always-empty array. The lattice has no bottom, so
        // this answer belongs to the caller and is asserted here rather than assumed.
        assertEquals(DataType.UNSUPPORTED, NdJsonSchemaInferrer.resolveObservedTypes(EnumSet.noneOf(DataType.class)));
    }

    /**
     * Verbatim copy of {@code FieldInfo.resolveType} as it stood immediately before the lattice
     * migration &mdash; that is, main's rules plus this branch's earlier DATETIME/DATE_NANOS clause,
     * not main's alone. The point of this test is to compare two implementations rather than an
     * implementation against a description of itself, so which state it copies matters.
     */
    private static DataType replacedRules(EnumSet<DataType> types) {
        if (types.isEmpty()) {
            return DataType.UNSUPPORTED;
        }
        if (types.size() == 1) {
            return types.iterator().next();
        }
        if (types.contains(DataType.KEYWORD)) {
            return DataType.KEYWORD;
        }
        if (hasOnly(types, EnumSet.of(DataType.DATETIME, DataType.DATE_NANOS))) {
            return DataType.DATE_NANOS;
        }
        if (hasOnly(types, EnumSet.of(DataType.DOUBLE, DataType.LONG, DataType.INTEGER))) {
            if (types.contains(DataType.DOUBLE)) {
                return DataType.DOUBLE;
            }
            if (types.contains(DataType.LONG)) {
                return DataType.LONG;
            }
            if (types.contains(DataType.INTEGER)) {
                return DataType.INTEGER;
            }
        }
        return DataType.KEYWORD;
    }

    private static boolean hasOnly(EnumSet<DataType> values, EnumSet<DataType> from) {
        if (values.isEmpty()) {
            return false;
        }
        EnumSet<DataType> copy = EnumSet.copyOf(values);
        copy.removeAll(from);
        return copy.isEmpty();
    }

    public void testLongValuedFieldInfersLong() throws IOException {
        check("""
            {"v": 9999999999}
            """, field("v", DataType.LONG));
    }

    /**
     * Every sampled line malformed: no field was ever observed, so there is no scalar column to
     * describe and the schema is empty rather than a guess.
     */
    public void testFileOfEntirelyMalformedLinesYieldsNoColumns() throws IOException {
        check("""
            [1, 2]
            [3, 4]
            """);
    }

    /**
     * Inference reads a bounded sample, so a value past that bound cannot change the answer. Asserted
     * because it is a real limit users hit — a conflicting value deep in a large file leaves the column
     * typed from the sample alone — not because the loop happens to stop there.
     */
    public void testValuesBeyondTheSampleBoundDoNotChangeTheType() throws IOException {
        StringBuilder ndjson = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            ndjson.append("{\"v\": ").append(i).append("}\n");
        }
        ndjson.append("{\"v\": \"past the sample\"}\n");
        check(ndjson.toString(), field("v", DataType.INTEGER));
    }

    /** Within the sample, the same conflict does resolve to KEYWORD — so the bound above is the reason. */
    public void testTheSameConflictWithinTheSampleDoesChangeTheType() throws IOException {
        check("""
            {"v": 1}
            {"v": "not a number"}
            """, field("v", DataType.KEYWORD));
    }

    /**
     * An invalid bare token (e.g. {@code not_json}) immediately followed by a valid record — with no blank
     * cushion line between them — must not cause the following record's columns to be lost from the inferred
     * schema. Exercises the streaming-path guard added for elastic/esql-planning#1704: without the fix,
     * {@link NdJsonUtils#moveToNextLine} consumes the following record as the remainder of the bad line, so
     * its columns never reach the schema sampler.
     * <p>
     * The existing {@link #testIgnoreEmptyAndInvalidLines} and {@link #testLineEndingVariations} tests do not
     * catch this because their bad-line fixtures are followed by a blank line (the cushion), which is what
     * the over-eager forward scan eats — the subsequent good record is unharmed.
     */
    public void testInvalidBareTokenWithoutCushionLine() throws IOException {
        // https://github.com/elastic/esql-planning/issues/1704
        // {"a":1} and {"b":2} are on consecutive lines with no blank between them.
        // Without the fix, "b" is never seen by the inferrer.
        check("{\"a\":1}\nnot_json\n{\"b\":2}\n", field("a", DataType.INTEGER, true), field("b", DataType.INTEGER, true));
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

    private static List<Attribute> infer(String ndjson, LimitedBreaker breaker) throws IOException {
        try (ByteArrayInputStream in = new ByteArrayInputStream(ndjson.getBytes(StandardCharsets.UTF_8))) {
            return NdJsonSchemaInferrer.inferSchema(in, 100, null, breaker);
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

    private void check(String ndjson, Attribute... expected) throws IOException {
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(ndjson.getBytes(StandardCharsets.UTF_8))) {
            List<Attribute> result = NdJsonSchemaInferrer.inferSchema(inputStream, 100, null, new NoopCircuitBreaker("test"));

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
