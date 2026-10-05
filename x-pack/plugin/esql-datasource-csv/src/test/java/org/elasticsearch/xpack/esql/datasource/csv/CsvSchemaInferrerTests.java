/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.TypeWidening;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class CsvSchemaInferrerTests extends ESTestCase {

    public void testAllKeyword() {
        String[] cols = { "name", "city" };
        List<String[]> rows = List.of(new String[] { "Alice", "London" }, new String[] { "Bob", "Paris" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals(2, schema.size());
        assertEquals(DataType.KEYWORD, schema.get(0).dataType());
        assertEquals(DataType.KEYWORD, schema.get(1).dataType());
    }

    public void testIntegerDetection() {
        String[] cols = { "id", "age" };
        List<String[]> rows = List.of(new String[] { "1", "30" }, new String[] { "2", "25" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals(DataType.INTEGER, schema.get(0).dataType());
        assertEquals(DataType.INTEGER, schema.get(1).dataType());
    }

    public void testLongDetection() {
        String[] cols = { "big" };
        List<String[]> rows = List.of(new String[] { "9999999999" }, new String[] { "42" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals(DataType.LONG, schema.get(0).dataType());
    }

    public void testDoubleDetection() {
        String[] cols = { "score" };
        List<String[]> rows = List.of(new String[] { "95.5" }, new String[] { "87.3" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals(DataType.DOUBLE, schema.get(0).dataType());
    }

    public void testBooleanDetection() {
        String[] cols = { "active" };
        List<String[]> rows = List.of(new String[] { "true" }, new String[] { "false" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals(DataType.BOOLEAN, schema.get(0).dataType());
    }

    public void testBooleanCaseInsensitive() {
        String[] cols = { "flag" };
        List<String[]> rows = List.of(new String[] { "True" }, new String[] { "FALSE" }, new String[] { "true" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals(DataType.BOOLEAN, schema.get(0).dataType());
    }

    public void testDatetimeDetection() {
        String[] cols = { "ts" };
        List<String[]> rows = List.of(new String[] { "2021-01-01T00:00:00Z" }, new String[] { "2022-06-15T12:00:00Z" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals(DataType.DATETIME, schema.get(0).dataType());
    }

    public void testDateOnlyDetection() {
        String[] cols = { "date" };
        List<String[]> rows = List.of(new String[] { "2021-01-01" }, new String[] { "2022-06-15" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals(DataType.DATETIME, schema.get(0).dataType());
    }

    public void testZonelessTimestampDetection() {
        String[] cols = { "ts" };
        List<String[]> rows = List.<String[]>of(new String[] { "2021-01-01T10:30:00" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals(DataType.DATETIME, schema.get(0).dataType());
    }

    public void testMixedTypesWiden() {
        String[] cols = { "value" };
        List<String[]> rows = List.of(new String[] { "42" }, new String[] { "9999999999" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals(DataType.LONG, schema.get(0).dataType());
    }

    public void testIntToDoubleWidening() {
        String[] cols = { "value" };
        List<String[]> rows = List.of(new String[] { "42" }, new String[] { "3.14" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals(DataType.DOUBLE, schema.get(0).dataType());
    }

    public void testBooleanMismatchSkipsToKeyword() {
        String[] cols = { "flag" };
        List<String[]> rows = List.of(new String[] { "true" }, new String[] { "maybe" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals(DataType.KEYWORD, schema.get(0).dataType());
    }

    public void testDatetimeMismatchSkipsToKeyword() {
        String[] cols = { "ts" };
        List<String[]> rows = List.of(new String[] { "2021-01-01T00:00:00Z" }, new String[] { "not_a_date" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals(DataType.KEYWORD, schema.get(0).dataType());
    }

    public void testNullValuesPreserveCandidate() {
        String[] cols = { "value" };
        List<String[]> rows = List.of(new String[] { "42" }, new String[] { null }, new String[] { "" }, new String[] { "7" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals(DataType.INTEGER, schema.get(0).dataType());
    }

    public void testAllNullsDefaultToKeyword() {
        String[] cols = { "empty" };
        List<String[]> rows = List.of(new String[] { null }, new String[] { "" }, new String[] { "null" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals(DataType.KEYWORD, schema.get(0).dataType());
    }

    public void testEmptyRowsDefaultToKeyword() {
        String[] cols = { "col" };
        List<String[]> rows = List.of();
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals(DataType.KEYWORD, schema.get(0).dataType());
    }

    public void testMixedColumns() {
        String[] cols = { "name", "age", "score", "active", "created" };
        List<String[]> rows = List.of(
            new String[] { "Alice", "30", "95.5", "true", "2021-01-01T00:00:00Z" },
            new String[] { "Bob", "25", "87.3", "false", "2022-06-15T12:00:00Z" }
        );
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals(5, schema.size());
        assertEquals(DataType.KEYWORD, schema.get(0).dataType());
        assertEquals(DataType.INTEGER, schema.get(1).dataType());
        assertEquals(DataType.DOUBLE, schema.get(2).dataType());
        assertEquals(DataType.BOOLEAN, schema.get(3).dataType());
        assertEquals(DataType.DATETIME, schema.get(4).dataType());
    }

    public void testFewerValuesThanColumns() {
        String[] cols = { "a", "b", "c" };
        List<String[]> rows = List.<String[]>of(new String[] { "1", "hello" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals(3, schema.size());
        assertEquals(DataType.INTEGER, schema.get(0).dataType());
        assertEquals(DataType.KEYWORD, schema.get(1).dataType());
        assertEquals(DataType.KEYWORD, schema.get(2).dataType());
    }

    public void testNegativeNumbers() {
        String[] cols = { "value" };
        List<String[]> rows = List.of(new String[] { "-42" }, new String[] { "-7" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals(DataType.INTEGER, schema.get(0).dataType());
    }

    public void testNegativeDouble() {
        String[] cols = { "value" };
        List<String[]> rows = List.<String[]>of(new String[] { "-3.14" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals(DataType.DOUBLE, schema.get(0).dataType());
    }

    public void testColumnNames() {
        String[] cols = { " name ", " age " };
        List<String[]> rows = List.<String[]>of(new String[] { "Alice", "30" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals("name", schema.get(0).name());
        assertEquals("age", schema.get(1).name());
    }

    public void testInferredAttributesAreNullable() {
        String[] cols = { "name", "age" };
        List<String[]> rows = List.of(new String[] { "Alice", "30" }, new String[] { "Bob", "25" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        for (Attribute attr : schema) {
            assertEquals(Nullability.TRUE, attr.nullable());
        }
    }

    // widening-within-a-sample tests (CsvSchemaInferrer.widenSchema was folded into inferSchema when the
    // two sampling windows were merged into one — elastic/esql-planning#2134)

    public void testWideningFromKeywordConflict() {
        String[] cols = { "id" };
        List<String[]> rows = List.of(new String[] { "1" }, new String[] { "2" }, new String[] { "3" }, new String[] { "hello" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);
        assertEquals(DataType.KEYWORD, schema.get(0).dataType());
    }

    public void testWideningDoesNotJumpPastIntermediate() {
        String[] cols = { "value" };
        // A value that fits LONG but not INTEGER should widen to LONG, not skip straight to KEYWORD.
        List<String[]> rows = List.of(new String[] { "42" }, new String[] { "100" }, new String[] { "9999999999" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);
        assertEquals(DataType.LONG, schema.get(0).dataType());
    }

    public void testWideningBooleanJumpsToKeyword() {
        String[] cols = { "flag" };
        // Confirmed BOOLEAN hit with a non-boolean value skips directly to KEYWORD.
        List<String[]> rows = List.of(new String[] { "true" }, new String[] { "false" }, new String[] { "42" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);
        assertEquals(DataType.KEYWORD, schema.get(0).dataType());
    }

    public void testWideningPartialColumns() {
        // Only the conflicting column widens; the non-conflicting one stays DOUBLE.
        String[] cols = { "id", "score" };
        List<String[]> rows = List.of(new String[] { "1", "9.5" }, new String[] { "2", "8.0" }, new String[] { "hello", "7.2" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);
        assertEquals(DataType.KEYWORD, schema.get(0).dataType());
        assertEquals(DataType.DOUBLE, schema.get(1).dataType());
    }

    // -- the `widenings` out-param (elastic/esql-planning#2134) --

    public void testWideningReportsColumnTypeValueAndRow() {
        String[] cols = { "id", "name" };
        List<String[]> rows = List.of(new String[] { "1", "alice" }, new String[] { "2", "bob" }, new String[] { "oops", "carol" });
        List<CsvSchemaInferrer.Widening> widenings = new ArrayList<>();
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null, new boolean[cols.length], widenings);

        assertEquals(DataType.KEYWORD, schema.get(0).dataType());
        assertEquals(1, widenings.size());
        CsvSchemaInferrer.Widening widening = widenings.get(0);
        assertEquals(0, widening.column());
        assertEquals(DataType.INTEGER, widening.fromType());
        assertEquals(DataType.KEYWORD, widening.toType());
        assertEquals("oops", widening.value());
        assertEquals(3, widening.row());
    }

    public void testLosslessPromotionReportsNoWidening() {
        // integer -> long is lossless, matching the cross-file emitters' own gating
        // (emitKeywordFallbackWarnings / emitPrecisionLossWarnings): it must stay silent.
        String[] cols = { "id" };
        List<String[]> rows = List.of(new String[] { "1" }, new String[] { "9999999999" });
        List<CsvSchemaInferrer.Widening> widenings = new ArrayList<>();
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null, new boolean[cols.length], widenings);

        assertEquals(DataType.LONG, schema.get(0).dataType());
        assertEquals(List.of(), widenings);
    }

    public void testLongDoubleMergeReportsWidening() {
        String[] cols = { "value" };
        // 9007199254740993 is 2^53 + 1, the smallest long a double cannot represent exactly.
        List<String[]> rows = List.of(new String[] { "9007199254740993" }, new String[] { "1.5" });
        List<CsvSchemaInferrer.Widening> widenings = new ArrayList<>();
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null, new boolean[cols.length], widenings);

        assertEquals(DataType.DOUBLE, schema.get(0).dataType());
        assertEquals(1, widenings.size());
        assertEquals(DataType.LONG, widenings.get(0).fromType());
        assertEquals(DataType.DOUBLE, widenings.get(0).toType());
        assertEquals("1.5", widenings.get(0).value());
    }

    /**
     * A column mixing whole numbers and decimals entirely within the range a double represents
     * exactly (a price column: "9.99", "10", "12.5") must not be flagged — nothing is lost there.
     */
    public void testOrdinaryWholeNumberAndDecimalMixReportsNoWidening() {
        String[] cols = { "value" };
        List<String[]> rows = List.of(new String[] { "9.99" }, new String[] { "10" }, new String[] { "12.5" });
        List<CsvSchemaInferrer.Widening> widenings = new ArrayList<>();
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null, new boolean[cols.length], widenings);

        assertEquals(DataType.DOUBLE, schema.get(0).dataType());
        assertEquals(List.of(), widenings);
    }

    /**
     * The reverse order of {@link #testLongDoubleMergeReportsWidening}: a column committed to DOUBLE by
     * a genuine decimal, then a later value that happens to be exactly long-representable. {@code
     * recognise} starts its walk at the column's own (DOUBLE) rung and never re-walks LONG below it, so
     * DOUBLE silently accepts the long-shaped value as "nothing new" unless {@code narrowCandidate}
     * checks for it explicitly — which is exactly the gap a confirmed-DOUBLE column has to close, since
     * this is precisely the case {@code emitPrecisionLossWarnings} exists to report cross-file: a column
     * unified to DOUBLE where both LONG and DOUBLE shapes contributed, silently losing precision above
     * 2^53. {@code fromType} reports LONG (this value's own shape), not DOUBLE (the column's unchanged
     * committed type), since reporting {@code fromType == toType == DOUBLE} would say nothing useful.
     */
    public void testLongDoubleMergeReversedOrderReportsWidening() {
        String[] cols = { "value" };
        // 9007199254740993 is 2^53 + 1, the smallest long a double cannot represent exactly.
        List<String[]> rows = List.of(new String[] { "1.5" }, new String[] { "9007199254740993" });
        List<CsvSchemaInferrer.Widening> widenings = new ArrayList<>();
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null, new boolean[cols.length], widenings);

        assertEquals(DataType.DOUBLE, schema.get(0).dataType());
        assertEquals(1, widenings.size());
        assertEquals(DataType.LONG, widenings.get(0).fromType());
        assertEquals(DataType.DOUBLE, widenings.get(0).toType());
        assertEquals("9007199254740993", widenings.get(0).value());
        assertEquals(2, widenings.get(0).row());
    }

    /** A confirmed-DOUBLE column seeing more than one long-shaped value reports the merge only once. */
    public void testLongDoubleMergeReversedOrderReportsOnlyOnce() {
        String[] cols = { "value" };
        List<String[]> rows = List.of(new String[] { "1.5" }, new String[] { "9007199254740993" }, new String[] { "9007199254740994" });
        List<CsvSchemaInferrer.Widening> widenings = new ArrayList<>();
        CsvSchemaInferrer.inferSchema(cols, rows, null, new boolean[cols.length], widenings);

        assertEquals(1, widenings.size());
    }

    /**
     * A column with no non-null value in the early rows of the sample must still be typed from
     * whichever row first carries one — not default to KEYWORD the way a column already confirmed
     * KEYWORD by that point would. This was a latent discrepancy between the old two-pass
     * sample-then-widen design (where a column empty in the first window defaulted to KEYWORD and the
     * second pass, treating it as already-confirmed KEYWORD, never revisited it) and a true single
     * pass, surfaced while merging the two sampling windows into one (elastic/esql-planning#2134).
     */
    public void testColumnEmptyEarlyInSampleIsStillTypedFromALaterValue() {
        String[] cols = { "id", "maybe" };
        List<String[]> rows = List.of(
            new String[] { "1", null },
            new String[] { "2", null },
            new String[] { "3", "42" },
            new String[] { "4", "43" }
        );
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);
        assertEquals(
            "a column empty in the early rows must still be typed from the row where it first appears",
            DataType.INTEGER,
            schema.get(1).dataType()
        );
    }

    // -- date_nanos inference (elastic/esql-planning#1798) --

    private static DataType inferOne(String... values) {
        List<String[]> rows = new ArrayList<>(values.length);
        for (String value : values) {
            rows.add(new String[] { value });
        }
        return CsvSchemaInferrer.inferSchema(new String[] { "ts" }, rows, null).get(0).dataType();
    }

    public void testNanosecondTimestampInfersDateNanos() {
        assertEquals(DataType.DATE_NANOS, inferOne("2023-10-23T12:15:03.360103847Z"));
    }

    public void testTrailingZeroFractionStaysDatetime() {
        // Nine digits of text, but millisecond-exact as a value: datetime loses nothing.
        assertEquals(DataType.DATETIME, inferOne("2023-10-23T12:15:03.360000000Z"));
    }

    /**
     * The order-independence pin: a file's column type must not depend on which row its writer emitted
     * first. Both orders reach DATE_NANOS because that is what the lattice says a millisecond and a
     * nanosecond timestamp combine to, whichever one the ladder recognised first.
     */
    public void testMixedPrecisionWidensToDateNanosBothOrders() {
        assertEquals(DataType.DATE_NANOS, inferOne("2023-10-23T12:15:03.360103847Z", "2023-10-23T12:15:03.360Z"));
        assertEquals(DataType.DATE_NANOS, inferOne("2023-10-23T12:15:03.360Z", "2023-10-23T12:15:03.360103847Z"));
    }

    public void testConfirmedDatetimeGarbageStillJumpsToKeyword() {
        // The skip rule is excepted only for the nanos step; everything else still collapses.
        assertEquals(DataType.KEYWORD, inferOne("2023-10-23T12:15:03.360Z", "not a date"));
    }

    public void testConfirmedDateNanosGarbageJumpsToKeyword() {
        assertEquals(DataType.KEYWORD, inferOne("2023-10-23T12:15:03.360103847Z", "not a date"));
    }

    public void testPreEpochNanosecondStaysDatetime() {
        assertEquals(DataType.DATETIME, inferOne("1969-12-31T23:59:59.999999999Z"));
    }

    public void testPostWindowNanosecondStaysDatetime() {
        assertEquals(DataType.DATETIME, inferOne("2263-01-01T00:00:00.123456789Z"));
    }

    /**
     * Once a value has established the column is nanosecond-precision, an out-of-window timestamp is a
     * bad cell rather than evidence the column is a string — and it must read that way whichever row
     * came first, which is why the DATE_NANOS rung accepts any timestamp.
     */
    public void testOutOfWindowValueInDateNanosColumnStaysDateNanosBothOrders() {
        assertEquals(DataType.DATE_NANOS, inferOne("2023-10-23T12:15:03.360103847Z", "2263-01-01T00:00:00.123456789Z"));
        assertEquals(DataType.DATE_NANOS, inferOne("2263-01-01T00:00:00.123456789Z", "2023-10-23T12:15:03.360103847Z"));
    }

    /**
     * The whitespace-separated dialect parses here but not on the date_nanos decode rail, so it must
     * never be the value that flips a column: doing so would turn a cell that reads today into a
     * per-cell error.
     */
    public void testSpaceSeparatedNanosecondFractionStaysDatetime() {
        assertEquals(DataType.DATETIME, inferOne("2023-10-23 12:15:03.360103847"));
    }

    public void testCustomDatetimeFormatNeverInfersDateNanos() {
        DateFormatter custom = DateFormatter.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXX");
        List<String[]> rows = List.<String[]>of(new String[] { "2023-10-23T12:15:03.360103847Z" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(new String[] { "ts" }, rows, custom);
        assertEquals(DataType.DATETIME, schema.get(0).dataType());
    }

    /**
     * A nanosecond value appearing after an earlier millisecond value in the same sample must still
     * promote the column, not collapse it to KEYWORD.
     */
    public void testNanosLaterInSamplePromotesFromDatetime() {
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(
            new String[] { "ts" },
            List.of(new String[] { "2023-10-23T12:15:03.360Z" }, new String[] { "2023-10-23T12:15:03.360103847Z" }),
            null
        );
        assertEquals(DataType.DATE_NANOS, schema.get(0).dataType());
    }

    public void testOutOfWindowNanosStaysDatetime() {
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(
            new String[] { "ts" },
            List.of(new String[] { "2023-10-23T12:15:03.360Z" }, new String[] { "2263-01-01T00:00:00.123456789Z" }),
            null
        );
        assertEquals(DataType.DATETIME, schema.get(0).dataType());
    }

    public void testCustomDatetimeFormatRejectsNonMatchingValue() {
        // The custom-format arm has to be able to say "not a timestamp" too, not only "millis".
        DateFormatter custom = DateFormatter.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXX");
        List<String[]> rows = List.<String[]>of(new String[] { "not a date at all" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(new String[] { "ts" }, rows, custom);
        assertEquals(DataType.KEYWORD, schema.get(0).dataType());
    }

    public void testWideningSkipsNullEmptyAndShortRows() {
        // A missing cell, an empty cell and the literal "null" carry no type evidence — otherwise a
        // ragged file would widen columns to KEYWORD on absence alone. Both columns stay numeric on
        // purpose: a column already resolved to KEYWORD is skipped before its cell is read, so it would
        // never exercise the missing-cell path.
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(
            new String[] { "id", "score" },
            List.of(
                new String[] { "1", "10" },
                new String[] { "2", "20" },
                new String[] { "3" },            // short row: "score" cell absent entirely
                new String[] { "4", "" },        // empty cell
                new String[] { "5", "null" },    // the literal null marker
                new String[] { "6", null }       // an actual null cell
            ),
            null
        );
        assertEquals("absence is not evidence, so nothing should widen", DataType.INTEGER, schema.get(0).dataType());
        assertEquals("absence is not evidence, so nothing should widen", DataType.INTEGER, schema.get(1).dataType());
    }

    /**
     * A canonical value for each type this rail can infer, so a pair of types can be turned into a
     * two-row column and put through real inference.
     */
    private static String canonicalValueFor(DataType type) {
        return switch (type) {
            case BOOLEAN -> "true";
            case INTEGER -> "42";
            case LONG -> "9999999999";
            case DOUBLE -> "3.14";
            case DATETIME -> "2024-05-01T10:00:00Z";
            case DATE_NANOS -> "2024-05-01T10:00:00.000000001Z";
            case KEYWORD -> "hello";
            default -> throw new AssertionError("no canonical value for " + type);
        };
    }

    private static final List<DataType> INFERABLE = List.of(
        DataType.BOOLEAN,
        DataType.INTEGER,
        DataType.LONG,
        DataType.DOUBLE,
        DataType.DATETIME,
        DataType.DATE_NANOS,
        DataType.KEYWORD
    );

    /**
     * Inference over a two-value column must land where {@link TypeWidening} says those two types
     * combine, whichever order the rows arrive in. This is the guard that makes the four scattered
     * answers stay one answer: a type added to the ladder but not the lattice, or a promotion added to
     * one and not the other, fails here.
     */
    public void testEveryOrderedTypePairAgreesWithTheLattice() {
        for (DataType first : INFERABLE) {
            for (DataType second : INFERABLE) {
                assertEquals(
                    first + " then " + second,
                    TypeWidening.join(first, second),
                    inferOne(canonicalValueFor(first), canonicalValueFor(second))
                );
            }
        }
    }

    /**
     * The invariant the whitespace screen exists to maintain: a column is inferred {@code date_nanos}
     * only when its value would actually decode on the {@code date_nanos} rail.
     * <p>
     * Production asks that rail directly now, after two cheap stand-ins for it missed three dialects
     * between them. This is what caught the last of those and what stops the next: for every value in
     * the corpus, ask the rail and require inference to agree.
     * If {@code DateUtils.asDateTime} gains a dialect the nanos rail rejects, or the rail drops one,
     * some value here starts disagreeing and this fails, whether or not anyone thought to enumerate
     * that dialect.
     * <p>
     * The corpus is randomized per run rather than a fixed list, so drift does not have to land on a
     * shape someone anticipated.
     */
    public void testInferenceOnlyCommitsDateNanosForValuesTheNanosRailCanDecode() {
        List<String> corpus = new ArrayList<>(
            List.of(
                "2023-10-23T12:15:03.360103847Z",
                "2023-10-23 12:15:03.360103847",
                "2023-10-23T12:15Z",
                "2023-10-23 12:15",
                "2023-10-23T12:15:03.360Z",
                "2023-10-23",
                "1969-12-31T23:59:59.999999999Z",
                "2263-01-01T00:00:00.123456789Z",
                "+12023-10-23T12:15:03.360103847Z"
            )
        );
        for (int i = 0; i < 200; i++) {
            corpus.add(randomTimestampish());
        }

        for (String value : corpus) {
            DataType inferred = inferOne(value);
            boolean railDecodes;
            long railNanos = 0L;
            try {
                railNanos = EsqlDataTypeConverter.dateNanosToLong(value);
                railDecodes = true;
            } catch (Exception e) {
                railDecodes = false;
            }
            if (inferred == DataType.DATE_NANOS) {
                assertTrue("inferred date_nanos for a value the nanos rail cannot decode: [" + value + "]", railDecodes);
            }
            // And the other direction, which catches a screen that drifts too BROAD — suppressing
            // values it should have promoted. Stated over this corpus rather than as a general law:
            // it holds because every value here reaches the default ISO rail in a form both parsers
            // read the same way. A corpus that grew, say, lowercase 't'/'z' separators could break it
            // legitimately — the nanos rail would decode what asDateTime rejects — so extend the
            // generator and this assertion together.
            if (railDecodes && railNanos % 1_000_000L != 0L) {
                assertEquals(
                    "did not infer date_nanos for a sub-millisecond value the rail decodes: [" + value + "]",
                    DataType.DATE_NANOS,
                    inferred
                );
            }
        }
    }

    /** A timestamp-shaped string with a randomized separator, fraction width and zone. */
    private String randomTimestampish() {
        String date = String.format(
            Locale.ROOT,
            "%04d-%02d-%02d",
            randomIntBetween(1965, 2270),
            randomIntBetween(1, 12),
            randomIntBetween(1, 28)
        );
        String time = String.format(Locale.ROOT, "%02d:%02d", randomIntBetween(0, 23), randomIntBetween(0, 59));
        if (randomBoolean()) {
            time += String.format(Locale.ROOT, ":%02d", randomIntBetween(0, 59));
            int fractionDigits = randomIntBetween(0, 9);
            if (fractionDigits > 0) {
                StringBuilder frac = new StringBuilder(".");
                for (int i = 0; i < fractionDigits; i++) {
                    frac.append((char) ('0' + randomIntBetween(0, 9)));
                }
                time += frac;
            }
        }
        String separator = randomBoolean() ? "T" : " ";
        String zone = randomFrom("", "Z", "+01:00", "-05:00");
        return date + separator + time + zone;
    }

    /**
     * The regression review asked about: a column flipped by a well-formed {@code T}-form nanosecond
     * value while also holding whitespace-separated cells. Screening the forcing value alone left those
     * cells to fail per-cell at read, so a file that reads today would stop reading. The column stays
     * {@code datetime} instead, in either row order.
     */
    public void testColumnHoldingASpaceFormTimestampNeverFlipsToDateNanos() {
        assertEquals(DataType.DATETIME, inferOne("2023-10-23 12:15:03", "2023-10-23T12:15:03.360103847Z"));
        assertEquals(DataType.DATETIME, inferOne("2023-10-23T12:15:03.360103847Z", "2023-10-23 12:15:03"));
    }

    /** A column with no space-form cell is unaffected — the demotion is not a blanket retreat. */
    public void testAllTFormNanosecondColumnStillFlips() {
        assertEquals(DataType.DATE_NANOS, inferOne("2023-10-23T12:15:03.360103847Z", "2023-10-23T12:15:04.000000001Z"));
    }

    /**
     * A ragged sample: a row shorter than the header leaves later columns with no cell at all, which
     * carries no type evidence and must not be mistaken for one.
     */
    public void testShortRowsInTheSampleCarryNoEvidence() {
        // The second column stays numeric on purpose: a column already resolved to KEYWORD is skipped
        // before its cell is even read, so it would never exercise the missing-cell path.
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(
            new String[] { "id", "score" },
            List.of(new String[] { "1", "10" }, new String[] { "2" }, new String[] { "3", "30" }),
            null
        );
        assertEquals(DataType.INTEGER, schema.get(0).dataType());
        assertEquals("a missing cell is absence, not evidence", DataType.INTEGER, schema.get(1).dataType());
    }

    /**
     * The single-value invariant above misses the case the demotion exists for: a column is only wrong
     * when an undecodable dialect COEXISTS with a value that would flip the column. Every pair from the
     * corpus, both orders — if the column lands on {@code date_nanos}, every one of its values must
     * decode there.
     */
    private static final DateFormatter NANOS_RAIL_FORMAT = DateFormatter.forPattern("strict_date_optional_time_nanos");

    public void testNoColumnLandsOnTheNanosRailHoldingAValueThatRailRejects() {
        List<String> corpus = new ArrayList<>(
            List.of(
                "2023-10-23T12:15:03.360103847Z",
                "2023-10-23 12:15:03.360103847",
                "2023-10-23 12:15:03",
                "2023-10-23T12:15Z",
                "2023-10-23T12:15",
                "2023-10-23T12:15:03.360Z",
                "2023-10-23",
                // Years the CSV parser takes and the nanos rail does not: it wants exactly four
                // unsigned digits. Fixed rather than generated because the generator could not produce
                // this shape, which is precisely how it went unnoticed.
                "+12023-10-23T12:15:03Z",
                "+12023-10-23T12:15:03.360103847Z"
            )
        );
        for (int i = 0; i < 60; i++) {
            corpus.add(randomTimestampish());
        }
        for (String a : corpus) {
            for (String b : corpus) {
                if (inferOne(a, b) != DataType.DATE_NANOS) {
                    continue;
                }
                for (String cell : List.of(a, b)) {
                    // Two different failures hide behind "the rail rejected it". A DIALECT the rail
                    // cannot parse must never coexist with date_nanos — that is what the demotion is
                    // for, and it is decidable from shape whatever order the rows arrive in. A value
                    // that parses but falls outside the representable window is a different thing: it
                    // is deliberately kept, because demoting on it would make the column's type depend
                    // on row order, and it fails per-cell exactly as a declared date_nanos schema makes
                    // it fail. Only the first is asserted here.
                    if (NANOS_RAIL_FORMAT.tryParse(cell) == null) {
                        fail("column [" + a + "] + [" + b + "] inferred date_nanos but [" + cell + "] is a dialect that rail cannot parse");
                    }
                }
            }
        }
    }

    /**
     * A headerless sample is as wide as its widest row, not its first. Anything sized per column has to
     * agree with that or a ragged file throws at planning before a single value is typed.
     */
    public void testRaggedHeaderlessSampleWithAShortFirstRow() {
        List<String[]> rows = List.of(new String[] { "a" }, new String[] { "b", "c" });
        List<Attribute> schema = CsvFormatReader.inferSyntheticSchema(
            rows,
            "col",
            null,
            new boolean[CsvFormatReader.syntheticColumnCount(rows)],
            new ArrayList<>()
        );
        assertEquals("the widest row decides the column count", 2, schema.size());
    }

    /**
     * Every dialect the CSV parser accepts and the {@code date_nanos} rail does not, each of which
     * keeps its column off that rail in either row order. The list grew twice by review — seconds-less,
     * then signed years — which is why the predicate now asks the rail instead of describing it.
     */
    public void testEveryUndecodableDialectKeepsAColumnOffTheNanosRail() {
        String nanos = "2023-10-23T12:15:03.360103847Z";
        for (String undecodable : List.of("2023-10-23 12:15:03", "2023-10-23T12:15Z", "+12023-10-23T12:15:03Z", "-12023-10-23T12:15:03Z")) {
            assertEquals(undecodable + " then nanos", DataType.DATETIME, inferOne(undecodable, nanos));
            assertEquals("nanos then " + undecodable, DataType.DATETIME, inferOne(nanos, undecodable));
        }
    }

    public void testSynthesizeColumnNames() {
        String[] names = CsvFormatReader.synthesizeColumnNames(4, "col");
        assertArrayEquals(new String[] { "col0", "col1", "col2", "col3" }, names);

        String[] custom = CsvFormatReader.synthesizeColumnNames(3, "f_");
        assertArrayEquals(new String[] { "f_0", "f_1", "f_2" }, custom);

        String[] zero = CsvFormatReader.synthesizeColumnNames(0, "col");
        assertEquals(0, zero.length);
    }
}
