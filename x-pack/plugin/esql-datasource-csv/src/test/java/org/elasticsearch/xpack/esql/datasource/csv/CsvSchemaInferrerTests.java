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
import org.elasticsearch.xpack.esql.core.util.DateUtils;
import org.elasticsearch.xpack.esql.datasources.spi.TypeWidening;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

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

    // widenSchema tests

    public void testWideningFromKeywordConflict() {
        String[] cols = { "id" };
        List<String[]> sampleRows = List.of(new String[] { "1" }, new String[] { "2" }, new String[] { "3" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, sampleRows, null);
        assertEquals(DataType.INTEGER, schema.get(0).dataType());

        List<String[]> additionalRows = List.<String[]>of(new String[] { "hello" });
        List<Attribute> widened = CsvSchemaInferrer.widenSchema(schema, additionalRows, null);
        assertEquals(DataType.KEYWORD, widened.get(0).dataType());
    }

    public void testWideningPreservesTypeWhenNoConflict() {
        String[] cols = { "id" };
        List<String[]> sampleRows = List.of(new String[] { "1" }, new String[] { "2" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, sampleRows, null);

        List<String[]> additionalRows = List.of(new String[] { "3" }, new String[] { "4" });
        List<Attribute> result = CsvSchemaInferrer.widenSchema(schema, additionalRows, null);
        assertSame(schema, result);
    }

    public void testWideningDoesNotJumpPastIntermediate() {
        String[] cols = { "value" };
        List<String[]> sampleRows = List.of(new String[] { "42" }, new String[] { "100" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, sampleRows, null);
        assertEquals(DataType.INTEGER, schema.get(0).dataType());

        // A value that fits LONG but not INTEGER should widen to LONG, not skip straight to KEYWORD.
        List<String[]> additionalRows = List.<String[]>of(new String[] { "9999999999" });
        List<Attribute> widened = CsvSchemaInferrer.widenSchema(schema, additionalRows, null);
        assertEquals(DataType.LONG, widened.get(0).dataType());
    }

    public void testWideningBooleanJumpsToKeyword() {
        String[] cols = { "flag" };
        List<String[]> sampleRows = List.of(new String[] { "true" }, new String[] { "false" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, sampleRows, null);
        assertEquals(DataType.BOOLEAN, schema.get(0).dataType());

        // Confirmed BOOLEAN hit with a non-boolean value skips directly to KEYWORD.
        List<String[]> additionalRows = List.<String[]>of(new String[] { "42" });
        List<Attribute> widened = CsvSchemaInferrer.widenSchema(schema, additionalRows, null);
        assertEquals(DataType.KEYWORD, widened.get(0).dataType());
    }

    public void testWideningAllKeywordSchemaReturnsIdentical() {
        // A schema where every column is already KEYWORD (e.g. all-null sample) should be returned
        // unchanged by widenSchema — no object allocation, assertSame passes.
        String[] cols = { "a", "b" };
        List<String[]> sampleRows = List.<String[]>of(new String[] { null, null });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, sampleRows, null);
        assertEquals(DataType.KEYWORD, schema.get(0).dataType());
        assertEquals(DataType.KEYWORD, schema.get(1).dataType());

        List<String[]> additionalRows = List.<String[]>of(new String[] { "hello", "world" });
        List<Attribute> result = CsvSchemaInferrer.widenSchema(schema, additionalRows, null);
        assertSame(schema, result);
    }

    public void testWideningPartialColumns() {
        // Only the conflicting column widens; the non-conflicting one keeps its original Attribute object.
        String[] cols = { "id", "score" };
        List<String[]> sampleRows = List.of(new String[] { "1", "9.5" }, new String[] { "2", "8.0" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, sampleRows, null);
        assertEquals(DataType.INTEGER, schema.get(0).dataType());
        assertEquals(DataType.DOUBLE, schema.get(1).dataType());

        // "id" becomes KEYWORD; "score" stays DOUBLE.
        List<String[]> additionalRows = List.<String[]>of(new String[] { "hello", "7.2" });
        List<Attribute> widened = CsvSchemaInferrer.widenSchema(schema, additionalRows, null);
        assertEquals(DataType.KEYWORD, widened.get(0).dataType());
        assertEquals(DataType.DOUBLE, widened.get(1).dataType());
        assertSame(schema.get(1), widened.get(1)); // non-widened column keeps original Attribute
    }

    public void testWideningEmptyAdditionalRows() {
        String[] cols = { "id" };
        List<String[]> sampleRows = List.<String[]>of(new String[] { "1" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, sampleRows, null);

        List<Attribute> result = CsvSchemaInferrer.widenSchema(schema, List.of(), null);
        assertSame(schema, result);
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
     * The widening window runs every column as already-confirmed, so this is the path where a
     * nanosecond value arriving after the sample must still promote the column rather than collapse
     * it to KEYWORD.
     */
    public void testWidenSchemaNanosOnlyInWideningWindow() {
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(
            new String[] { "ts" },
            List.<String[]>of(new String[] { "2023-10-23T12:15:03.360Z" }),
            null
        );
        assertEquals(DataType.DATETIME, schema.get(0).dataType());

        List<Attribute> widened = CsvSchemaInferrer.widenSchema(
            schema,
            List.<String[]>of(new String[] { "2023-10-23T12:15:03.360103847Z" }),
            null
        );
        assertEquals(DataType.DATE_NANOS, widened.get(0).dataType());
    }

    public void testWidenSchemaOutOfWindowNanosStaysDatetime() {
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(
            new String[] { "ts" },
            List.<String[]>of(new String[] { "2023-10-23T12:15:03.360Z" }),
            null
        );
        List<Attribute> widened = CsvSchemaInferrer.widenSchema(
            schema,
            List.<String[]>of(new String[] { "2263-01-01T00:00:00.123456789Z" }),
            null
        );
        assertEquals(DataType.DATETIME, widened.get(0).dataType());
        assertSame("nothing widened, so the original list is returned", schema, widened);
    }

    public void testCustomDatetimeFormatRejectsNonMatchingValue() {
        // The custom-format arm has to be able to say "not a timestamp" too, not only "millis".
        DateFormatter custom = DateFormatter.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXX");
        List<String[]> rows = List.<String[]>of(new String[] { "not a date at all" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(new String[] { "ts" }, rows, custom);
        assertEquals(DataType.KEYWORD, schema.get(0).dataType());
    }

    public void testWideningSkipsNullEmptyAndShortRows() {
        // Widening must treat a missing cell, an empty cell and the literal "null" as carrying no
        // type evidence, exactly as the initial inference pass does — otherwise a ragged file would
        // widen columns to KEYWORD on absence alone.
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(
            new String[] { "id", "note" },
            List.of(new String[] { "1", "alpha" }, new String[] { "2", "beta" }),
            null
        );
        assertEquals(DataType.INTEGER, schema.get(0).dataType());

        List<Attribute> widened = CsvSchemaInferrer.widenSchema(
            schema,
            List.of(
                new String[] { "3" },               // short row: "note" cell absent entirely
                new String[] { "", "gamma" },       // empty cell
                new String[] { "null", "delta" },   // the literal null marker
                new String[] { null, "epsilon" }    // an actual null cell
            ),
            null
        );
        assertSame("absence is not evidence, so nothing should widen", schema, widened);
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
                    TypeWidening.join(first, second, TypeWidening.Policy.INFERENCE),
                    inferOne(canonicalValueFor(first), canonicalValueFor(second))
                );
            }
        }
    }

    /**
     * The whitespace screen in {@code classifyTemporal} encodes a fact about two parsers that live
     * elsewhere: which dialects the CSV datetime parser accepts that the {@code date_nanos} decode rail
     * rejects. Today that set is exactly {whitespace-separated, seconds-less}, and only the first can
     * ever be a forcing value, so only the first needs screening.
     * <p>
     * If {@code DateUtils.asDateTime} gains a dialect, or the nanos rail drops one, that reasoning goes
     * stale silently and a column starts flipping onto a rail that cannot decode it. This fails instead.
     */
    public void testTheDialectGapBetweenTheTwoParsersIsStillTheOneWeScreenFor() {
        record Dialect(String label, String value, boolean expectedToNeedScreening) {}
        List<Dialect> dialects = List.of(
            new Dialect("T-form with nanos", "2023-10-23T12:15:03.360103847Z", false),
            new Dialect("T-form millis", "2023-10-23T12:15:03.360Z", false),
            new Dialect("T-form no fraction", "2023-10-23T12:15:03Z", false),
            new Dialect("date only", "2023-10-23", false),
            new Dialect("whitespace-separated", "2023-10-23 12:15:03.360103847", true),
            new Dialect("seconds-less", "2023-10-23T12:15Z", true)
        );
        List<String> drift = new ArrayList<>();
        for (Dialect d : dialects) {
            boolean csvAccepts;
            try {
                DateUtils.asDateTime(d.value());
                csvAccepts = true;
            } catch (DateTimeParseException e) {
                csvAccepts = false;
            }
            boolean nanosRailAccepts;
            try {
                EsqlDataTypeConverter.dateNanosToLong(d.value());
                nanosRailAccepts = true;
            } catch (Exception e) {
                nanosRailAccepts = false;
            }
            boolean needsScreening = csvAccepts && nanosRailAccepts == false;
            if (needsScreening != d.expectedToNeedScreening()) {
                drift.add(
                    d.label()
                        + " ["
                        + d.value()
                        + "]: csvAccepts="
                        + csvAccepts
                        + " nanosRailAccepts="
                        + nanosRailAccepts
                        + " -> needsScreening="
                        + needsScreening
                        + ", expected "
                        + d.expectedToNeedScreening()
                );
            }
        }
        assertTrue("the dialect gap the whitespace screen assumes has moved:\n" + String.join("\n", drift), drift.isEmpty());
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
