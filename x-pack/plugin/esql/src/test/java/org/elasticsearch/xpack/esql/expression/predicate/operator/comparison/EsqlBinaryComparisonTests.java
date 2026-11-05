/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.querydsl.query.BoolQuery;
import org.elasticsearch.xpack.esql.core.querydsl.query.MatchAll;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.RangeQuery;
import org.elasticsearch.xpack.esql.core.querydsl.query.TermQuery;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison.BinaryComparisonOperation;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;

public class EsqlBinaryComparisonTests extends ESTestCase {

    public void testSerializationOfBinaryComparisonOperation() throws IOException {
        for (BinaryComparisonOperation op : BinaryComparisonOperation.values()) {
            BinaryComparisonOperation newOp = copyWriteable(
                op,
                new NamedWriteableRegistry(List.of()),
                BinaryComparisonOperation::readFromStream
            );
            assertEquals(op, newOp);
        }
    }

    /**
     * Test that a serialized
     * {@code BinaryComparisonOperation}
     * from {@code org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison}
     * can be read back as a
     * {@link BinaryComparisonOperation}
     */
    public void testCompatibleWithQLBinaryComparisonOperation() throws IOException {
        validateCompatibility(
            org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparisonOperation.EQ,
            BinaryComparisonOperation.EQ
        );
        validateCompatibility(
            org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparisonOperation.NEQ,
            BinaryComparisonOperation.NEQ
        );
        validateCompatibility(
            org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparisonOperation.GT,
            BinaryComparisonOperation.GT
        );
        validateCompatibility(
            org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparisonOperation.GTE,
            BinaryComparisonOperation.GTE
        );
        validateCompatibility(
            org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparisonOperation.LT,
            BinaryComparisonOperation.LT
        );
        validateCompatibility(
            org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparisonOperation.LTE,
            BinaryComparisonOperation.LTE
        );
    }

    private static void validateCompatibility(
        org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparisonOperation original,
        BinaryComparisonOperation expected
    ) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setTransportVersion(TransportVersion.current());
            output.writeEnum(original);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), new NamedWriteableRegistry(List.of()))) {
                in.setTransportVersion(TransportVersion.current());
                BinaryComparisonOperation newOp = BinaryComparisonOperation.readFromStream(in);
                assertEquals(expected, newOp);
            }
        }
    }

    /**
     * Test that null types are compatible with any other type in binary comparisons.
     */
    public void testAreTypesCompatibleWithNull() {
        // null is compatible with any type
        assertTrue(EsqlBinaryComparison.areTypesCompatible(DataType.NULL, DataType.INTEGER));
        assertTrue(EsqlBinaryComparison.areTypesCompatible(DataType.INTEGER, DataType.NULL));
        assertTrue(EsqlBinaryComparison.areTypesCompatible(DataType.NULL, DataType.KEYWORD));
        assertTrue(EsqlBinaryComparison.areTypesCompatible(DataType.KEYWORD, DataType.NULL));
        assertTrue(EsqlBinaryComparison.areTypesCompatible(DataType.NULL, DataType.DATETIME));
        assertTrue(EsqlBinaryComparison.areTypesCompatible(DataType.DATETIME, DataType.NULL));
        assertTrue(EsqlBinaryComparison.areTypesCompatible(DataType.NULL, DataType.DOUBLE));
        assertTrue(EsqlBinaryComparison.areTypesCompatible(DataType.DOUBLE, DataType.NULL));
        assertTrue(EsqlBinaryComparison.areTypesCompatible(DataType.NULL, DataType.LONG));
        assertTrue(EsqlBinaryComparison.areTypesCompatible(DataType.LONG, DataType.NULL));
        assertTrue(EsqlBinaryComparison.areTypesCompatible(DataType.NULL, DataType.BOOLEAN));
        assertTrue(EsqlBinaryComparison.areTypesCompatible(DataType.BOOLEAN, DataType.NULL));
        assertTrue(EsqlBinaryComparison.areTypesCompatible(DataType.NULL, DataType.IP));
        assertTrue(EsqlBinaryComparison.areTypesCompatible(DataType.IP, DataType.NULL));
        assertTrue(EsqlBinaryComparison.areTypesCompatible(DataType.NULL, DataType.VERSION));
        assertTrue(EsqlBinaryComparison.areTypesCompatible(DataType.VERSION, DataType.NULL));

        // null with null
        assertTrue(EsqlBinaryComparison.areTypesCompatible(DataType.NULL, DataType.NULL));

        // null with unsigned_long (special case)
        assertTrue(EsqlBinaryComparison.areTypesCompatible(DataType.NULL, DataType.UNSIGNED_LONG));
        assertTrue(EsqlBinaryComparison.areTypesCompatible(DataType.UNSIGNED_LONG, DataType.NULL));
    }

    /**
     * Test that incompatible types (without null) still produce false.
     */
    public void testAreTypesCompatibleWithoutNull() {
        // Same types are compatible
        assertTrue(EsqlBinaryComparison.areTypesCompatible(DataType.INTEGER, DataType.INTEGER));
        assertTrue(EsqlBinaryComparison.areTypesCompatible(DataType.KEYWORD, DataType.KEYWORD));

        // Numeric types are compatible with each other (except unsigned_long with others)
        assertTrue(EsqlBinaryComparison.areTypesCompatible(DataType.INTEGER, DataType.LONG));
        assertTrue(EsqlBinaryComparison.areTypesCompatible(DataType.DOUBLE, DataType.INTEGER));

        // String types are compatible with each other
        assertTrue(EsqlBinaryComparison.areTypesCompatible(DataType.KEYWORD, DataType.TEXT));

        // Date types are compatible with each other
        assertTrue(EsqlBinaryComparison.areTypesCompatible(DataType.DATETIME, DataType.DATE_NANOS));

        // unsigned_long is only compatible with itself
        assertTrue(EsqlBinaryComparison.areTypesCompatible(DataType.UNSIGNED_LONG, DataType.UNSIGNED_LONG));
        assertFalse(EsqlBinaryComparison.areTypesCompatible(DataType.UNSIGNED_LONG, DataType.INTEGER));
        assertFalse(EsqlBinaryComparison.areTypesCompatible(DataType.INTEGER, DataType.UNSIGNED_LONG));

        // Incompatible types
        assertFalse(EsqlBinaryComparison.areTypesCompatible(DataType.INTEGER, DataType.KEYWORD));
        assertFalse(EsqlBinaryComparison.areTypesCompatible(DataType.DATETIME, DataType.INTEGER));
        assertFalse(EsqlBinaryComparison.areTypesCompatible(DataType.BOOLEAN, DataType.INTEGER));
    }

    /**
     * {@code NaN} is unequal to every value and compares as {@code false} with all of them, so a comparison against it must
     * push down to what evaluation returns: every value for {@code !=} and none for the other comparisons.
     */
    public void testNaNComparisonPushdown() {
        FieldAttribute field = new FieldAttribute(
            EMPTY,
            "field",
            new EsField("field", randomFrom(DataType.DOUBLE, DataType.INTEGER), Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        Literal nan = new Literal(EMPTY, Double.NaN, DataType.DOUBLE);
        Query matchNone = new MatchAll(EMPTY).negate(EMPTY);

        assertEquals(new MatchAll(EMPTY), asQuery(new NotEquals(EMPTY, field, nan, null)));
        assertEquals(matchNone, asQuery(new Equals(EMPTY, field, nan, null)));
        assertEquals(matchNone, asQuery(new GreaterThan(EMPTY, field, nan, null)));
        assertEquals(matchNone, asQuery(new GreaterThanOrEqual(EMPTY, field, nan, null)));
        assertEquals(matchNone, asQuery(new LessThan(EMPTY, field, nan, null)));
        assertEquals(matchNone, asQuery(new LessThanOrEqual(EMPTY, field, nan, null)));
    }

    /**
     * Lucene orders {@code -0.0} below {@code 0.0} on floating-point fields, while evaluation compares them as equal, so a
     * comparison against zero must push down to bounds that take in or leave out both zeros, whatever the literal's sign
     * or type.
     */
    public void testZeroComparisonPushdownOnRationalField() {
        FieldAttribute field = new FieldAttribute(
            EMPTY,
            "field",
            new EsField("field", DataType.DOUBLE, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        Literal zero = randomFrom(
            new Literal(EMPTY, 0.0, DataType.DOUBLE),
            new Literal(EMPTY, -0.0, DataType.DOUBLE),
            new Literal(EMPTY, 0, DataType.INTEGER)
        );

        assertEquals(
            new RangeQuery(EMPTY, "field", 0.0, false, null, false, null, null),
            asQuery(new GreaterThan(EMPTY, field, zero, null))
        );
        assertEquals(
            new RangeQuery(EMPTY, "field", -0.0, true, null, false, null, null),
            asQuery(new GreaterThanOrEqual(EMPTY, field, zero, null))
        );
        assertEquals(new RangeQuery(EMPTY, "field", null, false, -0.0, false, null, null), asQuery(new LessThan(EMPTY, field, zero, null)));
        assertEquals(
            new RangeQuery(EMPTY, "field", null, false, 0.0, true, null, null),
            asQuery(new LessThanOrEqual(EMPTY, field, zero, null))
        );
        assertEquals(new RangeQuery(EMPTY, "field", -0.0, true, 0.0, true, null, null), asQuery(new Equals(EMPTY, field, zero, null)));
        assertEquals(
            new BoolQuery(
                EMPTY,
                false,
                new RangeQuery(EMPTY, "field", null, false, -0.0, false, null, null),
                new RangeQuery(EMPTY, "field", 0.0, false, null, false, null, null)
            ),
            asQuery(new NotEquals(EMPTY, field, zero, null))
        );
    }

    /**
     * Whole-number fields have a single zero, so a comparison against zero pushes down unchanged.
     */
    public void testZeroComparisonPushdownOnWholeNumberField() {
        FieldAttribute field = new FieldAttribute(
            EMPTY,
            "field",
            new EsField("field", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        Literal zero = new Literal(EMPTY, 0, DataType.INTEGER);

        assertEquals(new TermQuery(EMPTY, "field", 0), asQuery(new Equals(EMPTY, field, zero, null)));
        assertEquals(new RangeQuery(EMPTY, "field", 0, false, null, false, null, null), asQuery(new GreaterThan(EMPTY, field, zero, null)));
    }

    private static Query asQuery(EsqlBinaryComparison comparison) {
        return comparison.asQuery(LucenePushdownPredicates.DEFAULT, TranslatorHandler.TRANSLATOR_HANDLER);
    }
}
