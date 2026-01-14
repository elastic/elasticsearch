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
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison.BinaryComparisonOperation;

import java.io.IOException;
import java.util.List;

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

}
