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
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison.BinaryComparisonOperation;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparisonProcessor;

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
     * {@link org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparisonProcessor.BinaryComparisonOperation}
     * can be read back as a
     * {@link BinaryComparisonOperation}
     */
    public void testCompatibleWithQLBinaryComparisonOperation() throws IOException {
        validateCompatibility(BinaryComparisonProcessor.BinaryComparisonOperation.EQ, BinaryComparisonOperation.EQ);
        validateCompatibility(BinaryComparisonProcessor.BinaryComparisonOperation.NEQ, BinaryComparisonOperation.NEQ);
        validateCompatibility(BinaryComparisonProcessor.BinaryComparisonOperation.GT, BinaryComparisonOperation.GT);
        validateCompatibility(BinaryComparisonProcessor.BinaryComparisonOperation.GTE, BinaryComparisonOperation.GTE);
        validateCompatibility(BinaryComparisonProcessor.BinaryComparisonOperation.LT, BinaryComparisonOperation.LT);
        validateCompatibility(BinaryComparisonProcessor.BinaryComparisonOperation.LTE, BinaryComparisonOperation.LTE);
    }

    private static void validateCompatibility(
        BinaryComparisonProcessor.BinaryComparisonOperation original,
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

}
