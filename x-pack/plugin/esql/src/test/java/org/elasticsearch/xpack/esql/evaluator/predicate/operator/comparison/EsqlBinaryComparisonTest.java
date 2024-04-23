/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.EsqlBinaryComparison.BinaryComparisonOpeartion;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparisonProcessor;

import java.io.IOException;
import java.util.List;

public class EsqlBinaryComparisonTest extends ESTestCase {

    public void testSerializationOfBinaryComparisonOperation() throws IOException {
        for (BinaryComparisonOpeartion op : BinaryComparisonOpeartion.values()) {
            BinaryComparisonOpeartion newOp = copyWriteable(
                op,
                new NamedWriteableRegistry(List.of()),
                EsqlBinaryComparison.BinaryComparisonOpeartion::readFromStream
            );
            assertEquals(op, newOp);
        }
    }

    /**
     * Test that a serialized
     * {@link org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparisonProcessor.BinaryComparisonOperation}
     * can be read back as a
     * {@link BinaryComparisonOpeartion}
     */
    public void testCompatibleWithQLBinaryComparisonOperation() throws IOException {
        validateCompatibility(BinaryComparisonProcessor.BinaryComparisonOperation.EQ, BinaryComparisonOpeartion.EQ);
        validateCompatibility(BinaryComparisonProcessor.BinaryComparisonOperation.NEQ, BinaryComparisonOpeartion.NEQ);
        validateCompatibility(BinaryComparisonProcessor.BinaryComparisonOperation.GT, BinaryComparisonOpeartion.GT);
        validateCompatibility(BinaryComparisonProcessor.BinaryComparisonOperation.GTE, BinaryComparisonOpeartion.GTE);
        validateCompatibility(BinaryComparisonProcessor.BinaryComparisonOperation.LT, BinaryComparisonOpeartion.LT);
        validateCompatibility(BinaryComparisonProcessor.BinaryComparisonOperation.LTE, BinaryComparisonOpeartion.LTE);
    }

    private static void validateCompatibility(
        BinaryComparisonProcessor.BinaryComparisonOperation original,
        BinaryComparisonOpeartion expected
    ) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setTransportVersion(TransportVersion.current());
            output.writeEnum(original);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), new NamedWriteableRegistry(List.of()))) {
                in.setTransportVersion(TransportVersion.current());
                BinaryComparisonOpeartion newOp = BinaryComparisonOpeartion.readFromStream(in);
                assertEquals(expected, newOp);
            }
        }
    }

}
