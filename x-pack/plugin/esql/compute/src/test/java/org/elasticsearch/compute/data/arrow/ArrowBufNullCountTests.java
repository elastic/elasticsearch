/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * A validity buffer that arrives over Flight has its exact length, ceil(positions / 8) bytes, not one padded to whole longs. Counting
 * its nulls must not read past the end.
 */
public class ArrowBufNullCountTests extends ESTestCase {

    public void testNullCountOfUnpaddedValidityBuffer() throws IOException {
        BlockFactory blockFactory = new BlockFactory(NoopCircuitBreaker.INSTANCE, BigArrays.NON_RECYCLING_INSTANCE);
        BufferAllocator allocator = blockFactory.arrowAllocator();
        try {
            for (int positions : new int[] { 1, 7, 8, 9, 63, 64, 65, 100, 129 }) {
                try (BigIntVector received = roundTrip(allocator, positions)) {
                    assertTrue(
                        "validity buffer is not padded to longs for " + positions,
                        received.getValidityBuffer().capacity() <= (positions + 7) / 8 + 7
                    );
                    try (LongBlock block = LongArrowBufBlock.of(received, blockFactory)) {
                        int nonNull = 0;
                        for (int i = 0; i < positions; i++) {
                            if (i % 3 != 0) {
                                nonNull++;
                            }
                        }
                        assertEquals(nonNull, block.getTotalValueCount());
                        assertEquals(positions == 1, block.areAllValuesNull()); // with one position, the only value is null
                    }
                }
            }
        } finally {
            allocator.close();
        }
    }

    /** Bits past the last position are not guaranteed to be unset: they must not turn an all-null block into a non-null one. */
    public void testAllNullBlockWithGarbageBitsPastTheLastPosition() throws IOException {
        BlockFactory blockFactory = new BlockFactory(NoopCircuitBreaker.INSTANCE, BigArrays.NON_RECYCLING_INSTANCE);
        BufferAllocator allocator = blockFactory.arrowAllocator();
        try {
            for (int positions : new int[] { 2, 7, 9, 65, 100 }) {
                try (BigIntVector received = roundTripAllNull(allocator, positions)) {
                    int lastByte = (positions - 1) / 8;
                    int used = positions - lastByte * 8; // bits of the last byte that belong to a position
                    if (used < 8) {
                        byte garbage = (byte) (0xFF << used);
                        received.getValidityBuffer().setByte(lastByte, received.getValidityBuffer().getByte(lastByte) | garbage);
                    }
                    try (LongBlock block = LongArrowBufBlock.of(received, blockFactory)) {
                        assertTrue("positions " + positions, block.areAllValuesNull());
                        assertEquals(0, block.getTotalValueCount());
                    }
                }
            }
        } finally {
            allocator.close();
        }
    }

    private static BigIntVector roundTripAllNull(BufferAllocator allocator, int positions) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (BigIntVector vector = new BigIntVector("v", allocator); VectorSchemaRoot root = VectorSchemaRoot.of(vector)) {
            vector.allocateNew(positions);
            for (int i = 0; i < positions; i++) {
                vector.setNull(i);
            }
            vector.setValueCount(positions);
            root.setRowCount(positions);
            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
        }
        ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(out.toByteArray()), allocator);
        reader.loadNextBatch();
        BigIntVector copy = new BigIntVector("v", allocator);
        ((BigIntVector) reader.getVectorSchemaRoot().getVector(0)).makeTransferPair(copy).transfer();
        reader.close();
        return copy;
    }

    /** Writes a vector with every third value null through the IPC stream format and reads it back. */
    private static BigIntVector roundTrip(BufferAllocator allocator, int positions) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (BigIntVector vector = new BigIntVector("v", allocator); VectorSchemaRoot root = VectorSchemaRoot.of(vector)) {
            vector.allocateNew(positions);
            for (int i = 0; i < positions; i++) {
                if (i % 3 == 0) {
                    vector.setNull(i);
                } else {
                    vector.set(i, i);
                }
            }
            vector.setValueCount(positions);
            root.setRowCount(positions);
            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
        }
        ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(out.toByteArray()), allocator);
        reader.loadNextBatch();
        BigIntVector received = (BigIntVector) reader.getVectorSchemaRoot().getVector(0);
        // transfer to an independent vector so that the reader can be closed
        BigIntVector copy = new BigIntVector("v", allocator);
        received.makeTransferPair(copy).transfer();
        reader.close();
        return copy;
    }
}
