/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.columnar;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.elasticsearch.columnar.numeric.BlockTerminal;

import java.io.IOException;

/**
 * Benchmark-only terminal that serializes a block as raw 8-byte longs, with no bit-packing.
 * Used to isolate individual {@link org.elasticsearch.columnar.numeric.BlockTransform} stages
 * from the cost of FOR bit-packing when measuring per-stage encode/decode throughput.
 */
final class RawTerminal implements BlockTerminal {

    static final RawTerminal INSTANCE = new RawTerminal();

    private RawTerminal() {}

    @Override
    public byte id() {
        return (byte) 0x7F;
    }

    @Override
    public void encode(long[] block, int valueCount, DataOutput out) throws IOException {
        for (int i = 0; i < valueCount; i++) {
            out.writeLong(block[i]);
        }
    }

    @Override
    public void decode(DataInput in, int valueCount, long[] block) throws IOException {
        for (int i = 0; i < valueCount; i++) {
            block[i] = in.readLong();
        }
    }
}
