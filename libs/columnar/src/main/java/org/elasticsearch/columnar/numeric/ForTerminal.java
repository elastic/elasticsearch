/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;
import java.util.Arrays;

/** Frame-of-reference bit-packs a block: a {@code vint} bits-per-value then the packed longs. Frozen id 0. */
public final class ForTerminal implements BlockTerminal {

    static final byte ID = 0;

    private final DocValuesForUtil forUtil;

    public ForTerminal(int blockSize) {
        this.forUtil = new DocValuesForUtil(blockSize);
    }

    @Override
    public byte id() {
        return ID;
    }

    @Override
    public void encode(long[] block, DataOutput out) throws IOException {
        long or = 0;
        for (long l : block) {
            or |= l;
        }
        int bitsPerValue = or == 0 ? 0 : DocValuesForUtil.roundBits(PackedInts.unsignedBitsRequired(or));
        out.writeVInt(bitsPerValue);
        if (bitsPerValue > 0) {
            forUtil.encode(block, bitsPerValue, out);
        }
    }

    @Override
    public void decode(DataInput in, long[] block) throws IOException {
        int bitsPerValue = in.readVInt();
        if (bitsPerValue != 0) {
            forUtil.decode(bitsPerValue, in, block);
        } else {
            Arrays.fill(block, 0L);
        }
    }
}
