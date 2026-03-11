/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.index.codec.tsdb.DocValuesForUtil;
import org.elasticsearch.index.codec.tsdb.pipeline.DecodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.EncodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.PayloadCodecStage;

import java.io.IOException;
import java.util.Arrays;

/**
 * Bit-packing terminal payload stage.
 *
 * <h2>Effectiveness</h2>
 * <p>Always applied as the terminal stage. Computes the minimum number of bits
 * needed to represent the largest value, then packs all values at that bit width
 * using {@link DocValuesForUtil}.
 *
 * <h2>Example</h2>
 * <p>Values {@code [0, 50, 100, 150]} need 8 bits per value (max=150), so 128
 * values are packed into 128 bytes instead of 1024 bytes (raw longs).
 *
 * <h2>Payload layout</h2>
 * <p>Written directly to the data stream as the payload section of the block
 * (see {@link org.elasticsearch.index.codec.tsdb.pipeline.BlockFormat}):
 * <pre>
 *   +------------------------+---------------------------------------------+
 *   | VInt(bitsPerValue)     | ForUtil packed data                         |
 *   | 1-5 bytes              | ceil(blockSize * bitsPerValue / 8) bytes    |
 *   +------------------------+---------------------------------------------+
 * </pre>
 * <p>The {@code bitsPerValue} is rounded by {@link DocValuesForUtil#roundBits} to
 * align with ForUtil's SIMD-friendly block widths. This data is written to the
 * payload section of the block, not through the metadata buffer. The packed data
 * size depends on both the bit width and the block size.
 *
 * @param forUtil the utility for encoding and decoding packed values
 */
public record BitPackCodecStage(DocValuesForUtil forUtil) implements PayloadCodecStage {

    @Override
    public byte id() {
        return StageId.BITPACK_PAYLOAD.id;
    }

    @Override
    public void encode(final long[] values, final int valueCount, final DataOutput out, final EncodingContext context) throws IOException {
        long or = 0;
        for (int i = 0; i < valueCount; i++) {
            or |= values[i];
        }
        final int bitsPerValue = or == 0 ? 0 : DocValuesForUtil.roundBits(PackedInts.unsignedBitsRequired(or));

        out.writeVInt(bitsPerValue);
        if (bitsPerValue > 0) {
            forUtil.encode(values, bitsPerValue, out);
        }
    }

    @Override
    public int decode(final long[] values, final DataInput in, final DecodingContext context) throws IOException {
        final int bitsPerValue = in.readVInt();
        if (bitsPerValue == 0) {
            Arrays.fill(values, 0L);
        } else {
            forUtil.decode(bitsPerValue, in, values);
        }
        return context.blockSize();
    }
}
