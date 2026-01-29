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

public final class BitPackCodecStage implements PayloadCodecStage {

    private final DocValuesForUtil forUtil;
    private final int blockSize;

    public BitPackCodecStage(int blockSize) {
        this.blockSize = blockSize;
        this.forUtil = new DocValuesForUtil(blockSize);
    }

    @Override
    public byte id() {
        return StageId.BIT_PACK.id;
    }

    @Override
    public String name() {
        return "bit-pack";
    }

    @Override
    public void encode(final long[] values, int valueCount, final DataOutput out, final EncodingContext context) throws IOException {
        if (valueCount < blockSize) {
            Arrays.fill(values, valueCount, blockSize, 0L);
        }

        int bitsPerValue = computeBitsPerValue(values, blockSize);
        out.writeVInt(bitsPerValue);

        if (bitsPerValue > 0) {
            forUtil.encode(values, bitsPerValue, out);
        }
    }

    @Override
    public int decode(final long[] values, final DataInput in, final DecodingContext context) throws IOException {
        final int bitsPerValue = in.readVInt();

        if (bitsPerValue > 0) {
            forUtil.decode(bitsPerValue, in, values);
        } else {
            // NOTE: Array is reused between blocks must explicitly zero when bitsPerValue=0 to avoid using a dirty `values` array
            Arrays.fill(values, 0, blockSize, 0L);
        }
        return context.blockSize();
    }

    private int computeBitsPerValue(final long[] values, int valueCount) {
        long or = 0;
        for (int i = 0; i < valueCount; i++) {
            or |= values[i];
        }
        return or == 0 ? 0 : DocValuesForUtil.roundBits(PackedInts.unsignedBitsRequired(or));
    }
}
