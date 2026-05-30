/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.elasticsearch.index.codec.tsdb.DocValuesForUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;

/**
 * Encodes a fixed-size block of ordinals using one of four per-block modes:
 * CONST, RLE, BITPACK_LOCAL, or LEGACY. The chosen mode minimizes serialized
 * bytes for the block. Wire format: 1 mode byte + mode-specific payload.
 *
 * <p>This commit lands CONST (single distinct value collapsed to one vlong)
 * on top of the LEGACY baseline. RLE and BITPACK_LOCAL follow in subsequent
 * commits.
 */
final class AdaptiveOrdinalEncoder {

    static final byte MODE_CONST = 0;
    static final byte MODE_RLE = 1;
    static final byte MODE_BITPACK_LOCAL = 2;
    static final byte MODE_LEGACY = 3;

    private final DocValuesForUtil forUtil;

    AdaptiveOrdinalEncoder(int blockSize) {
        this.forUtil = new DocValuesForUtil(blockSize);
    }

    /**
     * Encodes one block of ordinals to {@code out}. May mutate {@code in} for
     * certain {@code bitsPerOrd} branches inside {@link DocValuesForUtil#encode};
     * callers that need the original must copy beforehand. {@code in} must have
     * length equal to the encoder block size; empty arrays are not supported.
     */
    void encodeOrdinals(long[] in, DataOutput out, int bitsPerOrd) throws IOException {
        long first = in[0];
        boolean allSame = true;
        for (int i = 1; i < in.length; i++) {
            if (in[i] != first) {
                allSame = false;
                break;
            }
        }
        if (allSame) {
            out.writeByte(MODE_CONST);
            out.writeVLong(first);
            return;
        }

        out.writeByte(MODE_LEGACY);
        forUtil.encode(in, bitsPerOrd, out);
    }

    void decodeOrdinals(DataInput in, long[] out, int bitsPerOrd) throws IOException {
        byte mode = in.readByte();
        switch (mode) {
            case MODE_CONST: {
                long constValue = in.readVLong();
                Arrays.fill(out, constValue);
                return;
            }
            case MODE_LEGACY:
                forUtil.decode(bitsPerOrd, in, out);
                return;
            default:
                throw new CorruptIndexException(String.format(Locale.ROOT, "unknown adaptive ordinal block mode 0x%02x", mode & 0xff), in);
        }
    }
}
