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
import java.util.Locale;

/**
 * Encodes a fixed-size block of ordinals using one of four per-block modes:
 * CONST, RLE, BITPACK_LOCAL, or LEGACY. The chosen mode minimizes serialized
 * bytes for the block. Wire format: 1 mode byte + mode-specific payload.
 *
 * <p>This skeleton only implements LEGACY (fixed-width bit packing at the
 * segment-global {@code bitsPerOrd}); CONST, BITPACK_LOCAL, and RLE land in
 * subsequent commits.
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
     * callers that need the original must copy beforehand.
     */
    void encodeOrdinals(long[] in, DataOutput out, int bitsPerOrd) throws IOException {
        out.writeByte(MODE_LEGACY);
        forUtil.encode(in, bitsPerOrd, out);
    }

    void decodeOrdinals(DataInput in, long[] out, int bitsPerOrd) throws IOException {
        byte mode = in.readByte();
        switch (mode) {
            case MODE_LEGACY:
                forUtil.decode(bitsPerOrd, in, out);
                return;
            default:
                throw new CorruptIndexException(String.format(Locale.ROOT, "unknown adaptive ordinal block mode 0x%02x", mode & 0xff), in);
        }
    }
}
