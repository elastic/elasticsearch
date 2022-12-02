/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

import java.io.IOException;

public class DocValuesForUtil {
    private final ForUtil forUtil = new ForUtil();
    private final int blockSize;

    public DocValuesForUtil() {
        this(ES87TSDBDocValuesFormat.DEFAULT_NUMERIC_BLOCK_SIZE);
    }

    public DocValuesForUtil(int blockSize) {
        this.blockSize = blockSize;
    }

    void encode(long[] in, int bitsPerValue, DataOutput out) throws IOException {
        if (bitsPerValue <= 24) { // these bpvs are handled efficiently by ForUtil
            forUtil.encode(in, bitsPerValue, out);
        } else if (bitsPerValue <= 32) {
            collapse32(in);
            for (int i = 0; i < blockSize / 2; ++i) {
                out.writeLong(in[i]);
            }
        } else {
            for (long l : in) {
                out.writeLong(l);
            }
        }
    }

    void decode(int bitsPerValue, DataInput in, long[] out) throws IOException {
        if (bitsPerValue <= 24) {
            forUtil.decode(bitsPerValue, in, out);
        } else if (bitsPerValue <= 32) {
            in.readLongs(out, 0, blockSize / 2);
            expand32(out);
        } else {
            in.readLongs(out, 0, blockSize);
        }
    }

    private static void collapse32(long[] arr) {
        for (int i = 0; i < 64; ++i) {
            arr[i] = (arr[i] << 32) | arr[64 + i];
        }
    }

    private static void expand32(long[] arr) {
        for (int i = 0; i < 64; ++i) {
            long l = arr[i];
            arr[i] = l >>> 32;
            arr[64 + i] = l & 0xFFFFFFFFL;
        }
    }
}
