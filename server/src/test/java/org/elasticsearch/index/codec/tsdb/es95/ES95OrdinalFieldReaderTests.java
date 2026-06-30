/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.index.codec.tsdb.OrdinalFieldReader;
import org.elasticsearch.index.codec.tsdb.RunLengthOrdinalEncoder;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesEncoder;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesFormatConfig;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class ES95OrdinalFieldReaderTests extends ESTestCase {

    private static final int BLOCK_SIZE = 128;

    public void testRunLengthVersionDecodesRunLengthEncoding() throws IOException {
        final long[] ordinals = runBased();
        final int bitsPerOrd = PackedInts.bitsRequired(maxOrd(ordinals));
        final DataInput encoded = encodeRunLength(ordinals, bitsPerOrd);

        final long[] decoded = new long[BLOCK_SIZE];
        decoderAt(TSDBDocValuesFormatConfig.VERSION_ORDINAL_RUN_LENGTH).decodeOrdinals(encoded, decoded, bitsPerOrd);

        assertArrayEquals(ordinals, decoded);
    }

    public void testPreRunLengthVersionDecodesFourStrategyEncoding() throws IOException {
        final long[] ordinals = runBased();
        final int bitsPerOrd = PackedInts.bitsRequired(maxOrd(ordinals));
        final DataInput encoded = encodeFourStrategy(ordinals, bitsPerOrd);

        final long[] decoded = new long[BLOCK_SIZE];
        decoderAt(TSDBDocValuesFormatConfig.VERSION_ORDINAL_BLOCK_SHIFT).decodeOrdinals(encoded, decoded, bitsPerOrd);

        assertArrayEquals(ordinals, decoded);
    }

    private static OrdinalFieldReader.Decoder decoderAt(int segmentVersion) {
        return new ES95OrdinalFieldReader(segmentVersion).decoder(BLOCK_SIZE);
    }

    // clone: the bit-packed branch mutates its input through `forUtil`, but the caller asserts on it
    private static DataInput encodeRunLength(long[] ordinals, int bitsPerOrd) throws IOException {
        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        new RunLengthOrdinalEncoder(BLOCK_SIZE).encode(ordinals.clone(), out, bitsPerOrd);
        return out.toDataInput();
    }

    private static DataInput encodeFourStrategy(long[] ordinals, int bitsPerOrd) throws IOException {
        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        new TSDBDocValuesEncoder(BLOCK_SIZE).encodeOrdinals(ordinals.clone(), out, bitsPerOrd);
        return out.toDataInput();
    }

    private static long[] runBased() {
        final long[] ordinals = new long[BLOCK_SIZE];
        final int runLength = BLOCK_SIZE / 4;
        for (int i = 0; i < BLOCK_SIZE; i++) {
            ordinals[i] = i / runLength;
        }
        return ordinals;
    }

    private static long maxOrd(long[] ordinals) {
        long max = 0;
        for (long ordinal : ordinals) {
            max = Math.max(max, ordinal);
        }
        return max;
    }
}
