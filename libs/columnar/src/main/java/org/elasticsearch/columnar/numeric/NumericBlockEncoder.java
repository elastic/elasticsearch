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

import java.io.IOException;
import java.util.Arrays;

/**
 * Encodes and decodes a block of {@code long}s through a {@link NumericPipeline}: an ordered chain of
 * {@link BlockTransform}s, each fired only when it shrinks the block, then a {@link BlockTerminal} that
 * serializes the residuals. The layout is self-describing (a fire bitmask records which transforms fired),
 * so stages can be added later without breaking already-written data. A whole block is decoded on any
 * random access.
 */
public final class NumericBlockEncoder {

    private final NumericPipeline pipeline;
    private final BlockTransform[] transforms;
    private final BlockTerminal terminal;
    private final int numericBlockSize;

    // Per-transform scratch for params captured during encoding; reset per block. Single threaded use.
    private final MetadataBuffer[] paramBuffers;
    // Reused across block decodes; bound to the active DataInput via reset() before each call.
    private final DataInputMetadataReader decoderParams = new DataInputMetadataReader();
    // Separate FOR helper for the ordinal path, which is independent of the pipeline.
    private final DocValuesForUtil ordinalForUtil;

    public NumericBlockEncoder(NumericPipeline pipeline, int numericBlockSize) {
        this.pipeline = pipeline;
        this.transforms = pipeline.transforms();
        this.terminal = pipeline.terminal();
        this.numericBlockSize = numericBlockSize;
        this.paramBuffers = new MetadataBuffer[transforms.length];
        for (int i = 0; i < transforms.length; i++) {
            paramBuffers[i] = new MetadataBuffer();
        }
        this.ordinalForUtil = new DocValuesForUtil(numericBlockSize);
    }

    /** The pipeline this encoder runs. */
    public NumericPipeline pipeline() {
        return pipeline;
    }

    /**
     * Encodes the first {@code valueCount} values of a block: {@code vint fireBitmask}, then the terminal
     * payload, then each fired transform's params in reverse pipeline order. Every stage sees only the
     * real values, so a partial last block never needs the caller to pad.
     */
    public void encode(long[] in, int valueCount, DataOutput out) throws IOException {
        assert in.length == numericBlockSize;
        assert valueCount >= 1 && valueCount <= numericBlockSize : valueCount;

        int fireBitmask = 0;
        for (int i = 0; i < transforms.length; i++) {
            MetadataBuffer params = paramBuffers[i];
            params.reset();
            if (transforms[i].tryEncode(in, valueCount, params)) {
                fireBitmask |= 1 << i;
            }
        }

        out.writeVInt(fireBitmask);
        terminal.encode(in, valueCount, out);
        for (int i = transforms.length - 1; i >= 0; i--) {
            if ((fireBitmask & (1 << i)) != 0) {
                paramBuffers[i].copyTo(out);
            }
        }
    }

    /** Decodes the first {@code valueCount} values of a block encoded with {@link #encode}. */
    public void decode(DataInput in, int valueCount, long[] out) throws IOException {
        assert out.length == numericBlockSize : out.length;
        assert valueCount >= 1 && valueCount <= numericBlockSize : valueCount;

        int fireBitmask = in.readVInt();
        terminal.decode(in, valueCount, out);
        decoderParams.reset(in);
        for (int i = transforms.length - 1; i >= 0; i--) {
            if ((fireBitmask & (1 << i)) != 0) {
                transforms[i].decode(out, valueCount, decoderParams);
            }
        }
    }

    /**
     * Optimizes for encoding sorted fields where we expect a block to mostly either be the same value
     * or to make a transition from one value to a second one.
     * <p>
     * The header is a vlong where the number of trailing ones defines the encoding strategy:
     * <ul>
     *   <li>0: single run</li>
     *   <li>1: two runs</li>
     *   <li>2: bit-packed</li>
     *   <li>3: cycle</li>
     * </ul>
     */
    public void encodeOrdinals(long[] in, DataOutput out, int bitsPerOrd) throws IOException {
        assert in.length == numericBlockSize;
        int numRuns = 1;
        long firstValue = in[0];
        long previousValue = firstValue;
        boolean cyclic = false;
        int cycleLength = 0;
        for (int i = 1; i < in.length; ++i) {
            long currentValue = in[i];
            if (previousValue != currentValue) {
                numRuns++;
            }
            if (currentValue == firstValue && cycleLength != -1) {
                if (cycleLength == 0) {
                    // first candidate cycle detected
                    cycleLength = i;
                } else if (cycleLength == 1 || i % cycleLength != 0) {
                    // if the first two values are the same this isn't a cycle, it might be a run, though
                    // this also isn't a cycle if the index of the next occurrence of the first value
                    // isn't a multiple of the candidate cycle length
                    // we can stop looking for cycles now
                    cycleLength = -1;
                }
            }
            previousValue = currentValue;
        }
        // if the cycle is too long, bit-packing may be more space efficient
        int maxCycleLength = in.length / 4;
        if (numRuns > 2 && cycleLength > 1 && cycleLength <= maxCycleLength) {
            cyclic = true;
            for (int i = cycleLength; i < in.length; ++i) {
                if (in[i] != in[i - cycleLength]) {
                    cyclic = false;
                    break;
                }
            }
        }
        if (numRuns == 1 && bitsPerOrd < 63) {
            long value = in[0];
            // unset first bit (0 trailing ones) to indicate the block has a single run
            out.writeVLong(value << 1);
        } else if (numRuns == 2 && bitsPerOrd < 62) {
            // set 1 trailing bit to indicate the block has two runs
            out.writeVLong((in[0] << 2) | 0b01);
            int firstRunLen = in.length;
            for (int i = 1; i < in.length; ++i) {
                if (in[i] != in[0]) {
                    firstRunLen = i;
                    break;
                }
            }
            out.writeVInt(firstRunLen);
            out.writeZLong(in[in.length - 1] - in[0]);
        } else if (cyclic) {
            // set 3 trailing bits to indicate the block cycles through the same values
            long headerAndCycleLength = ((long) cycleLength << 4) | 0b0111;
            out.writeVLong(headerAndCycleLength);
            for (int i = 0; i < cycleLength; i++) {
                out.writeVLong(in[i]);
            }
        } else {
            // set 2 trailing bits to indicate the block is bit-packed
            out.writeVLong(0b11);
            ordinalForUtil.encode(in, bitsPerOrd, out);
        }
    }

    public void decodeOrdinals(DataInput in, long[] out, int bitsPerOrd) throws IOException {
        assert out.length == numericBlockSize : out.length;

        long v1 = in.readVLong();
        int encoding = Long.numberOfTrailingZeros(~v1);
        v1 >>>= encoding + 1;
        if (encoding == 0) {
            // single run
            Arrays.fill(out, v1);
        } else if (encoding == 1) {
            // two runs
            int runLen = in.readVInt();
            long v2 = v1 + in.readZLong();
            Arrays.fill(out, 0, runLen, v1);
            Arrays.fill(out, runLen, out.length, v2);
        } else if (encoding == 2) {
            // bit-packed
            ordinalForUtil.decode(bitsPerOrd, in, out);
        } else if (encoding == 3) {
            // cycle encoding
            int cycleLength = (int) v1;
            for (int i = 0; i < cycleLength; i++) {
                out[i] = in.readVLong();
            }
            int length = cycleLength;
            while (length < out.length) {
                int copyLength = Math.min(length, out.length - length);
                System.arraycopy(out, 0, out, length, copyLength);
                length += copyLength;
            }
        }
    }
}
