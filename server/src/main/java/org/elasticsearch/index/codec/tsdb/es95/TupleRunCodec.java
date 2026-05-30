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

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;

/**
 * Tuple-run block codec (encoding 3, sub-mode {@link #SUB_MODE}). Encodes the SORTED_SET
 * ordinal stream of a 128-element block as a sequence of (K, runLen, tuple) entries, where
 * each entry groups consecutive docs that emit the same K-ord tuple. Targets the K-cycle
 * pattern produced by multi-valued docs sharing the same ord set within a {@code _tsid}
 * run (e.g. {@code host.ip}, {@code host.mac}).
 *
 * <p>Within each tuple the K ords are encoded as one absolute vlong followed by K-1 deltas,
 * exploiting Lucene's invariant that the K ords of a SORTED_SET doc are strictly sorted
 * ascending (so every delta is positive).
 *
 * <p>Block-boundary handling: docs can straddle blocks, so the codec accepts a
 * {@code headOffset} (ords of the first doc that live in the previous block) and a
 * {@code tailMissing} (ords of the last doc that live in the next block). Runs at the block
 * edges fall into three cases:
 *
 * <ul>
 *   <li>If the head-partial doc shares a tuple with the next doc (typical TSDB mid-tsid
 *       crossing), the encoder extends the run after the second doc reveals the missing
 *       positions; the wire stores the full K-ord tuple as usual.</li>
 *   <li>If the head-partial doc is alone in its run (runLen = 1 with headOffset &gt; 0),
 *       the encoder writes only its visible portion (K - headOffset ords).</li>
 *   <li>If the tail-partial doc is alone in its run (runLen = 1 with tailMissing &gt; 0),
 *       same shape: only the visible portion (K - tailMissing ords) is written.</li>
 *   <li>If a single run spans the whole block with both head and tail partial (runLen = 1,
 *       block contains just one straddling doc), only K - headOffset - tailMissing ords
 *       are written.</li>
 * </ul>
 *
 * <p>Wire format after the encoding 3 header and sub-mode byte:
 * <pre>
 *   vint headOffset
 *   vint tailMissing
 *   vint nRuns
 *   for each run r in 0..nRuns - 1:
 *     vint K
 *     vint runLen
 *     vlong first
 *     vlong delta_1, ..., delta_{ordsInWire - 1}     // delta_i = ord_i - ord_{i-1} - 1
 * </pre>
 *
 * <p>{@code ordsInWire} is implicit per run; it is K for full runs and the visible portion
 * size for the edge cases above. The decoder derives it from r, runLen, headOffset,
 * tailMissing, and nRuns.
 *
 * <p>Stateless; access via {@link #INSTANCE}.
 */
public final class TupleRunCodec {

    /** Trailing-one-bits count for the ADAPTIVE_EXTRA dispatch. */
    public static final int ENCODING = 3;

    /** Sub-mode byte inside the ADAPTIVE_EXTRA dispatch. */
    public static final byte SUB_MODE = 3;

    public static final TupleRunCodec INSTANCE = new TupleRunCodec();

    private TupleRunCodec() {}

    /**
     * Estimates the exact byte cost of encoding the block. The estimate is byte-accurate so
     * a SortedSet codec can use it directly when picking between encodings.
     */
    public long estimateSize(final long[] ords, final int[] perDocK, int numDocs, int headOffset, int tailMissing) {
        if (numDocs == 0) {
            return Long.MAX_VALUE;
        }
        final RunBuilder runs = buildRuns(ords, perDocK, numDocs, headOffset, tailMissing);
        long size = 1L + 1L + vIntSize(headOffset) + vIntSize(tailMissing) + vIntSize(runs.count);
        for (int r = 0; r < runs.count; r++) {
            final int K = runs.runKs[r];
            final long[] tuple = runs.runTuples[r];
            final int ordsInWire = ordsInWire(r, K, runs.runLens[r], headOffset, tailMissing, runs.count);
            final int startK = (r == 0 && runs.runLens[r] == 1 && headOffset > 0) ? headOffset : 0;
            size += vIntSize(K) + vIntSize(runs.runLens[r]);
            size += vLongSize(tuple[startK]);
            for (int k = 1; k < ordsInWire; k++) {
                size += vLongSize(tuple[startK + k] - tuple[startK + k - 1] - 1L);
            }
        }
        return size;
    }

    /**
     * Encodes the block payload, including the leading vlong header and sub-mode byte.
     */
    public void encodePayload(final long[] ords, final int[] perDocK, int numDocs, int headOffset, int tailMissing, final DataOutput out)
        throws IOException {
        final RunBuilder runs = buildRuns(ords, perDocK, numDocs, headOffset, tailMissing);
        out.writeVLong(0b111);
        out.writeByte(SUB_MODE);
        out.writeVInt(headOffset);
        out.writeVInt(tailMissing);
        out.writeVInt(runs.count);
        for (int r = 0; r < runs.count; r++) {
            final int K = runs.runKs[r];
            final int runLen = runs.runLens[r];
            final long[] tuple = runs.runTuples[r];
            final int ordsInWire = ordsInWire(r, K, runLen, headOffset, tailMissing, runs.count);
            final int startK = (r == 0 && runLen == 1 && headOffset > 0) ? headOffset : 0;
            out.writeVInt(K);
            out.writeVInt(runLen);
            out.writeVLong(tuple[startK]);
            for (int k = 1; k < ordsInWire; k++) {
                out.writeVLong(tuple[startK + k] - tuple[startK + k - 1] - 1L);
            }
        }
    }

    /**
     * Decodes the payload. The wrapper has already consumed the leading vlong and dispatched
     * on its trailing one-bits count; the sub-mode byte has been consumed too. Fills
     * {@code out} (block-size positions) with the ord sequence produced by the tuple-runs.
     * Positions not reached by any run remain zero (final-block padding).
     */
    public void decodePayload(final DataInput in, final long[] out) throws IOException {
        Arrays.fill(out, 0L);
        final int headOffset = in.readVInt();
        final int tailMissing = in.readVInt();
        final int nRuns = in.readVInt();
        if (nRuns < 0 || nRuns > out.length + 1) {
            throw new CorruptIndexException(String.format(Locale.ROOT, "invalid tuple run count %d", nRuns), in);
        }
        int pos = 0;
        for (int r = 0; r < nRuns; r++) {
            final int K = in.readVInt();
            final int runLen = in.readVInt();
            if (K < 1 || runLen < 1) {
                throw new CorruptIndexException(String.format(Locale.ROOT, "invalid tuple-run K=%d runLen=%d", K, runLen), in);
            }
            final int ordsInWire = ordsInWire(r, K, runLen, headOffset, tailMissing, nRuns);
            if (ordsInWire < 1 || ordsInWire > K) {
                throw new CorruptIndexException(
                    String.format(Locale.ROOT, "invalid ordsInWire=%d for K=%d runLen=%d", ordsInWire, K, runLen),
                    in
                );
            }
            final int startK = (r == 0 && runLen == 1 && headOffset > 0) ? headOffset : 0;
            final long[] tuple = new long[K];
            tuple[startK] = in.readVLong();
            for (int k = 1; k < ordsInWire; k++) {
                tuple[startK + k] = tuple[startK + k - 1] + in.readVLong() + 1L;
            }
            final int totalOrds;
            final int cursor;
            if (r == 0 && r == nRuns - 1 && runLen == 1) {
                totalOrds = ordsInWire;
                cursor = startK;
            } else if (r == 0 && runLen == 1 && headOffset > 0) {
                totalOrds = ordsInWire;
                cursor = startK;
            } else if (r == nRuns - 1 && runLen == 1 && tailMissing > 0) {
                totalOrds = ordsInWire;
                cursor = 0;
            } else if (r == 0 && r == nRuns - 1) {
                totalOrds = runLen * K - headOffset - tailMissing;
                cursor = headOffset;
            } else if (r == 0 && headOffset > 0) {
                totalOrds = runLen * K - headOffset;
                cursor = headOffset;
            } else if (r == nRuns - 1 && tailMissing > 0) {
                totalOrds = runLen * K - tailMissing;
                cursor = 0;
            } else {
                totalOrds = runLen * K;
                cursor = 0;
            }
            if (totalOrds < 0 || pos + totalOrds > out.length) {
                throw new CorruptIndexException(
                    String.format(Locale.ROOT, "tuple-run overflow at r=%d: totalOrds=%d pos=%d outLen=%d", r, totalOrds, pos, out.length),
                    in
                );
            }
            int c = cursor;
            for (int i = 0; i < totalOrds; i++) {
                out[pos++] = tuple[c];
                c++;
                if (c == K) {
                    c = 0;
                }
            }
        }
    }

    private static int ordsInWire(int r, int K, int runLen, int headOffset, int tailMissing, int nRuns) {
        if (r == 0 && r == nRuns - 1 && runLen == 1) {
            return Math.max(1, K - headOffset - tailMissing);
        }
        if (r == 0 && runLen == 1 && headOffset > 0) {
            return K - headOffset;
        }
        if (r == nRuns - 1 && runLen == 1 && tailMissing > 0) {
            return K - tailMissing;
        }
        return K;
    }

    private static RunBuilder buildRuns(final long[] ords, final int[] perDocK, int numDocs, int headOffset, int tailMissing) {
        final RunBuilder runs = new RunBuilder(numDocs);
        int ordPos = 0;
        for (int d = 0; d < numDocs; d++) {
            final int K = perDocK[d];
            final int startInTuple = (d == 0) ? headOffset : 0;
            final int endInTuple = (d == numDocs - 1) ? K - tailMissing : K;
            final int inBlockOrds = endInTuple - startInTuple;

            boolean continues = false;
            if (runs.count > 0 && K == runs.prevK) {
                continues = true;
                final long[] prevTuple = runs.prevTuple;
                if (d == 1 && runs.count == 1 && headOffset > 0) {
                    // NOTE: doc 0's visible positions are [headOffset..K). If doc 1 matches
                    // them, doc 1's positions [0..headOffset) reveal doc 0's missing slots
                    // and the head-partial run extends.
                    for (int k = headOffset; k < K; k++) {
                        if (ords[ordPos + k] != prevTuple[k]) {
                            continues = false;
                            break;
                        }
                    }
                    if (continues) {
                        for (int k = 0; k < headOffset; k++) {
                            prevTuple[k] = ords[ordPos + k];
                        }
                    }
                } else {
                    for (int k = 0; k < inBlockOrds; k++) {
                        if (ords[ordPos + k] != prevTuple[startInTuple + k]) {
                            continues = false;
                            break;
                        }
                    }
                }
            }

            if (continues) {
                runs.runLens[runs.count - 1]++;
            } else {
                final long[] full = new long[K];
                System.arraycopy(ords, ordPos, full, startInTuple, inBlockOrds);
                runs.append(K, full);
            }
            ordPos += inBlockOrds;
        }
        return runs;
    }

    private static final class RunBuilder {
        int count;
        int prevK;
        long[] prevTuple;
        final long[][] runTuples;
        final int[] runKs;
        final int[] runLens;

        RunBuilder(int maxDocs) {
            this.runTuples = new long[Math.max(1, maxDocs)][];
            this.runKs = new int[Math.max(1, maxDocs)];
            this.runLens = new int[Math.max(1, maxDocs)];
            this.count = 0;
            this.prevK = -1;
        }

        void append(int K, long[] tuple) {
            runTuples[count] = tuple;
            runKs[count] = K;
            runLens[count] = 1;
            prevK = K;
            prevTuple = tuple;
            count++;
        }
    }

    private static int vIntSize(int value) {
        int bytes = 1;
        int unsigned = value;
        while ((unsigned & ~0x7F) != 0) {
            bytes++;
            unsigned >>>= 7;
        }
        return bytes;
    }

    private static int vLongSize(long value) {
        int bytes = 1;
        long unsigned = value;
        while ((unsigned & ~0x7FL) != 0) {
            bytes++;
            unsigned >>>= 7;
        }
        return bytes;
    }
}
