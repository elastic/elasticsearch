/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

import java.io.IOException;

/**
 * Per-mode encoder/decoder for one block of ordinals. The enclosing ordinal codec wrapper
 * picks the cheapest implementation via exact byte cost, delegates the full payload write
 * (including the leading vlong header), then on decode reads the leading vlong itself and
 * dispatches by the trailing one-bits count.
 *
 * <p>The header is a vlong whose trailing one-bits count selects the encoding:
 * <ul>
 *   <li>0: CONST (single run, value embedded in the header bits)</li>
 *   <li>1: TWO_RUN (first ord embedded; run length and delta follow)</li>
 *   <li>2: BIT_PACKED (full block packed at the segment-global bitsPerOrd)</li>
 *   <li>3: ADAPTIVE_EXTRA (sub-mode byte selects RLE_N, BITPACK_LOCAL, or TUPLE_RUN)</li>
 *   <li>4: CYCLE_COMPACT (period rides in bits 5 and above of the same vlong)</li>
 * </ul>
 *
 * <p>Implementations are stateless singletons; any mutable scratch buffers required during
 * encode or decode are supplied via the shared {@link CodecContext} threaded through
 * {@link #encodePayload} and {@link #decodePayload}.
 */
sealed interface BlockModeCodec permits ConstantCodec, TwoRunCodec, BitPackedCodec, RleCodec, BitpackCodec, CycleCodec {

    /** The wire-format encoding identifier derived from trailing one-bits. */
    int encoding();

    /**
     * Returns the estimated payload byte cost of encoding {@code in} given
     * the precomputed {@code stats}. The estimate includes the leading
     * vlong header and any sub-mode byte and must equal the actual bytes
     * written by {@link #encodePayload}. Returns {@link Long#MAX_VALUE}
     * when this codec does not apply to the block.
     */
    long estimateSize(long[] in, BlockStats stats, int bitsPerOrd);

    /**
     * Encodes the full payload, including the leading vlong header and (for
     * encoding 3) the sub-mode byte. The wrapper does not write any prefix.
     */
    void encodePayload(long[] in, BlockStats stats, CodecContext ctx, DataOutput out, int bitsPerOrd) throws IOException;

    /**
     * Decodes the payload. The wrapper has already consumed the leading
     * vlong and dispatched on its trailing one-bits count; the consumed
     * value is passed back as {@code leadingVLong} so the codec can recover
     * the header bits (CONST value, TWO_RUN first ord, etc.).
     */
    void decodePayload(CodecContext ctx, DataInput in, long[] out, int bitsPerOrd, long leadingVLong) throws IOException;
}
