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
 * Per-mode encoder/decoder for one block of ordinals. The enclosing
 * AdaptiveOrdinalCodec wrapper writes the mode byte and dispatches to the
 * cheapest codec; each implementation handles only its own payload bytes.
 */
sealed interface BlockModeCodec permits LegacyCodec, ConstantCodec, RleCodec, BitpackCodec {

    /** The wire-format mode byte that identifies this codec. */
    byte mode();

    /**
     * Returns the estimated payload byte cost of encoding {@code in} given
     * the precomputed {@code stats}. Returns {@link Long#MAX_VALUE} when this
     * codec does not apply to the block.
     */
    long estimateSize(long[] in, BlockStats stats, int bitsPerOrd);

    /** Encodes the payload (no mode byte). Caller has already written the mode byte. */
    void encodePayload(long[] in, BlockStats stats, DataOutput out, int bitsPerOrd) throws IOException;

    /** Decodes the payload (no mode byte). Caller has already consumed the mode byte. */
    void decodePayload(DataInput in, long[] out, int bitsPerOrd) throws IOException;
}
