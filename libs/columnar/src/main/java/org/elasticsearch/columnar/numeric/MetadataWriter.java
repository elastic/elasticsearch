/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

/**
 * Write-side view of per-block stage metadata for a {@link BlockTransform}. Exposes only the scalar
 * operations valid for compact reversal parameters — a fixed or variable-length integer, a
 * zigzag-encoded signed value, etc. Bulk-copy and stream-bridging operations present on
 * {@link org.apache.lucene.store.DataOutput} are intentionally absent: stages should never write
 * arbitrary bytes through this interface.
 *
 * <p>The on-disk layout within a block is managed by {@link NumericBlockEncoder}: each transform
 * writes to its own private {@link MetadataBuffer}, and the encoder flushes them in reverse
 * pipeline order after the terminal payload so the decoder reads forward without seeking. Stages
 * are fully decoupled from that ordering decision.
 *
 * <p>Methods return {@code this} to allow chaining, but transforms are not required to chain.
 *
 * @see MetadataReader
 * @see MetadataBuffer
 */
public interface MetadataWriter {

    /** Writes a single byte. */
    MetadataWriter writeByte(byte v);

    /** Writes a fixed-width 4-byte big-endian integer. */
    MetadataWriter writeInt(int v);

    /** Writes a fixed-width 8-byte big-endian long. */
    MetadataWriter writeLong(long v);

    /**
     * Writes an unsigned variable-length integer. {@code v} must be non-negative; use
     * {@link #writeZInt} for signed values.
     */
    MetadataWriter writeVInt(int v);

    /**
     * Writes an unsigned variable-length long. {@code v} must be non-negative; use
     * {@link #writeZLong} for signed values.
     */
    MetadataWriter writeVLong(long v);

    /** Writes a signed variable-length integer using zigzag encoding. */
    MetadataWriter writeZInt(int v);

    /** Writes a signed variable-length long using zigzag encoding. */
    MetadataWriter writeZLong(long v);
}
