/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

/**
 * Factory for block-level encoders and decoders of the ordinal stream in SORTED and SORTED_SET
 * doc values.
 *
 * <p>TSDB doc values are stored in fixed-size blocks. This interface controls how each block
 * of ordinals is encoded on write and decoded on read. The field-level layout (block index,
 * DISI, metadata) is handled by {@link TSDBDocValuesBlockWriter} and
 * {@link TSDBDocValuesBlockReader}; this interface only provides the per-block encoding strategy.
 *
 * <p>An instance is held by {@link AbstractTSDBDocValuesProducer} and
 * {@link AbstractTSDBDocValuesConsumer} for the lifetime of a segment and consulted once per
 * field. Implementations should return fresh instances to avoid shared mutable state across
 * merge threads.
 */
public interface OrdinalBlockCodec {

    /**
     * Returns a reader that can decode ordinal value blocks for a field in this segment.
     *
     * @param ctx segment-scoped read state shared by every field in this segment
     * @return    the block-level field reader
     */
    OrdinalFieldReader createReader(NumericReadContext ctx);

    /**
     * Returns a writer that can encode ordinal value blocks for a field in this segment.
     *
     * @param ctx segment-scoped write state shared by every field in this segment
     * @return    the block-level field writer
     */
    OrdinalFieldWriter createWriter(NumericWriteContext ctx);
}
