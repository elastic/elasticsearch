/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.index.FieldInfo;

/**
 * Factory for block-level encoders and decoders of the terms dictionary in SORTED and SORTED_SET
 * doc values.
 *
 * <p>TSDB doc values store the terms dictionary in fixed-size blocks; each block body is either
 * LZ4-compressed against the block's first term or written raw. The two implementations are
 * stateless singletons exposed through the per-field {@link TermsDictFieldReader} and
 * {@link TermsDictFieldWriter} returned by this factory. Dispatch keys on the field name plus
 * the segment-level {@code skipTsidLz4Encoding} flag carried in the context records.
 *
 * <p>An instance is held by {@link AbstractTSDBDocValuesProducer} and
 * {@link AbstractTSDBDocValuesConsumer} for the lifetime of a segment and consulted once per
 * field. Implementations should return fresh instances to avoid shared mutable state across
 * merge threads.
 */
public interface TermsDictBlockCodec {

    /**
     * Returns a reader that can decode terms-dictionary blocks for {@code field} in this segment.
     *
     * @param ctx   segment-scoped read state shared by every field in this segment
     * @param field the field whose terms dictionary is about to be read
     * @return      the per-field terms-dictionary reader
     */
    TermsDictFieldReader createReader(TermsDictReadContext ctx, FieldInfo field);

    /**
     * Returns a writer that can encode terms-dictionary blocks for {@code field} in this segment.
     *
     * @param ctx   segment-scoped write state shared by every field in this segment
     * @param field the field whose terms dictionary is about to be written
     * @return      the per-field terms-dictionary writer
     */
    TermsDictFieldWriter createWriter(TermsDictWriteContext ctx, FieldInfo field);
}
