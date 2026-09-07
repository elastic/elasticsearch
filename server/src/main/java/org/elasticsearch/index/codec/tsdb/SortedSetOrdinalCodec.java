/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.store.IndexInput;

/**
 * Factory for the writer and reader of the ordinal stream of multi-valued {@code SortedSet} fields.
 *
 * <p>An instance is held by {@link AbstractTSDBDocValuesProducer} and
 * {@link AbstractTSDBDocValuesConsumer} for the lifetime of a segment and consulted once per field.
 * Symmetric with {@link SortedOrdinalCodec}; the reader produces a multi-valued ordinal stream.
 * Implementations should return fresh instances so each field owns its own state without sharing
 * mutable buffers across merge threads.
 */
public interface SortedSetOrdinalCodec {

    /**
     * Returns a writer that encodes the ordinal stream of a sorted-set field in this segment.
     *
     * @param ctx segment-scoped write state shared by every field in this segment
     * @return    the sorted-set ordinal field writer
     */
    SortedSetOrdinalWriter createWriter(NumericWriteContext ctx);

    /**
     * Returns a reader that decodes the ordinal stream of a sorted-set field in this segment.
     *
     * @param ctx    segment-scoped read state shared by every field in this segment
     * @param data   the shared segment data input; the reader may clone it for independent sequential reads
     * @param maxDoc the document count for this segment
     * @return       the sorted-set ordinal field reader
     */
    SortedSetOrdinalReader createReader(NumericReadContext ctx, IndexInput data, int maxDoc);

    /**
     * Returns the merge-time writer that encodes a sorted-set field's ordinal columns at run granularity during
     * segment merge. The default returns a no-op that reports "not handled", so a codec without a run-granularity
     * merge falls back to the per-doc re-encode; run-table codecs override this.
     *
     * @return the sorted-set ordinal merge writer
     */
    default SortedSetOrdinalMergeWriter createMergeWriter() {
        return (field, mergeState, ordinalMap, ctx, maxOrd, mergeStats) -> false;
    }
}
