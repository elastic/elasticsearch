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
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.OrdinalMap;

import java.io.IOException;

/**
 * Writes a {@code Sorted} field's ordinal columns during segment merge at run granularity, the merge-time
 * counterpart of {@link SortedOrdinalWriter}. Created by {@link SortedOrdinalCodec#createMergeWriter()} so a
 * codec can offer merging alongside encoding and decoding; codecs that cannot merge at run granularity inherit
 * the no-op default and the merge falls back to the per-doc re-encode.
 *
 * <p>The terms dictionary and field framing are written by the consumer before this is invoked, so an
 * implementation writes only the ordinal columns, using the same encoding its {@link SortedOrdinalWriter} emits
 * at flush, keeping merge output byte-identical to a per-doc re-encode.
 */
public interface SortedOrdinalMergeWriter {

    /**
     * Writes the merged ordinal columns for {@code field} at run granularity into {@code ctx}, returning
     * {@code true} if handled. Returns {@code false} without writing when the field is not eligible or a source
     * segment did not store it run-table encoded, in which case the caller performs the per-doc merge.
     *
     * @param field       the merged field being written
     * @param mergeState  the merge state carrying the source segments
     * @param ordinalMap  the field's ordinal map (source-local to merged-global ordinal)
     * @param ctx         the write context for the merged segment
     * @param maxOrd      the merged field cardinality; the absent sentinel ordinal equals this value
     */
    boolean writeMergedOrdinals(FieldInfo field, MergeState mergeState, OrdinalMap ordinalMap, NumericWriteContext ctx, long maxOrd)
        throws IOException;
}
