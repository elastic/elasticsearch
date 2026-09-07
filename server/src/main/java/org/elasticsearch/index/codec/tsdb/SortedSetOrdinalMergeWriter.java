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
 * Writes a multi-valued {@code SortedSet} field's ordinal columns during segment merge at run granularity, the
 * merge-time counterpart of {@link SortedSetOrdinalWriter}. Created by
 * {@link SortedSetOrdinalCodec#createMergeWriter()} so a codec can offer merging alongside encoding and decoding;
 * codecs that cannot merge at run granularity inherit the no-op default and the merge falls back to the per-doc
 * re-encode.
 *
 * <p>The field type byte is written by the consumer before this is invoked and the terms dictionary after it, so
 * an implementation writes only the ordinal metadata and columns (the same {@code numDocsWithField}/{@code
 * numValues} header plus layout its {@link SortedSetOrdinalWriter} emits at flush), keeping merge output
 * byte-identical to a per-doc re-encode. The pre-computed {@code mergeStats} carry the merged
 * {@code numDocsWithField} and {@code numValues}, which for a delete-free merge equal the per-doc-merged counts.
 */
public interface SortedSetOrdinalMergeWriter {

    /**
     * Writes the merged ordinal metadata and columns for {@code field} at run granularity into {@code ctx},
     * returning {@code true} if handled. Returns {@code false} without writing when the field is not eligible or a
     * source segment did not store it run-table encoded, in which case the caller performs the per-doc merge.
     */
    boolean writeMergedOrdinals(
        FieldInfo field,
        MergeState mergeState,
        OrdinalMap ordinalMap,
        NumericWriteContext ctx,
        long maxOrd,
        DocValuesConsumerUtil.MergeStats mergeStats
    ) throws IOException;
}
