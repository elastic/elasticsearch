/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.queries;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.codec.tsdb.es95.ES95TSDBDocValuesFormatFactory;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingBinaryDocValues;

import java.io.IOException;

/**
 * Marks a query whose {@code matches()} opens a decoder over a field's binary doc values and decompresses whole
 * blocks into heap arrays — the {@code Scanning*} queries in this package, {@link BinaryDocValuesLengthQuery}, and
 * the wildcard module's {@code BinaryDvConfirmedQuery}.
 * <p>
 * {@link org.elasticsearch.search.internal.MaxClauseCountQueryVisitor} uses this to charge the real per-clause
 * decode cost instead of the generic per-leaf floor, so a query with thousands of these clauses is rejected up
 * front instead of OOMing.
 */
public interface BinaryDocValuesScanCost {

    /** Fallback for a genuine I/O error while probing; see {@link #realDecodeBytes}. */
    long PER_CLAUSE_DECODE_BYTES_ESTIMATE = ES95TSDBDocValuesFormatFactory.BINARY_BLOCK_BYTES_LARGE + (long) Integer.BYTES
        * (ES95TSDBDocValuesFormatFactory.BINARY_BLOCK_COUNT_LARGE + 1);

    /**
     * @return the field this query reads binary doc values from.
     */
    String field();

    /**
     * @param reader reader to probe for the field's real decode-block size via {@link BlockLoader.OptionalDecodeMemoryUsageEstimator},
     *               or {@code null} when unavailable.
     * @return the real per-field bound when {@code reader} is available, otherwise {@link TrackingBinaryDocValues#ESTIMATED_SIZE}
     *         — without a reader to probe, {@link #PER_CLAUSE_DECODE_BYTES_ESTIMATE} would overcharge the common case
     *         badly enough to reject queries that would otherwise have run fine.
     */
    static long estimateDecodeBytes(String field, @Nullable IndexReader reader) {
        // reader is null when building a query with no live searcher, e.g. percolator query indexing.
        return reader == null ? TrackingBinaryDocValues.ESTIMATED_SIZE : realDecodeBytes(field, reader);
    }

    /**
     * @return the max decode bytes for {@code field} across {@code reader}'s leaves: the real bound where the leaf's
     *         codec reports one, {@link TrackingBinaryDocValues#ESTIMATED_SIZE} otherwise (e.g. plain Lucene doc
     *         values), or {@code 0} if the field is absent everywhere. An I/O error falls back to the fully
     *         conservative {@link #PER_CLAUSE_DECODE_BYTES_ESTIMATE}.
     */
    private static long realDecodeBytes(String field, IndexReader reader) {
        long max = 0;
        for (LeafReaderContext leaf : reader.leaves()) {
            BinaryDocValues values;
            try {
                values = leaf.reader().getBinaryDocValues(field);
            } catch (IOException e) {
                // Genuine read failure (e.g. corrupt segment), not a normal "don't know" case; stay conservative.
                return PER_CLAUSE_DECODE_BYTES_ESTIMATE;
            }
            if (values == null) {
                continue;
            }
            long estimate = values instanceof BlockLoader.OptionalDecodeMemoryUsageEstimator hint ? hint.maxDecodeBytes() : -1;
            max = Math.max(max, estimate >= 0 ? estimate : TrackingBinaryDocValues.ESTIMATED_SIZE);
        }
        return max;
    }
}
