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
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingBinaryDocValues;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Marks a query whose {@code matches()} opens a decoder over a field's binary doc values and decompresses whole
 * blocks into heap arrays — the {@code Scanning*} queries in this package, {@link BinaryDocValuesLengthQuery}, and
 * the wildcard module's {@code BinaryDvConfirmedQuery}.
 * <p>
 * {@link org.elasticsearch.search.internal.MaxClauseCountQueryVisitor} uses this to account for the real per-clause
 * decode cost instead of the generic per-leaf floor, so a query with thousands of these clauses is rejected up
 * front instead of OOMing.
 */
public interface BinaryDocValuesScanCost {

    /**
     * @return the field this query reads binary doc values from.
     */
    String field();

    /**
     * @param reader reader to probe for the field's real decode-block size via {@link BlockLoader.OptionalDecodeMemoryUsageEstimator},
     *               or {@code null} when unavailable.
     * @return {@link TrackingBinaryDocValues#ESTIMATED_SIZE} when {@code reader} is {@code null}. Otherwise the max decode
     *         bytes for {@code field} across {@code reader}'s leaves: the real bound where the leaf's codec reports one,
     *         {@link TrackingBinaryDocValues#ESTIMATED_SIZE} otherwise (e.g. plain Lucene doc values), or {@code 0} if the
     *         field is absent everywhere.
     * @throws UncheckedIOException if a leaf fails to read its binary doc values — a genuine problem (e.g. a
     *                               corrupt segment), not a "don't know" case we can safely estimate around.
     */
    static long estimateDecodeBytes(String field, @Nullable IndexReader reader) {
        // reader is null when building a query with no live searcher, e.g. percolator query indexing.
        if (reader == null) {
            return TrackingBinaryDocValues.ESTIMATED_SIZE;
        }
        long max = 0;
        for (LeafReaderContext leaf : reader.leaves()) {
            BinaryDocValues values;
            try {
                values = leaf.reader().getBinaryDocValues(field);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
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
