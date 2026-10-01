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

import java.io.IOException;

/**
 * Marks a query whose {@code matches()} opens a decoder over a field's binary doc values and decompresses whole
 * blocks into heap arrays — the {@code Scanning*} queries in this package, {@link BinaryDocValuesLengthQuery}, and
 * the wildcard module's {@code BinaryDvConfirmedQuery}.
 * <p>
 * {@link org.elasticsearch.search.internal.MaxClauseCountQueryVisitor} uses this interface to charge that real cost
 * per clause instead of the generic per-leaf floor, so a query with thousands of these clauses is rejected up front
 * instead of OOMing.
 */
public interface BinaryDocValuesScanCost {

    /** Conservative fallback when a real per-field bound isn't available; see {@link #estimateDecodeBytes}. */
    long PER_CLAUSE_DECODE_BYTES_ESTIMATE = ES95TSDBDocValuesFormatFactory.BINARY_BLOCK_BYTES_LARGE + (long) Integer.BYTES
        * (ES95TSDBDocValuesFormatFactory.BINARY_BLOCK_COUNT_LARGE + 1);

    /**
     * @return the field this query reads binary doc values from.
     */
    String field();

    /**
     * @param segmentCount unused by the default estimate; kept so a future concurrency-scaled estimate doesn't need
     *                      to change every caller.
     * @param reader reader to probe for the field's real decode-block size via {@link BlockLoader.OptionalDecodeSizeHint},
     *               or {@code null} when unavailable.
     * @return the real per-field bound when every leaf holding the field supports it, otherwise the fixed estimate.
     */
    default long estimateDecodeBytes(int segmentCount, @Nullable IndexReader reader) {
        return reader == null ? PER_CLAUSE_DECODE_BYTES_ESTIMATE : realDecodeBytes(field(), reader);
    }

    /**
     * @return the real max decode bytes for {@code field} across {@code reader}'s leaves, or the conservative
     *         fallback if any leaf holding the field can't report it — callers must not trust a partial answer.
     *         {@code 0} if the field is absent from every leaf: no decoder will ever open for it against this reader.
     */
    private static long realDecodeBytes(String field, IndexReader reader) {
        long max = 0;
        boolean sawData = false;
        for (LeafReaderContext leaf : reader.leaves()) {
            BinaryDocValues values;
            try {
                values = leaf.reader().getBinaryDocValues(field);
            } catch (IOException e) {
                return PER_CLAUSE_DECODE_BYTES_ESTIMATE;
            }
            if (values == null) {
                continue;
            }
            if (values instanceof BlockLoader.OptionalDecodeSizeHint hint) {
                sawData = true;
                max = Math.max(max, hint.maxDecodeBytes());
            } else {
                return PER_CLAUSE_DECODE_BYTES_ESTIMATE;
            }
        }
        return sawData ? max : 0;
    }
}
