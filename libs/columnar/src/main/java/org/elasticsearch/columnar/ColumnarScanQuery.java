/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

/**
 * Marks a query that answers by looking at the values of a ColumNAR column, rather than by an inverted index.
 *
 * <p>Such a query can reach every document of a segment, and confirming one is paid for in decoded column data, so
 * where it sits in a boolean query matters. {@link ColumnarNegatedQuery} uses the mark to tell an exclusion worth
 * reordering from one Lucene already handles well.
 */
public interface ColumnarScanQuery {

    /**
     * Whether, as an exclusion, this query is expensive to confirm because it has to read the values of documents it
     * cannot rule out beforehand.
     *
     * <p>Where the answer is no, Lucene's own handling of an exclusion is already a good fit: the query reaches only a
     * short list of candidates, so walking them costs little however few documents the other clauses keep. Requiring
     * it instead then adds a check on every document and gives up skipping a run of excluded documents at once, for
     * nothing in return.
     *
     * <p>The default is yes, which is the safe answer: the reordering never changes which documents match, so being
     * wrong costs time, not results.
     */
    default boolean costlyToExclude() {
        return true;
    }
}
