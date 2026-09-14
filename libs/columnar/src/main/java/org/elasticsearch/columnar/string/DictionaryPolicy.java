/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

/**
 * When a string column is worth storing as a dictionary, and how large that dictionary may get.
 *
 * <p>The size is bounded in bytes rather than in terms, because a term count says nothing about what a
 * dictionary costs: eight thousand log levels and eight thousand URLs are three orders of magnitude apart.
 * Long terms therefore yield a smaller dictionary, which is the intended behaviour, and the bound also caps
 * what the survey holds while it is choosing. The width of an ordinal does not enter into it: a block is
 * packed to the widest ordinal it actually contains.
 *
 * <p>Whether to keep it is then two questions. {@link #minCoverage} asks whether enough of the column's
 * values are in the dictionary for an ordinal to be worth reading. {@link #maxShareOfColumn} asks whether
 * the dictionary is small against the data it describes — a dictionary as large as the values it stands in
 * for has bought nothing, however well it covers them.
 *
 * @param maxBytes         the most term bytes a dictionary may hold
 * @param minCoverage      the share of a column's values the dictionary must account for
 * @param maxShareOfColumn the largest share of the column's value bytes the dictionary may occupy
 */
public record DictionaryPolicy(int maxBytes, double minCoverage, double maxShareOfColumn) {

    /** Never builds a dictionary. */
    public static final DictionaryPolicy NONE = new DictionaryPolicy(0, 1.0, 0.0);

    public DictionaryPolicy {
        if (maxBytes < 0) {
            throw new IllegalArgumentException("dictionary bounds must not be negative");
        }
        if (minCoverage < 0.0 || minCoverage > 1.0) {
            throw new IllegalArgumentException("minCoverage must be a share, got " + minCoverage);
        }
        if (maxShareOfColumn < 0.0) {
            throw new IllegalArgumentException("maxShareOfColumn must not be negative, got " + maxShareOfColumn);
        }
    }

    public boolean enabled() {
        return maxBytes > 0;
    }

    /**
     * The term bytes a dictionary for a column of {@code columnBytes} may hold. A dictionary is bounded
     * both absolutely and relative to what it describes, and the tighter of the two governs: filling the
     * absolute bound with rare terms would leave a dictionary as large as the column it stands in for.
     */
    public long budgetFor(long columnBytes) {
        return Math.min(maxBytes, (long) (maxShareOfColumn * columnBytes));
    }

    /** Whether a surveyed vocabulary is worth keeping. */
    boolean worthKeeping(double coverage, long dictionaryBytes, long columnBytes) {
        return coverage >= minCoverage && dictionaryBytes <= maxShareOfColumn * columnBytes;
    }
}
