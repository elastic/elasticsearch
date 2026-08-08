/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

/**
 * Shared constants and selection heuristic for the run-table ordinal codecs. The {@link RunTableSortedCodec}
 * and {@link RunTableSortedSetCodec} write the same discriminator byte at the front of a field's ordinal
 * metadata and apply the same run-count selection bar, so those pieces live here rather than being duplicated.
 * The two codecs and their writer/reader interfaces otherwise stay separate.
 */
final class RunTableLayout {

    private RunTableLayout() {}

    /** Discriminator: the fallback codec owns this field's ordinal stream. */
    static final byte LAYOUT_DEFAULT = 0;
    /** Discriminator: the run-table layout follows. */
    static final byte LAYOUT_RUN_TABLE = 1;

    /**
     * Whether the run table is compact enough to select: an average run must span at least two docs over
     * the full doc space. This keeps the layout for contiguous runs and rejects it for scattered absence
     * or degenerate near-churn where runs approach docs.
     *
     * <p>This is the logical complement of {@code RunTableSortedOrdinalWriter.exceedsThreshold} and
     * {@code RunTableSortedSetOrdinalWriter.exceedsThreshold}, which the codec writers call directly on
     * the in-progress accumulator during the doc walk. This method is used in tests.
     */
    static boolean fitsRunTable(int numRuns, int maxDoc) {
        return (long) numRuns * 2 <= maxDoc;
    }
}
