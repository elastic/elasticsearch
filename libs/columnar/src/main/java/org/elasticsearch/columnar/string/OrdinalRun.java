/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import java.io.IOException;

/**
 * The slots a merge cursor is about to hand over that come from one dictionary column, in the order that column holds
 * them and with nothing in between, every one of them a term the column being written also names, or a null. A writer
 * can then {@link #read} their ordinals a block at a time, already translated, instead of asking for each value's
 * ordinal on its own.
 */
public final class OrdinalRun {

    private final DictionaryStringColumnReader source;
    private final int[] ordinalMap;
    private final long from;
    private final long to;
    /** The next slot {@link #read} hands over. */
    private long at;

    OrdinalRun(DictionaryStringColumnReader source, int[] ordinalMap, long from, long to) {
        assert from < to : "[" + from + ", " + to + ")";
        assert source.escapeCount() == 0 : "a column that let values escape has slots no ordinal names";
        this.source = source;
        this.ordinalMap = ordinalMap;
        this.from = from;
        this.to = to;
        this.at = from;
    }

    /** How many slots the run covers, starting at the cursor's current document. */
    public long slots() {
        return to - from;
    }

    /**
     * Reads the next of the run's ordinals into {@code dst}, translated into those of the column being written, and
     * answers how many, or {@code 0} once the run is read. A null slot stays
     * {@link StringColumnMetadata.Dictionary#NULL_ORDINAL}, which both columns reserve for it. A read stops at the end
     * of the source's block, so it may fill less of {@code dst} than there is room for while slots remain.
     */
    int read(int[] dst) throws IOException {
        if (at == to) {
            return 0;
        }
        final int count = source.translateOrdinals(at, (int) Math.min(dst.length, to - at), ordinalMap, dst);
        at += count;
        return count;
    }
}
