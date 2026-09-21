/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.elasticsearch.columnar.substrate.MonotonicWriter;

import java.io.IOException;

/**
 * A {@code DirectMonotonic} table of value addresses that a column may or may not need, written as its entries
 * arrive. A column that needs none writes nothing, and a caller that miscounts what it is about to write must
 * not be able to leave a table the reader would trust, so the entries are counted either way.
 */
abstract class SlotTableWriter {

    /** Null when the column does not need this table, in which case nothing is written. */
    private final MonotonicWriter table;

    /** Entries added so far, which each subclass checks against the total it was opened for. */
    private long written;

    SlotTableWriter(MonotonicWriter table) {
        this.table = table;
    }

    /**
     * Records {@code valueAddress} as the next entry. Addresses must arrive in non-decreasing order.
     *
     * <p>Counted whether or not there is a table to put it in, so a column that needs none is still checked
     * against the total it was opened for — the entries are what say the caller wrote what it said it would,
     * and that has to hold either way.
     */
    final void add(long valueAddress) throws IOException {
        if (table != null) {
            table.add(valueAddress);
        }
        written++;
    }

    /** How many entries have been added. */
    final long written() {
        return written;
    }

    /**
     * Finishes the table, or answers {@link MonotonicWriter.Table#NONE} when the column
     * needs none. Subclasses check their totals before calling this.
     */
    final MonotonicWriter.Table finishTable() throws IOException {
        return table == null ? MonotonicWriter.Table.NONE : table.finish();
    }

    /** Adds a final entry, for a table whose last value is a sentinel rather than an address of its own. */
    final void addSentinel(long value) throws IOException {
        if (table != null) {
            table.add(value);
        }
    }
}
