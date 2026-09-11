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

import java.io.Closeable;
import java.io.IOException;

/**
 * A {@code DirectMonotonic} table of value addresses that a column may or may not need, streamed to a
 * temporary file rather than held on the heap. Holds what {@link AddressingWriter} and {@link NullSlotWriter}
 * share — an optional writer, the entries put into it, and closing it — and leaves each of them the part that
 * is theirs: when the table is worth writing at all, and what has to hold before it is closed.
 *
 * <p>Those differ more than they look. One table has an entry for every document and ends with a sentinel;
 * the other has one only for the addresses that hold a null and ends where it ends. What they have in common
 * is that a column that needs neither opens no file, and that a caller which miscounts what it is about to
 * write must not be able to leave a table the reader would trust.
 */
abstract class SlotTableWriter implements Closeable {

    /** Null when the column does not need this table, in which case nothing is written and no file is opened. */
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
     * Closes the table into the data output, or answers {@link MonotonicWriter.Table#NONE} when the column
     * needs none. Subclasses check their totals before calling this.
     */
    final MonotonicWriter.Table finishTable(org.apache.lucene.store.IndexOutput data) throws IOException {
        return table == null ? MonotonicWriter.Table.NONE : table.finish(data);
    }

    /** Adds a final entry, for a table whose last value is a sentinel rather than an address of its own. */
    final void addSentinel(long value) throws IOException {
        if (table != null) {
            table.add(value);
        }
    }

    @Override
    public final void close() throws IOException {
        if (table != null) {
            table.close();
        }
    }
}
