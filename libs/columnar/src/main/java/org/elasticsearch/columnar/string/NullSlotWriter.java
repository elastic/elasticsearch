/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.columnar.substrate.MonotonicWriter;

import java.io.IOException;

/**
 * Builds the table of value addresses holding a null, for {@link StringColumnLayout#PLAIN} alone.
 *
 * <p>A plain column stores a null as a zero-length value, which is the same bytes an empty string stores, so
 * without this table the two are indistinguishable. {@link StringColumnLayout#DICTIONARY} has no such
 * problem: it names a null with a reserved ordinal below every term and needs no table at all.
 */
final class NullSlotWriter extends SlotTableWriter {

    private final long numNullSlots;

    /** @param numNullSlots how many of the column's slots are null */
    static NullSlotWriter open(long numNullSlots, Directory directory, IOContext context, String name) throws IOException {
        return new NullSlotWriter(numNullSlots > 0 ? new MonotonicWriter(directory, context, name, numNullSlots) : null, numNullSlots);
    }

    private NullSlotWriter(MonotonicWriter nullSlots, long numNullSlots) {
        super(nullSlots);
        this.numNullSlots = numNullSlots;
    }

    /** Records that the slot at {@code valueAddress} is null; a slot that holds a value leaves no trace. */
    void recordNull(long valueAddress) throws IOException {
        add(valueAddress);
    }

    /**
     * Closes the table into {@code data}, or {@link MonotonicWriter.Table#NONE} when nothing was null.
     * Checked rather than asserted, for the same reason the addressing table checks its own totals: a
     * cursor that miscounts its nulls would otherwise write a table the reader trusts.
     */
    MonotonicWriter.Table finish(IndexOutput data) throws IOException {
        if (written() != numNullSlots) {
            throw new IllegalStateException("wrote " + written() + " null slots, counted " + numNullSlots);
        }
        return finishTable(data);
    }
}
