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

import java.io.Closeable;
import java.io.IOException;

/**
 * Builds the table of value addresses holding a null, for {@link StringColumnLayout#PLAIN} alone.
 *
 * <p>A plain column stores a null as a zero-length value, which is the same bytes an empty string stores, so
 * without this table the two are indistinguishable. {@link StringColumnLayout#DICTIONARY} has no such
 * problem: it names a null with an ordinal above every term and needs no table at all.
 *
 * <p>Like the value addresses it accompanies the table goes through {@link MonotonicWriter}, which streams it
 * to a temporary file. A column with no null slot opens no file.
 */
final class NullSlotWriter implements Closeable {

    /** Null when no slot in the column is null. */
    private final MonotonicWriter nullSlots;
    private final long numNullSlots;

    private long nulls;

    /** @param numNullSlots how many of the column's slots are null */
    static NullSlotWriter open(long numNullSlots, Directory directory, IOContext context, String name) throws IOException {
        return new NullSlotWriter(numNullSlots > 0 ? new MonotonicWriter(directory, context, name, numNullSlots) : null, numNullSlots);
    }

    private NullSlotWriter(MonotonicWriter nullSlots, long numNullSlots) {
        this.nullSlots = nullSlots;
        this.numNullSlots = numNullSlots;
    }

    /** Records that the slot at {@code valueAddress} is null; a slot that holds a value leaves no trace. */
    void recordNull(long valueAddress) throws IOException {
        assert nullSlots != null : "null slot at [" + valueAddress + "] in a column counted as having none";
        nullSlots.add(valueAddress);
        nulls++;
    }

    /** Closes the table into {@code data}, or {@link MonotonicWriter.Table#NONE} when nothing was null. */
    MonotonicWriter.Table finish(IndexOutput data) throws IOException {
        assert nulls == numNullSlots : "wrote " + nulls + " null slots, counted " + numNullSlots;
        return nullSlots == null ? MonotonicWriter.Table.NONE : nullSlots.finish(data);
    }

    @Override
    public void close() throws IOException {
        if (nullSlots != null) {
            nullSlots.close();
        }
    }
}
