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
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.columnar.substrate.MonotonicWriter;

import java.io.Closeable;
import java.io.IOException;

/**
 * Builds the two tables that say where a document's slots are and which of them are null — see
 * {@link StringColumnMetadata.Addressing}. Both go through {@link MonotonicWriter}, which streams them to a
 * temporary file, so nothing column-proportional is held on the heap.
 *
 * <p>Held apart from the layouts because the question it answers is the same for all of them: a dictionary
 * column names its values with ordinals, but a document's slots are found and a null slot is recognised
 * exactly as they are in a column that stores its values.
 */
final class AddressingWriter implements Closeable {

    /** Null when every document holds exactly one slot, in which case a document's value address is its rank. */
    private final MonotonicWriter valueAddresses;
    /** Null when no slot in the column is null. */
    private final MonotonicWriter nullSlots;
    private final long numNullSlots;

    private long nulls;

    /**
     * @param numDocsWithField documents that have at least one slot
     * @param numValues        slots across all of them, null slots included
     * @param numNullSlots     how many of those slots are null
     */
    static AddressingWriter open(
        int numDocsWithField,
        long numValues,
        long numNullSlots,
        Directory directory,
        IOContext context,
        String name
    ) throws IOException {
        MonotonicWriter valueAddresses = null;
        MonotonicWriter nullSlots = null;
        try {
            // A document holding several slots and one holding none both put the slots out of step with the
            // documents, and either way a rank stops being its own value address. One past the end, so the
            // last document's slot count is a difference like any other.
            if (numValues != numDocsWithField) {
                valueAddresses = new MonotonicWriter(directory, context, name, numDocsWithField + 1L);
            }
            if (numNullSlots > 0) {
                nullSlots = new MonotonicWriter(directory, context, name, numNullSlots);
            }
            return new AddressingWriter(valueAddresses, nullSlots, numNullSlots);
        } catch (Exception e) {
            IOUtils.closeWhileHandlingException(valueAddresses, nullSlots);
            throw e;
        }
    }

    private AddressingWriter(MonotonicWriter valueAddresses, MonotonicWriter nullSlots, long numNullSlots) {
        this.valueAddresses = valueAddresses;
        this.nullSlots = nullSlots;
        this.numNullSlots = numNullSlots;
    }

    /** Records that the document about to be written begins at {@code valueAddress}. */
    void startDocument(long valueAddress) throws IOException {
        if (valueAddresses != null) {
            valueAddresses.add(valueAddress);
        }
    }

    /** Records what the slot at {@code valueAddress} is; only a null leaves a trace. */
    void slot(long valueAddress, boolean isNull) throws IOException {
        if (isNull) {
            assert nullSlots != null : "null slot at [" + valueAddress + "] in a column counted as having none";
            nullSlots.add(valueAddress);
            nulls++;
        }
    }

    /** Closes both tables into {@code data}, {@code numValues} being the address one past the column's last slot. */
    StringColumnMetadata.Addressing finish(long numValues, IndexOutput data) throws IOException {
        assert nulls == numNullSlots : "wrote " + nulls + " null slots, counted " + numNullSlots;
        if (valueAddresses != null) {
            valueAddresses.add(numValues);
        }
        return new StringColumnMetadata.Addressing(
            numNullSlots,
            valueAddresses == null ? MonotonicWriter.Table.NONE : valueAddresses.finish(data),
            nullSlots == null ? MonotonicWriter.Table.NONE : nullSlots.finish(data)
        );
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(valueAddresses, nullSlots);
    }
}
