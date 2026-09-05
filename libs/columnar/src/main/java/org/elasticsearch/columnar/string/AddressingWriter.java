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
 * Builds the table that says where each document's slots begin. It goes through {@link MonotonicWriter},
 * which streams it to a temporary file, so nothing column-proportional is held on the heap.
 *
 * <p>Every layout writes this one, because finding a document's slots is the same question whichever layout
 * names the values: a dictionary column names its with ordinals, but its documents are addressed exactly as
 * they are in a column that stores its values. Which of those slots are null is <em>not</em> a shared
 * question — a dictionary has a spare ordinal to name a null with, and only {@link StringColumnLayout#PLAIN}
 * needs {@link NullSlotWriter}.
 */
final class AddressingWriter implements Closeable {

    /** Null when every document holds exactly one slot, in which case a document's value address is its rank. */
    private final MonotonicWriter valueAddresses;
    private final int numDocsWithField;

    private int docs;

    /**
     * @param numDocsWithField documents that have at least one slot
     * @param numValues        slots across all of them, null slots included
     */
    static AddressingWriter open(int numDocsWithField, long numValues, Directory directory, IOContext context, String name)
        throws IOException {
        // A document holding several slots and one holding none both put the slots out of step with the
        // documents, and either way a rank stops being its own value address. One past the end, so the last
        // document's slot count is a difference like any other.
        final MonotonicWriter valueAddresses = numValues != numDocsWithField
            ? new MonotonicWriter(directory, context, name, numDocsWithField + 1L)
            : null;
        return new AddressingWriter(valueAddresses, numDocsWithField);
    }

    private AddressingWriter(MonotonicWriter valueAddresses, int numDocsWithField) {
        this.valueAddresses = valueAddresses;
        this.numDocsWithField = numDocsWithField;
    }

    /** Records that the document about to be written begins at {@code valueAddress}. */
    void startDocument(long valueAddress) throws IOException {
        docs++;
        if (valueAddresses != null) {
            valueAddresses.add(valueAddress);
        }
    }

    /** Closes the table into {@code data}, {@code numValues} being the address one past the column's last slot. */
    MonotonicWriter.Table finish(long numValues, IndexOutput data) throws IOException {
        assert docs == numDocsWithField : "wrote " + docs + " documents, counted " + numDocsWithField;
        if (valueAddresses == null) {
            return MonotonicWriter.Table.NONE;
        }
        valueAddresses.add(numValues);
        return valueAddresses.finish(data);
    }

    @Override
    public void close() throws IOException {
        if (valueAddresses != null) {
            valueAddresses.close();
        }
    }
}
