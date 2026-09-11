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
 * Builds the table that says where each document's slots begin.
 *
 * <p>Every layout writes this one, because finding a document's slots is the same question whichever layout
 * names the values: a dictionary column names its with ordinals, but its documents are addressed exactly as
 * they are in a column that stores its values. Which of those slots are null is <em>not</em> a shared
 * question — a dictionary has a spare ordinal to name a null with, and only {@link StringColumnLayout#PLAIN}
 * needs {@link NullSlotWriter}.
 */
final class AddressingWriter extends SlotTableWriter {

    private final int numDocsWithField;
    private final long numValues;

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
        return new AddressingWriter(valueAddresses, numDocsWithField, numValues);
    }

    private AddressingWriter(MonotonicWriter valueAddresses, int numDocsWithField, long numValues) {
        super(valueAddresses);
        this.numDocsWithField = numDocsWithField;
        this.numValues = numValues;
    }

    /** Records that the document about to be written begins at {@code valueAddress}. */
    void startDocument(long valueAddress) throws IOException {
        add(valueAddress);
    }

    /**
     * Closes the table into {@code data}, {@code writtenSlots} being the number of slots the caller actually
     * wrote — the address one past the column's last slot, and the sentinel this table ends with.
     *
     * <p>Checked rather than asserted, because nothing else would catch it. The table holds one entry a
     * document, so a wrong document count makes {@link MonotonicWriter} fail on its own declared length; the
     * slot count sizes nothing and is only ever written here, as that sentinel. A cursor that reported a
     * total it then contradicted would otherwise leave the last document reading back the wrong
     * {@code valueCount} in a release build, with nothing to say so.
     */
    MonotonicWriter.Table finish(long writtenSlots, IndexOutput data) throws IOException {
        if (written() != numDocsWithField) {
            throw new IllegalStateException("wrote " + written() + " documents, counted " + numDocsWithField);
        }
        if (writtenSlots != numValues) {
            throw new IllegalStateException("wrote " + writtenSlots + " slots, counted " + numValues);
        }
        addSentinel(writtenSlots);
        return finishTable(data);
    }
}
