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
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.columnar.numeric.LongBlocks;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.substrate.BlockBytesCodec;
import org.elasticsearch.columnar.substrate.ColumnOutputs;
import org.elasticsearch.columnar.substrate.MonotonicWriter;

import java.io.Closeable;
import java.io.IOException;

/**
 * Builds what says where each document's slots begin: a count a document, and the address every block of
 * counts starts at.
 *
 * <p>Every layout writes this, because finding a document's slots is the same question whichever layout
 * names the values: a dictionary column names its with ordinals, but its documents are addressed exactly as
 * they are in a column that stores its values. Which of those slots are null is <em>not</em> a shared
 * question — a dictionary has a spare ordinal to name a null with, and only {@link StringColumnLayout#PLAIN}
 * needs {@link NullSlotWriter}.
 *
 * <p>A count is known only once the next document starts, so the counts arrive one document behind and the
 * last of them is closed by the total. Both the counts and the bases build where they belong while the
 * column's values are still being written, and land in it when they are done.
 */
final class AddressingWriter implements Closeable {

    /**
     * Documents to a block of counts when a caller names none. Small, so that a read landing in a block it
     * was not already in sums few counts to reach its document.
     */
    static final int DEFAULT_COUNTS_BLOCK_SIZE = 128;

    private final int numDocsWithField;
    private final long numValues;
    private final int countsBlockSize;

    /** Null when the slots are in step with the documents, in which case nothing is written. */
    private final LongBlocks.Writer counts;
    private final MonotonicWriter bases;

    /** Where the document last started, whose count is only known once the next one starts. */
    private long previousAddress = -1;
    private long written;

    /**
     * @param numDocsWithField documents that have at least one slot
     * @param numValues        slots across all of them, null slots included
     * @param countsBlockSize  documents a block of counts holds, and so the granularity of the bases
     */
    static AddressingWriter open(
        int numDocsWithField,
        long numValues,
        int countsBlockSize,
        Directory directory,
        IOContext context,
        String name
    ) throws IOException {
        // A document holding several slots and one holding none both put the slots out of step with the
        // documents, and either way a rank stops being its own value address.
        if (numValues == numDocsWithField) {
            return new AddressingWriter(null, null, numDocsWithField, numValues, countsBlockSize);
        }
        LongBlocks.Writer counts = null;
        try {
            // One count a document, through the chain that takes out the runs a column of like documents
            // makes and the occasional document holding far more than the rest.
            counts = LongBlocks.Writer.staged(
                NumericPipeline.runsAndOutliersPipeline(countsBlockSize),
                BlockBytesCodec.forId(BlockBytesCodec.IDENTITY_ID),
                numDocsWithField,
                directory,
                context,
                name,
                "columnar-counts"
            );
            final MonotonicWriter bases = new MonotonicWriter(
                directory,
                context,
                name,
                SlotAddressing.numBlocks(numDocsWithField, countsBlockSize)
            );
            return new AddressingWriter(counts, bases, numDocsWithField, numValues, countsBlockSize);
        } catch (Throwable t) {
            IOUtils.closeWhileHandlingException(counts);
            throw t;
        }
    }

    private AddressingWriter(LongBlocks.Writer counts, MonotonicWriter bases, int numDocsWithField, long numValues, int countsBlockSize) {
        this.counts = counts;
        this.bases = bases;
        this.numDocsWithField = numDocsWithField;
        this.numValues = numValues;
        this.countsBlockSize = countsBlockSize;
    }

    /** Records that the document about to be written begins at {@code valueAddress}. */
    void startDocument(long valueAddress) throws IOException {
        if (counts != null) {
            if (previousAddress >= 0) {
                counts.add(valueAddress - previousAddress);
            }
            if (written % countsBlockSize == 0) {
                bases.add(valueAddress);
            }
            previousAddress = valueAddress;
        }
        written++;
    }

    /**
     * Writes the counts into the addressing and the bases, one a block of counts, into the navigation, {@code writtenSlots} being the number of slots the
     * caller actually wrote — the address one past the column's last slot, which closes the last document's
     * count.
     *
     * <p>The totals are checked rather than asserted, because nothing else would catch them. A caller that
     * reported a document count it then contradicted would leave the counts declaring a length they never
     * fill; one that reported the wrong slot total would close the last document on the wrong count, and
     * every read of that document would answer wrongly in a release build with nothing to say so.
     */
    SlotAddressing finish(long writtenSlots, ColumnOutputs outputs) throws IOException {
        if (written != numDocsWithField) {
            throw new IllegalStateException("wrote " + written + " documents, counted " + numDocsWithField);
        }
        if (writtenSlots != numValues) {
            throw new IllegalStateException("wrote " + writtenSlots + " slots, counted " + numValues);
        }
        if (counts == null) {
            return SlotAddressing.NONE;
        }
        counts.add(writtenSlots - previousAddress);
        final MonotonicWriter.Table basesTable = bases.finish(outputs.navigation());
        return new SlotAddressing(counts.finish(outputs.addressing(), outputs.navigation()), basesTable);
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(counts, bases);
    }
}
