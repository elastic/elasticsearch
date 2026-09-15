/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.columnar.numeric.NumericColumnMetadata;
import org.elasticsearch.columnar.numeric.NumericColumnValues;
import org.elasticsearch.columnar.numeric.NumericColumnWriter;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.substrate.BlockBytesCodec;
import org.elasticsearch.columnar.substrate.MonotonicWriter;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
 * <p>The counts are staged to a temporary file as they arrive and written as a numeric column once the last
 * one is in, so they go through the same adaptive per-block encoding a field's values do. The bases are
 * built as the documents arrive, since a block's first address is known when its first document starts.
 */
final class AddressingWriter implements Closeable {

    /**
     * Documents to a block of counts, and so to a base. Small, so that a read landing in a block it was not
     * already in sums few counts to reach its document.
     */
    static final int COUNTS_BLOCK_SIZE = 128;

    private final int numDocsWithField;
    private final long numValues;
    private final Directory directory;
    private final IOContext context;

    /** Null when the slots are in step with the documents, in which case nothing is written. */
    private final IndexOutput countsTemp;
    private final MonotonicWriter bases;
    private final List<IndexInput> replays = new ArrayList<>();

    /** Where the document last started, whose count is only known once the next one starts. */
    private long previousAddress = -1;
    private long written;
    /** Set once the staged counts are closed, so closing this writer does not close them a second time. */
    private boolean countsStaged;

    /**
     * @param numDocsWithField documents that have at least one slot
     * @param numValues        slots across all of them, null slots included
     */
    static AddressingWriter open(int numDocsWithField, long numValues, Directory directory, IOContext context, String name)
        throws IOException {
        // A document holding several slots and one holding none both put the slots out of step with the
        // documents, and either way a rank stops being its own value address.
        if (numValues == numDocsWithField) {
            return new AddressingWriter(null, null, numDocsWithField, numValues, directory, context);
        }
        IndexOutput countsTemp = null;
        try {
            countsTemp = directory.createTempOutput(name, "columnar-counts", context);
            final MonotonicWriter bases = new MonotonicWriter(
                directory,
                context,
                name,
                SlotAddressing.numBlocks(numDocsWithField, COUNTS_BLOCK_SIZE)
            );
            return new AddressingWriter(countsTemp, bases, numDocsWithField, numValues, directory, context);
        } catch (Throwable t) {
            if (countsTemp != null) {
                IOUtils.closeWhileHandlingException(countsTemp);
                IOUtils.deleteFilesIgnoringExceptions(directory, countsTemp.getName());
            }
            throw t;
        }
    }

    private AddressingWriter(
        IndexOutput countsTemp,
        MonotonicWriter bases,
        int numDocsWithField,
        long numValues,
        Directory directory,
        IOContext context
    ) {
        this.countsTemp = countsTemp;
        this.bases = bases;
        this.numDocsWithField = numDocsWithField;
        this.numValues = numValues;
        this.directory = directory;
        this.context = context;
    }

    /** Records that the document about to be written begins at {@code valueAddress}. */
    void startDocument(long valueAddress) throws IOException {
        if (countsTemp != null) {
            if (previousAddress >= 0) {
                countsTemp.writeVLong(valueAddress - previousAddress);
            }
            if (written % COUNTS_BLOCK_SIZE == 0) {
                bases.add(valueAddress);
            }
            previousAddress = valueAddress;
        }
        written++;
    }

    /**
     * Writes the counts and the bases into {@code data}, {@code writtenSlots} being the number of slots the
     * caller actually wrote — the address one past the column's last slot, which closes the last document's
     * count.
     *
     * <p>The totals are checked rather than asserted, because nothing else would catch them. A caller that
     * reported a document count it then contradicted would leave the counts column declaring a length it
     * never fills; one that reported the wrong slot total would close the last document on the wrong count,
     * and every read of that document would answer wrongly in a release build with nothing to say so.
     */
    SlotAddressing finish(long writtenSlots, IndexOutput data) throws IOException {
        if (written != numDocsWithField) {
            throw new IllegalStateException("wrote " + written + " documents, counted " + numDocsWithField);
        }
        if (writtenSlots != numValues) {
            throw new IllegalStateException("wrote " + writtenSlots + " slots, counted " + numValues);
        }
        if (countsTemp == null) {
            return SlotAddressing.NONE;
        }
        countsTemp.writeVLong(writtenSlots - previousAddress);
        final String staged = countsTemp.getName();
        countsTemp.close();
        countsStaged = true;

        final MonotonicWriter.Table basesTable = bases.finish(data);
        // One count a document, reached by the document's own rank, so the counts table no addressing.
        final NumericColumnMetadata counts = NumericColumnWriter.write(numDocsWithField, numDocsWithField, numDocsWithField, false, () -> {
            final IndexInput in = directory.openInput(staged, context);
            replays.add(in);
            return stagedCounts(in, numDocsWithField);
        },
            NumericPipeline.runsAndOutliersPipeline(COUNTS_BLOCK_SIZE),
            BlockBytesCodec.forId(BlockBytesCodec.IDENTITY_ID),
            // The counts build no skip index, so nothing is ever written to one.
            null,
            directory,
            context,
            data,
            null
        );
        return new SlotAddressing(counts, basesTable);
    }

    /** The staged counts, one a document over a dense run of ranks, so they can be written as a numeric column. */
    private static NumericColumnValues stagedCounts(IndexInput staged, int numDocs) {
        return new NumericColumnValues() {
            private int rank = -1;

            @Override
            public int valueCount() {
                return 1;
            }

            @Override
            public long nextValue() throws IOException {
                return staged.readVLong();
            }

            @Override
            public int docID() {
                return rank;
            }

            @Override
            public int nextDoc() {
                if (rank == DocIdSetIterator.NO_MORE_DOCS || rank + 1 >= numDocs) {
                    return rank = DocIdSetIterator.NO_MORE_DOCS;
                }
                return ++rank;
            }

            @Override
            public int advance(int target) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long cost() {
                return numDocs;
            }
        };
    }

    @Override
    public void close() throws IOException {
        final String staged = countsTemp == null ? null : countsTemp.getName();
        try {
            IOUtils.close(replays);
            IOUtils.close(countsStaged ? null : countsTemp, bases);
        } finally {
            if (staged != null) {
                IOUtils.deleteFilesIgnoringExceptions(directory, staged);
            }
        }
    }
}
