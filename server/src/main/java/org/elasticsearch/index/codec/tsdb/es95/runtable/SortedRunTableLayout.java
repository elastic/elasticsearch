/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95.runtable;

import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.packed.DirectMonotonicWriter;
import org.apache.lucene.util.packed.DirectReader;
import org.apache.lucene.util.packed.DirectWriter;

import java.io.IOException;

/**
 * Reads and writes the run-table layout of a single-valued ordinal stream.
 *
 * <p>Wire format:
 * <ul>
 *   <li>meta: {@code numDocsWithField}, {@code numRuns}, {@code valueCount} and {@code maxOrd} (the
 *       largest ordinal written, the sentinel included) as vints, the {@link DirectMonotonicReader.Meta}
 *       of {@code startDoc[]}, then the absolute data offset and the byte length of {@code startDoc[]}
 *       as vlongs;</li>
 *   <li>data: {@code startDoc[]}, the first doc of every run, via {@link DirectMonotonicWriter}, then
 *       {@code ordPerRun[]} via {@link DirectWriter} at {@code bitsRequired(maxOrd)} bits per run. Both
 *       are read off-heap from random-access slices.</li>
 * </ul>
 */
public final class SortedRunTableLayout {

    /** Block shift of the {@code startDoc[]} column, shared with {@link SortedSetRunTableLayout}. */
    static final int START_DOCS_BLOCK_SHIFT = 16;

    private SortedRunTableLayout() {}

    /**
     * The parsed metadata of one field's run table. {@link #readMeta} builds it when the segment opens,
     * because the {@code startDoc[]} metadata can only be read from the meta stream then, and
     * {@link #open} uses it every time the field's doc values are opened.
     *
     * @param numDocsWithField number of docs with a value
     * @param numRuns          number of runs, sentinel runs included
     * @param valueCount       the field's value count {@code K}, which is also the sentinel ordinal
     * @param maxOrd           largest ordinal written, the sentinel included; sizes {@code ordPerRun[]}
     * @param startDocsMeta    metadata of the {@code startDoc[]} monotonic column
     * @param dataStart        absolute data offset of {@code startDoc[]}; {@code ordPerRun[]} follows it
     * @param startDocsLength  byte length of {@code startDoc[]}
     */
    public record Meta(
        int numDocsWithField,
        int numRuns,
        int valueCount,
        int maxOrd,
        DirectMonotonicReader.Meta startDocsMeta,
        long dataStart,
        long startDocsLength
    ) {}

    /** Writes the runs collected by {@code runTable}. */
    public static void encode(
        final RunTableSortedOrdinalWriter runTable,
        int numDocsWithField,
        final IndexOutput data,
        final IndexOutput meta
    ) throws IOException {
        final int numRuns = runTable.numRuns();
        final int[] startDocs = runTable.runStartDocs();
        final int[] runOrds = runTable.runOrds();
        final int maxOrd = runTable.maxOrdWritten();

        meta.writeVInt(numDocsWithField);
        meta.writeVInt(numRuns);
        meta.writeVInt(runTable.valueCount());
        meta.writeVInt(maxOrd);

        final long dataStart = data.getFilePointer();
        final DirectMonotonicWriter startDocsWriter = DirectMonotonicWriter.getInstance(meta, data, numRuns, START_DOCS_BLOCK_SHIFT);
        for (int run = 0; run < numRuns; run++) {
            startDocsWriter.add(startDocs[run]);
        }
        startDocsWriter.finish();
        meta.writeVLong(dataStart);
        meta.writeVLong(data.getFilePointer() - dataStart);

        final DirectWriter ordsWriter = DirectWriter.getInstance(data, numRuns, DirectWriter.bitsRequired(maxOrd));
        for (int run = 0; run < numRuns; run++) {
            ordsWriter.add(runOrds[run]);
        }
        ordsWriter.finish();
    }

    /** Reads the metadata {@link #encode} wrote, leaving {@code meta} right after it. */
    public static Meta readMeta(final IndexInput meta) throws IOException {
        final int numDocsWithField = meta.readVInt();
        final int numRuns = meta.readVInt();
        final int valueCount = meta.readVInt();
        final int maxOrd = meta.readVInt();
        final DirectMonotonicReader.Meta startDocsMeta = DirectMonotonicReader.loadMeta(meta, numRuns, START_DOCS_BLOCK_SHIFT);
        final long dataStart = meta.readVLong();
        final long startDocsLength = meta.readVLong();
        return new Meta(numDocsWithField, numRuns, valueCount, maxOrd, startDocsMeta, dataStart, startDocsLength);
    }

    /**
     * Opens the per-doc ordinals of the field described by {@code meta}. Each call returns an independent
     * iterator; {@code data} is only sliced, never positioned.
     */
    public static NumericDocValues open(final Meta meta, final IndexInput data, int maxDoc) throws IOException {
        final int numRuns = meta.numRuns();
        final LongValues startDocs = DirectMonotonicReader.getInstance(
            meta.startDocsMeta(),
            data.randomAccessSlice(meta.dataStart(), meta.startDocsLength())
        );
        final int bitsPerOrd = DirectWriter.bitsRequired(meta.maxOrd());
        final long ordsStart = meta.dataStart() + meta.startDocsLength();
        final LongValues ordsPerRun = DirectReader.getInstance(
            data.randomAccessSlice(ordsStart, DirectWriter.bytesRequired(numRuns, bitsPerOrd)),
            bitsPerOrd
        );
        return new RunTableSortedDocValues(
            new RunTableCursor(startDocs, numRuns, maxDoc),
            ordsPerRun,
            maxDoc,
            meta.valueCount(),
            meta.numDocsWithField()
        );
    }
}
