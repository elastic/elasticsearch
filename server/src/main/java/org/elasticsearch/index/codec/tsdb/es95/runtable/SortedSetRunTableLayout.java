/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95.runtable;

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.packed.DirectMonotonicWriter;
import org.apache.lucene.util.packed.DirectReader;
import org.apache.lucene.util.packed.DirectWriter;

import java.io.IOException;

import static org.elasticsearch.index.codec.tsdb.es95.runtable.SortedRunTableLayout.START_DOCS_BLOCK_SHIFT;

/**
 * Reads and writes the run-table layout of a multi-valued ordinal stream. Unlike the blocked layout it needs
 * no per-doc addresses table: every doc of a run shares the run's set, so a per-run offset delimits it.
 *
 * <p>Wire format:
 * <ul>
 *   <li>meta: {@code numDocsWithField} (vint), {@code numValues} (vlong), {@code numRuns}, {@code valueCount} and
 *       {@code numOrds} (the length of {@code ordStream[]}) as vints, the {@link DirectMonotonicReader.Meta} of
 *       {@code startDoc[]} and of {@code setOffset[]}, then the absolute data offset and the byte lengths of
 *       {@code startDoc[]} and {@code setOffset[]} as vlongs;</li>
 *   <li>data: {@code startDoc[]}, the first doc of every run, and {@code setOffset[]}, the start of every run's
 *       set in {@code ordStream[]} plus a final fencepost, both via {@link DirectMonotonicWriter}; then
 *       {@code ordStream[]}, the ascending sets of all runs back to back, via {@link DirectWriter} at
 *       {@code bitsRequired(valueCount - 1)} bits per ordinal. All three are read off-heap from random-access
 *       slices.</li>
 * </ul>
 */
public final class SortedSetRunTableLayout {

    private SortedSetRunTableLayout() {}

    /**
     * The parsed metadata of one field's run table. {@link #readMeta} builds it when the segment opens,
     * because the monotonic column metadata can only be read from the meta stream then, and {@link #open}
     * uses it every time the field's doc values are opened.
     *
     * @param numDocsWithField number of docs with at least one value
     * @param numValues        total number of values over all docs
     * @param numRuns          number of runs, runs of docs without a value included
     * @param valueCount       the field's value count {@code K}; every ordinal is in {@code [0, K)}
     * @param numOrds          length of {@code ordStream[]}
     * @param startDocsMeta    metadata of the {@code startDoc[]} monotonic column
     * @param setOffsetsMeta   metadata of the {@code setOffset[]} monotonic column, {@code numRuns + 1} entries
     * @param dataStart        absolute data offset of {@code startDoc[]}; the other columns follow it
     * @param startDocsLength  byte length of {@code startDoc[]}
     * @param setOffsetsLength byte length of {@code setOffset[]}
     */
    public record Meta(
        int numDocsWithField,
        long numValues,
        int numRuns,
        int valueCount,
        int numOrds,
        DirectMonotonicReader.Meta startDocsMeta,
        DirectMonotonicReader.Meta setOffsetsMeta,
        long dataStart,
        long startDocsLength,
        long setOffsetsLength
    ) {}

    /** Writes the runs collected by {@code runTable}. */
    public static void encode(
        final RunTableSortedSetOrdinalWriter runTable,
        int numDocsWithField,
        long numValues,
        final IndexOutput data,
        final IndexOutput meta
    ) throws IOException {
        final int numRuns = runTable.numRuns();
        final int numOrds = runTable.numOrds();
        final int[] startDocs = runTable.runStartDocs();
        final int[] setStarts = runTable.runSetStarts();
        final int[] ords = runTable.ords();

        meta.writeVInt(numDocsWithField);
        meta.writeVLong(numValues);
        meta.writeVInt(numRuns);
        meta.writeVInt(runTable.valueCount());
        meta.writeVInt(numOrds);

        final long dataStart = data.getFilePointer();
        final DirectMonotonicWriter startDocsWriter = DirectMonotonicWriter.getInstance(meta, data, numRuns, START_DOCS_BLOCK_SHIFT);
        for (int run = 0; run < numRuns; run++) {
            startDocsWriter.add(startDocs[run]);
        }
        startDocsWriter.finish();
        final long setOffsetsStart = data.getFilePointer();

        final DirectMonotonicWriter setOffsetsWriter = DirectMonotonicWriter.getInstance(meta, data, numRuns + 1L, START_DOCS_BLOCK_SHIFT);
        for (int run = 0; run < numRuns; run++) {
            setOffsetsWriter.add(setStarts[run]);
        }
        setOffsetsWriter.add(numOrds);
        setOffsetsWriter.finish();

        meta.writeVLong(dataStart);
        meta.writeVLong(setOffsetsStart - dataStart);
        meta.writeVLong(data.getFilePointer() - setOffsetsStart);

        final DirectWriter ordsWriter = DirectWriter.getInstance(data, numOrds, bitsPerOrd(runTable.valueCount()));
        for (int i = 0; i < numOrds; i++) {
            ordsWriter.add(ords[i]);
        }
        ordsWriter.finish();
    }

    /** Reads the metadata {@link #encode} wrote, leaving {@code meta} right after it. */
    public static Meta readMeta(final IndexInput meta) throws IOException {
        final int numDocsWithField = meta.readVInt();
        final long numValues = meta.readVLong();
        final int numRuns = meta.readVInt();
        final int valueCount = meta.readVInt();
        final int numOrds = meta.readVInt();
        final DirectMonotonicReader.Meta startDocsMeta = DirectMonotonicReader.loadMeta(meta, numRuns, START_DOCS_BLOCK_SHIFT);
        final DirectMonotonicReader.Meta setOffsetsMeta = DirectMonotonicReader.loadMeta(meta, numRuns + 1L, START_DOCS_BLOCK_SHIFT);
        final long dataStart = meta.readVLong();
        final long startDocsLength = meta.readVLong();
        final long setOffsetsLength = meta.readVLong();
        return new Meta(
            numDocsWithField,
            numValues,
            numRuns,
            valueCount,
            numOrds,
            startDocsMeta,
            setOffsetsMeta,
            dataStart,
            startDocsLength,
            setOffsetsLength
        );
    }

    /**
     * Opens the per-doc ordinal sets of the field described by {@code meta}. Each call returns an independent
     * iterator; {@code data} is only sliced, never positioned.
     */
    public static SortedNumericDocValues open(final Meta meta, final IndexInput data, int maxDoc) throws IOException {
        final LongValues startDocs = DirectMonotonicReader.getInstance(
            meta.startDocsMeta(),
            data.randomAccessSlice(meta.dataStart(), meta.startDocsLength())
        );
        final long setOffsetsStart = meta.dataStart() + meta.startDocsLength();
        final LongValues setOffsets = DirectMonotonicReader.getInstance(
            meta.setOffsetsMeta(),
            data.randomAccessSlice(setOffsetsStart, meta.setOffsetsLength())
        );
        final int bitsPerOrd = bitsPerOrd(meta.valueCount());
        final LongValues ordStream = DirectReader.getInstance(
            data.randomAccessSlice(setOffsetsStart + meta.setOffsetsLength(), DirectWriter.bytesRequired(meta.numOrds(), bitsPerOrd)),
            bitsPerOrd
        );
        return new RunTableSortedSetDocValues(
            new RunTableCursor(startDocs, meta.numRuns(), maxDoc),
            setOffsets,
            ordStream,
            maxDoc,
            meta.numDocsWithField()
        );
    }

    private static int bitsPerOrd(int valueCount) {
        return DirectWriter.bitsRequired(valueCount - 1);
    }
}
