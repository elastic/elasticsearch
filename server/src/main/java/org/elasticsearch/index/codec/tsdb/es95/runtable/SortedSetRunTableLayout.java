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

/**
 * Owns the binary encoding and decoding of the multi-valued run-table layout written by
 * {@link RunTableSortedSetOrdinalWriter}. All I/O lives here; the writer and reader contain no
 * stream access of their own.
 *
 * <p>Wire format:
 * <ul>
 *   <li>meta: {@code numRuns} (vint), {@code valueCount} (vint), {@code totalOrds} (vint),
 *       {@code blockShift} (vint), the {@link DirectMonotonicReader.Meta} for {@code startDoc[]}
 *       ({@code numRuns} entries, blockShift {@value AbstractRunTableLayout#BLOCK_SHIFT}), the
 *       {@link DirectMonotonicReader.Meta} for {@code setOffset[]} ({@code numRuns + 1} entries,
 *       same block shift), the absolute data start (vlong), then the byte lengths of the
 *       {@code startDoc[]} section and the {@code setOffset[]} section (two vlongs)</li>
 *   <li>data: {@code startDoc[]} via {@link DirectMonotonicWriter} (ascending start doc per run),
 *       then {@code setOffset[]} via {@link DirectMonotonicWriter} ({@code numRuns + 1} fencepost
 *       entries delimiting each run's slice into {@code ordStream[]}), then {@code ordStream[]}
 *       via {@link DirectWriter} at {@code bitsRequired(valueCount - 1)} bits per ord; each run's
 *       slice is ascending, preserving the {@link SortedNumericDocValues} contract for free</li>
 * </ul>
 */
public final class SortedSetRunTableLayout extends AbstractRunTableLayout {

    private SortedSetRunTableLayout() {}

    /**
     * Encodes the accumulated run table from {@code accumulator} to {@code data} and {@code meta}
     * and returns the run count together with the bytes written across both streams.
     *
     * @throws IllegalStateException if no ordinal sets have been added to the accumulator
     */
    public static RunTableSortedSetOrdinalWriter.Stats encode(
        final RunTableSortedSetOrdinalWriter accumulator,
        final IndexOutput data,
        final IndexOutput meta
    ) throws IOException {
        final int numRuns = accumulator.numRuns();
        final int[] runStartDocs = accumulator.runStartDocs();
        final int[] runOrdCounts = accumulator.runOrdCounts();
        final int[] ordStreamBuffer = accumulator.ordStreamBuffer();
        final int ordStreamSize = accumulator.ordStreamSize();
        final int totalOrds = accumulator.totalOrds();
        final int valueCount = accumulator.valueCount();

        if (accumulator.docCount() == 0) {
            throw new IllegalStateException("no ordinal sets added");
        }

        final int[] setOffsets = new int[numRuns + 1];
        int offset = 0;
        for (int run = 0; run < numRuns; run++) {
            setOffsets[run] = offset;
            offset += runOrdCounts[run];
        }
        setOffsets[numRuns] = offset;

        final long dataStart = data.getFilePointer();
        final long metaStart = meta.getFilePointer();

        meta.writeVInt(numRuns);
        meta.writeVInt(valueCount);
        meta.writeVInt(totalOrds);
        meta.writeVInt(BLOCK_SHIFT);

        final long startDocsLength = writeStartDocs(runStartDocs, numRuns, data, meta);

        final DirectMonotonicWriter setOffsetsWriter = DirectMonotonicWriter.getInstance(meta, data, numRuns + 1, BLOCK_SHIFT);
        for (int i = 0; i <= numRuns; i++) {
            setOffsetsWriter.add(setOffsets[i]);
        }
        setOffsetsWriter.finish();
        final long setOffsetsLength = data.getFilePointer() - dataStart - startDocsLength;

        meta.writeVLong(dataStart);
        meta.writeVLong(startDocsLength);
        meta.writeVLong(setOffsetsLength);

        final int bitsPerOrd = DirectWriter.bitsRequired(Math.max(valueCount - 1, 0));
        final DirectWriter ordsWriter = DirectWriter.getInstance(data, totalOrds, bitsPerOrd);
        for (int i = 0; i < ordStreamSize; i++) {
            ordsWriter.add(ordStreamBuffer[i]);
        }
        ordsWriter.finish();

        final long totalBytes = (data.getFilePointer() - dataStart) + (meta.getFilePointer() - metaStart);
        return new RunTableSortedSetOrdinalWriter.Stats(numRuns, totalBytes);
    }

    /**
     * Reads the run-table header from {@code meta} at segment-open time. Must run at segment-open time
     * because it loads the {@link DirectMonotonicReader.Meta} for {@code startDoc[]} and {@code setOffset[]}
     * out of the meta stream.
     */
    public static RunTableSortedSetOrdinalReader.Meta readMeta(final IndexInput meta) throws IOException {
        final int numRuns = meta.readVInt();
        final int valueCount = meta.readVInt();
        final int totalOrds = meta.readVInt();
        final int blockShift = meta.readVInt();
        final DirectMonotonicReader.Meta startDocsMeta = readStartDocsMeta(meta, numRuns, blockShift);
        final DirectMonotonicReader.Meta setOffsetsMeta = DirectMonotonicReader.loadMeta(meta, numRuns + 1, blockShift);
        final long dataStart = meta.readVLong();
        final long startDocsLength = meta.readVLong();
        final long setOffsetsLength = meta.readVLong();
        return new RunTableSortedSetOrdinalReader.Meta(
            numRuns,
            valueCount,
            totalOrds,
            blockShift,
            startDocsMeta,
            setOffsetsMeta,
            dataStart,
            startDocsLength,
            setOffsetsLength
        );
    }

    /**
     * Builds the {@link SortedNumericDocValues} view from an already-parsed
     * {@link RunTableSortedSetOrdinalReader.Meta} and a data input. {@code ordStream[]} is served
     * off-heap on demand through a {@link DirectReader} over a random-access slice.
     */
    public static SortedNumericDocValues open(final RunTableSortedSetOrdinalReader.Meta meta, final IndexInput data, int maxDoc)
        throws IOException {
        final DirectMonotonicReader startDocs = openStartDocs(meta.startDocsMeta(), data, meta.dataStart(), meta.startDocsLength());
        final DirectMonotonicReader setOffsets = openSetOffsets(meta, data);
        final LongValues ordStream = openOrdStream(meta, data);
        return RunTableSortedSetOrdinalReader.open(new RunTableCursor(startDocs, meta.numRuns(), maxDoc), setOffsets, ordStream, maxDoc);
    }

    /**
     * Builds a run-granularity view over the same columns as {@link #open}, exposing each run's start doc,
     * length, and ordinal set for segment merge, which reads a source field once per run rather than once per doc.
     */
    public static RunTableSortedSetOrdinalReader.Runs openRuns(
        final RunTableSortedSetOrdinalReader.Meta meta,
        final IndexInput data,
        int maxDoc
    ) throws IOException {
        final DirectMonotonicReader startDocs = openStartDocs(meta.startDocsMeta(), data, meta.dataStart(), meta.startDocsLength());
        final DirectMonotonicReader setOffsets = openSetOffsets(meta, data);
        final LongValues ordStream = openOrdStream(meta, data);
        return RunTableSortedSetOrdinalReader.openRuns(startDocs, setOffsets, ordStream, meta.numRuns(), maxDoc);
    }

    private static DirectMonotonicReader openSetOffsets(final RunTableSortedSetOrdinalReader.Meta meta, final IndexInput data)
        throws IOException {
        return DirectMonotonicReader.getInstance(
            meta.setOffsetsMeta(),
            data.randomAccessSlice(meta.dataStart() + meta.startDocsLength(), meta.setOffsetsLength())
        );
    }

    private static LongValues openOrdStream(final RunTableSortedSetOrdinalReader.Meta meta, final IndexInput data) throws IOException {
        final int bitsPerOrd = DirectWriter.bitsRequired(Math.max(meta.valueCount() - 1, 0));
        final long ordStreamStart = meta.dataStart() + meta.startDocsLength() + meta.setOffsetsLength();
        final long ordStreamLength = DirectWriter.bytesRequired(meta.totalOrds(), bitsPerOrd);
        return DirectReader.getInstance(data.randomAccessSlice(ordStreamStart, ordStreamLength), bitsPerOrd);
    }
}
