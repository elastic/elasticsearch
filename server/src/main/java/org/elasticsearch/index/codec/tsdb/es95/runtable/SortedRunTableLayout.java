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
 * Owns the binary encoding and decoding of the single-valued run-table layout written by
 * {@link RunTableSortedOrdinalWriter}. All I/O lives here; the writer and reader contain no
 * stream access of their own.
 *
 * <p>Wire format:
 * <ul>
 *   <li>meta: {@code numRuns} (vint), {@code valueCount} (vint), {@code maxOrd} (vint — the largest
 *       ordinal actually written, including the sentinel that equals {@code valueCount}),
 *       {@code blockShift} (vint, always {@value AbstractRunTableLayout#BLOCK_SHIFT}), the
 *       {@link DirectMonotonicReader.Meta} for {@code startDoc[]} ({@code numRuns} entries), the
 *       absolute data start (vlong), then the byte length of the {@code startDoc[]} data section
 *       (vlong)</li>
 *   <li>data: {@code startDoc[]} via {@link DirectMonotonicWriter} (ascending start doc per run,
 *       {@code numRuns} entries), then {@code ordPerRun[]} via {@link DirectWriter} at
 *       {@code bitsRequired(maxOrd)} bits per entry ({@code numRuns} entries), allowing the reader to
 *       decode each run's ordinal off-heap on demand from the mapped input without loading the full
 *       column into heap</li>
 * </ul>
 */
public final class SortedRunTableLayout extends AbstractRunTableLayout {

    private SortedRunTableLayout() {}

    /**
     * Encodes the accumulated run table from {@code accumulator} to {@code data} and {@code meta}
     * and returns the run count together with the bytes written across both streams.
     */
    public static RunTableSortedOrdinalWriter.Stats encode(
        final RunTableSortedOrdinalWriter accumulator,
        final IndexOutput data,
        final IndexOutput meta
    ) throws IOException {
        final int numRuns = accumulator.numRuns();
        final int[] startDocs = accumulator.startDocs();
        final int[] runOrds = accumulator.runOrds();
        final int maxOrdWritten = accumulator.maxOrdWritten();
        final int valueCount = accumulator.valueCount();

        final long dataStart = data.getFilePointer();
        final long metaStart = meta.getFilePointer();

        meta.writeVInt(numRuns);
        meta.writeVInt(valueCount);
        meta.writeVInt(maxOrdWritten);
        meta.writeVInt(BLOCK_SHIFT);

        final long startDocsLength = writeStartDocs(startDocs, numRuns, data, meta);

        meta.writeVLong(dataStart);
        meta.writeVLong(startDocsLength);

        final int bitsPerOrd = DirectWriter.bitsRequired(Math.max(maxOrdWritten, 0));
        final DirectWriter ordsWriter = DirectWriter.getInstance(data, numRuns, bitsPerOrd);
        for (int run = 0; run < numRuns; run++) {
            ordsWriter.add(runOrds[run]);
        }
        ordsWriter.finish();

        final long totalBytes = (data.getFilePointer() - dataStart) + (meta.getFilePointer() - metaStart);
        return new RunTableSortedOrdinalWriter.Stats(numRuns, totalBytes);
    }

    /**
     * Reads the run-table header from {@code meta} at segment-open time. Must run at segment-open time
     * because it loads the {@link DirectMonotonicReader.Meta} for {@code startDoc[]} out of the meta stream.
     */
    public static RunTableSortedOrdinalReader.Meta readMeta(final IndexInput meta) throws IOException {
        final int numRuns = meta.readVInt();
        final int valueCount = meta.readVInt();
        final int maxOrd = meta.readVInt();
        final int blockShift = meta.readVInt();
        final DirectMonotonicReader.Meta startDocsMeta = readStartDocsMeta(meta, numRuns, blockShift);
        final long dataStart = meta.readVLong();
        final long startDocsLength = meta.readVLong();
        return new RunTableSortedOrdinalReader.Meta(numRuns, valueCount, maxOrd, blockShift, startDocsMeta, dataStart, startDocsLength);
    }

    /**
     * Builds the {@link NumericDocValues} view from an already-parsed {@link RunTableSortedOrdinalReader.Meta}
     * and a data input. {@code ordPerRun[]} is served off-heap on demand through a {@link DirectReader} over
     * a random-access slice.
     */
    public static NumericDocValues open(final RunTableSortedOrdinalReader.Meta meta, final IndexInput data, int maxDoc) throws IOException {
        final int numRuns = meta.numRuns();
        final DirectMonotonicReader startDocs = openStartDocs(meta.startDocsMeta(), data, meta.dataStart(), meta.startDocsLength());
        final LongValues ordsPerRun = openOrdsPerRun(meta, data, numRuns);
        return RunTableSortedOrdinalReader.open(new RunTableCursor(startDocs, numRuns, maxDoc), ordsPerRun, maxDoc, meta.valueCount());
    }

    /**
     * Builds a run-granularity view over the same columns as {@link #open}, exposing each run's start doc,
     * length, and ordinal for segment merge, which reads a source field once per run rather than once per doc.
     */
    public static RunTableSortedOrdinalReader.Runs openRuns(final RunTableSortedOrdinalReader.Meta meta, final IndexInput data, int maxDoc)
        throws IOException {
        final int numRuns = meta.numRuns();
        final DirectMonotonicReader startDocs = openStartDocs(meta.startDocsMeta(), data, meta.dataStart(), meta.startDocsLength());
        final LongValues ordsPerRun = openOrdsPerRun(meta, data, numRuns);
        return RunTableSortedOrdinalReader.openRuns(startDocs, ordsPerRun, numRuns, maxDoc, meta.valueCount());
    }

    private static LongValues openOrdsPerRun(final RunTableSortedOrdinalReader.Meta meta, final IndexInput data, int numRuns)
        throws IOException {
        final int bitsPerOrd = DirectWriter.bitsRequired(Math.max(meta.maxOrd(), 0));
        final long ordsStart = meta.dataStart() + meta.startDocsLength();
        final long ordsLength = DirectWriter.bytesRequired(numRuns, bitsPerOrd);
        return DirectReader.getInstance(data.randomAccessSlice(ordsStart, ordsLength), bitsPerOrd);
    }
}
