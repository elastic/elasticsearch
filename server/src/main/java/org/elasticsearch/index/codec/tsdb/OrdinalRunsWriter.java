/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.packed.DirectMonotonicWriter;
import org.apache.lucene.util.packed.DirectWriter;

import java.io.IOException;
import java.util.Arrays;

/**
 * Records the first run of documents of each ordinal of a secondary index sort field while the field is written: a mapping from
 * ordinal to run, and the start and end document of each run in document order. See {@link OrdinalRunsDocValues} for the layout
 * this describes, who reads it, and why documents of an ordinal outside its first run are tolerated but not covered.
 * <p>
 * {@link #onDoc} is guaranteed to be called at the first document of every run, in document order, but not necessarily for every
 * document. The observer writes a flag byte to the metadata, so that readers know whether to expect the runs.
 */
public final class OrdinalRunsWriter implements SortedFieldObserver {

    private final int maxDoc;
    private final int blockShift;

    private int numOrds;
    private int[] ordToRun;
    private int[] runStarts;
    private int[] runEnds;
    private int numRuns;
    private int currentRun = -1;
    private long lastOrd = -1;

    /**
     * @param maxDoc     the number of documents of the segment, the end of a run that reaches the end of the segment
     * @param blockShift the block shift of the {@link DirectMonotonicWriter} encoding the run bounds
     */
    public OrdinalRunsWriter(int maxDoc, int blockShift) {
        this.maxDoc = maxDoc;
        this.blockShift = blockShift;
    }

    @Override
    public void onTerm(BytesRef term, long ord) {
        numOrds = Math.toIntExact(ord + 1);
    }

    @Override
    public void prepareForDocs() {
        ordToRun = new int[numOrds];
        Arrays.fill(ordToRun, -1);
        runStarts = new int[numOrds];
        runEnds = new int[numOrds];
    }

    @Override
    public void onDoc(int docId, long ord) {
        if (ord == lastOrd) {
            return;
        }
        lastOrd = ord;
        if (currentRun != -1) {
            runEnds[currentRun] = docId;
        }
        final int o = Math.toIntExact(ord);
        if (ordToRun[o] == -1) {
            ordToRun[o] = numRuns;
            runStarts[numRuns] = docId;
            currentRun = numRuns++;
        } else {
            // The ordinal appeared before: its documents here are strays, see OrdinalRunsDocValues
            currentRun = -1;
        }
    }

    @Override
    public void flush(IndexOutput data, IndexOutput meta) throws IOException {
        if (numOrds == 0) {
            meta.writeByte((byte) 0);
            return;
        }
        assert numRuns == numOrds : numRuns + " != " + numOrds;
        if (currentRun != -1) {
            runEnds[currentRun] = maxDoc;
        }
        meta.writeByte((byte) 1);
        meta.writeVInt(numRuns);
        writeMonotonic(data, meta, runStarts);
        writeMonotonic(data, meta, runEnds);

        // Ordinal to run.
        final int bitsPerValue = DirectWriter.unsignedBitsRequired(numRuns - 1L);
        final long ordToRunOffset = data.getFilePointer();
        meta.writeLong(ordToRunOffset);
        meta.writeByte((byte) bitsPerValue);
        final DirectWriter ordToRunWriter = DirectWriter.getInstance(data, numOrds, bitsPerValue);
        for (int ord = 0; ord < numOrds; ord++) {
            ordToRunWriter.add(ordToRun[ord]);
        }
        ordToRunWriter.finish();
        meta.writeLong(data.getFilePointer() - ordToRunOffset);
    }

    /** Writes the run bounds, which ascend with the run, as read by {@code AbstractTSDBDocValuesProducer#readSorted}. */
    private void writeMonotonic(IndexOutput data, IndexOutput meta, int[] values) throws IOException {
        final long offset = data.getFilePointer();
        meta.writeLong(offset);
        meta.writeVInt(blockShift);
        final DirectMonotonicWriter writer = DirectMonotonicWriter.getInstance(meta, data, numRuns, blockShift);
        for (int run = 0; run < numRuns; run++) {
            writer.add(values[run]);
        }
        writer.finish();
        meta.writeLong(data.getFilePointer() - offset);
    }
}
