/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.translog;

import org.elasticsearch.common.io.Channels;
import org.elasticsearch.index.engine.IndexOperationBatch;
import org.elasticsearch.index.seqno.SequenceNumbers;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;

final class TranslogSnapshot extends BaseTranslogReader {

    private final int totalOperations;
    private final Checkpoint checkpoint;
    protected final long length;

    private final ByteBuffer reusableBuffer;
    private long position;
    private int skippedOperations;
    private int readOperations;
    private BufferedChecksumStreamInput reuse;
    // Only used by next(): when the most recently read record was a batch, its exploded ops are
    // buffered here and emitted one-by-one before reading the next on-disk record.
    private final Deque<Translog.Operation> pendingExploded;

    /**
     * Create a snapshot of translog file channel.
     */
    TranslogSnapshot(final BaseTranslogReader reader, final long length) {
        super(reader.generation, reader.channel, reader.path, reader.header);
        this.length = length;
        this.totalOperations = reader.totalOperations();
        this.checkpoint = reader.getCheckpoint();
        this.reusableBuffer = ByteBuffer.allocate(1024);
        this.readOperations = 0;
        this.position = reader.getFirstOperationOffset();
        this.reuse = null;
        this.pendingExploded = new ArrayDeque<>();
    }

    @Override
    public int totalOperations() {
        return totalOperations;
    }

    int skippedOperations() {
        return skippedOperations;
    }

    @Override
    Checkpoint getCheckpoint() {
        return checkpoint;
    }

    /**
     * Reads the next on-disk record, dropping operations trimmed above the checkpoint. A batch record is
     * returned whole; its trimmed rows are marked skipped via
     * {@link IndexOperationBatch.TranslogRecord#filterRows} and a record whose rows are all trimmed is
     * dropped like a trimmed operation.
     */
    public Translog.Record nextRecord() throws IOException {
        while (readOperations < totalOperations) {
            final int opSize = readSize(reusableBuffer, position);
            reuse = checksummedStream(reusableBuffer, position, opSize, reuse);
            final Translog.Record record = readRecord(reuse);
            position += opSize;
            if (record instanceof Translog.Operation op) {
                readOperations++;
                if (isTrimmed(op.seqNo())) {
                    skippedOperations++;
                    continue;
                }
                return op;
            }
            // A batch record contributed its seqNo-consuming row count to operationCounter (and hence to totalOperations).
            final IndexOperationBatch.TranslogRecord batch = (IndexOperationBatch.TranslogRecord) record;
            readOperations += batch.operationCount();
            final IndexOperationBatch.TranslogRecord kept = batch.filterRows(seqNo -> isTrimmed(seqNo) == false);
            skippedOperations += batch.operationCount() - (kept == null ? 0 : kept.replayCount());
            if (kept != null) {
                return kept;
            }
        }
        reuse = null; // release buffer, it may be large and is no longer needed
        return null;
    }

    public Translog.Operation next() throws IOException {
        // First drain any pending exploded ops from a previously-read batch record.
        final Translog.Operation pending = pendingExploded.pollFirst();
        if (pending != null) {
            return pending;
        }
        Translog.Record record;
        while ((record = nextRecord()) != null) {
            if (record instanceof Translog.Operation op) {
                return op;
            }
            // skipped rows are already excluded from the exploded operations
            pendingExploded.addAll(((IndexOperationBatch.TranslogRecord) record).explode());
            final Translog.Operation first = pendingExploded.pollFirst();
            if (first != null) {
                return first;
            }
        }
        return null;
    }

    private boolean isTrimmed(long seqNo) {
        return checkpoint.trimmedAboveSeqNo != SequenceNumbers.UNASSIGNED_SEQ_NO && seqNo > checkpoint.trimmedAboveSeqNo;
    }

    public long sizeInBytes() {
        return length;
    }

    /**
     * reads an operation at the given position into the given buffer.
     */
    protected void readBytes(ByteBuffer buffer, long position) throws IOException {
        try {
            if (position >= length) {
                throw new EOFException(
                    "read requested past EOF. pos ["
                        + position
                        + "] end: ["
                        + length
                        + "], generation: ["
                        + getGeneration()
                        + "], path: ["
                        + path
                        + "]"
                );
            }
            if (position < getFirstOperationOffset()) {
                throw new IOException(
                    "read requested before position of first ops. pos ["
                        + position
                        + "] first op on: ["
                        + getFirstOperationOffset()
                        + "], generation: ["
                        + getGeneration()
                        + "], path: ["
                        + path
                        + "]"
                );
            }
            Channels.readFromFileChannelWithEofException(channel, position, buffer);
        } catch (EOFException e) {
            throw new TranslogCorruptedException(path.toString(), "translog truncated", e);
        }
    }

    @Override
    public String toString() {
        return "TranslogSnapshot{"
            + "readOperations="
            + readOperations
            + ", position="
            + position
            + ", estimateTotalOperations="
            + totalOperations
            + ", length="
            + length
            + ", generation="
            + generation
            + ", reusableBuffer="
            + reusableBuffer
            + '}';
    }
}
