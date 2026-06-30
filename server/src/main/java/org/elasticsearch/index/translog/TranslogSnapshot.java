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
import org.elasticsearch.index.seqno.SequenceNumbers;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

final class TranslogSnapshot extends BaseTranslogReader {

    private final int totalOperations;
    private final Checkpoint checkpoint;
    protected final long length;

    private final ByteBuffer reusableBuffer;
    private long position;
    private int skippedOperations;
    private int readOperations;
    private BufferedChecksumStreamInput reuse;
    // When the most recently read record was an IndexBatch, its exploded ops are buffered here
    // and emitted one-by-one by subsequent next() calls before reading the next on-disk record.
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
     * Used for translog recovery. Returns Operation for a single op and a BatchIndex for a batch read
     * @throws IOException
     */
    public Translog.Record nextRecord() throws IOException {

        while (readOperations < totalOperations) {
            final int opSize = readSize(reusableBuffer, position);
            reuse = checksummedStream(reusableBuffer, position, opSize, reuse);
            final Translog.Record record = readRecord(reuse);
            position += opSize;

            if (record instanceof Translog.Operation op) {
                readOperations++;
                if (op.seqNo() <= checkpoint.trimmedAboveSeqNo || checkpoint.trimmedAboveSeqNo == SequenceNumbers.UNASSIGNED_SEQ_NO) {
                    return op;
                }
                skippedOperations++;
                continue;
            }

            final Translog.IndexBatch batch = (Translog.IndexBatch) record;
            readOperations += batch.docCount();

            final List<Translog.IndexBatch.Op> batchOps = new ArrayList<>(batch.ops().size());
            for (Translog.IndexBatch.Op batchOp : batch.ops()) {
                if (batchOp.seqNo() <= checkpoint.trimmedAboveSeqNo || checkpoint.trimmedAboveSeqNo == SequenceNumbers.UNASSIGNED_SEQ_NO) {
                    batchOps.add(batchOp);
                } else {
                    skippedOperations++;
                }
            }

            // If some records were trimmed, send a trimmed batch, else send the whole batch
            return batch.ops().size() == batchOps.size()  ? batch : new Translog.IndexBatch(batch.batchData(), batch.primaryTerm(), batchOps);

        }

        reuse = null; // release buffer, it may be large and is no longer needed
        return null;
    }

    public Translog.Operation next() throws IOException {
        while (readOperations < totalOperations) {
            final Translog.Operation operation = nextOperation();
            if (operation == null) {
                continue;
            }
            if (operation.seqNo() <= checkpoint.trimmedAboveSeqNo || checkpoint.trimmedAboveSeqNo == SequenceNumbers.UNASSIGNED_SEQ_NO) {
                return operation;
            }
            skippedOperations++;
        }
        reuse = null; // release buffer, it may be large and is no longer needed
        return null;
    }

    private Translog.Operation nextOperation() throws IOException {
        // First drain any pending exploded ops from a previously-read batch record.
        Translog.Operation pending = pendingExploded.pollFirst();
        if (pending != null) {
            readOperations++;
            return pending;
        }
        final int opSize = readSize(reusableBuffer, position);
        reuse = checksummedStream(reusableBuffer, position, opSize, reuse);
        final Translog.Record record = readRecord(reuse);
        position += opSize;
        if (record instanceof Translog.Operation op) {
            readOperations++;
            return op;
        }
        // A batch record contributed docCount to operationCounter (and hence to totalOperations).
        // Explode and queue them; the next loop iteration will emit one and bump readOperations.
        final Translog.IndexBatch batch = (Translog.IndexBatch) record;
        final List<Translog.Operation> exploded = batch.explode();
        pendingExploded.addAll(exploded);
        return null;
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
