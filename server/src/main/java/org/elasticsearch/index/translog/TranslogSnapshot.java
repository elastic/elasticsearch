/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.translog;

import org.elasticsearch.common.io.Channels;
import org.elasticsearch.index.seqno.SequenceNumbers;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.LongFunction;

final class TranslogSnapshot extends BaseTranslogReader {

    private final Checkpoint checkpoint;

    private final ByteBuffer reusableBuffer;
    private long position;
    private int skippedOperations;
    private int readOperations;
    private BufferedChecksumStreamInput reuse;
    private final LongFunction<Translog.Location> seqNoToLocations;

    /**
     * Create a snapshot of translog file channel.
     */
    TranslogSnapshot(final BaseTranslogReader reader, final LongFunction<Translog.Location> seqNoToLocations) {
        super(reader.generation, reader.channel, reader.path, reader.header);
        this.checkpoint = reader.getCheckpoint();
        this.reusableBuffer = ByteBuffer.allocate(1024);
        this.readOperations = 0;
        this.position = reader.getFirstOperationOffset();
        this.reuse = null;
        this.seqNoToLocations = seqNoToLocations;
    }

    @Override
    public int totalOperations() {
        return checkpoint.numOps;
    }

    int skippedOperations(){
        return skippedOperations;
    }

    @Override
    Checkpoint getCheckpoint() {
        return checkpoint;
    }

    Translog.Location findLocation(long seqNo) {
        if (checkpoint.minSeqNo <= seqNo && seqNo <= checkpoint.maxEffectiveSeqNo()) {
            final Translog.Location location = seqNoToLocations.apply(seqNo);
            if (location != null) {
                assert location.generation == generation : location.generation + " != " + getGeneration();
                if ((location.translogLocation + location.size) <= sizeInBytes()) {
                    return location;
                }
            }
        }
        return null;
    }

    public Translog.Operation next() throws IOException {
        while (readOperations < totalOperations()) {
            final Translog.Operation operation = readOperation();
            if (operation.seqNo() <= checkpoint.trimmedAboveSeqNo || checkpoint.trimmedAboveSeqNo == SequenceNumbers.UNASSIGNED_SEQ_NO) {
                return operation;
            }
            skippedOperations++;
        }
        return null;
    }

    private Translog.Operation readOperation() throws IOException {
        final int opSize = readSize(reusableBuffer, position);
        reuse = checksummedStream(reusableBuffer, position, opSize, reuse);
        Translog.Operation op = read(reuse);
        position += opSize;
        readOperations++;
        return op;
    }

    public long sizeInBytes() {
        return checkpoint.offset;
    }

    @Override
    protected TranslogSnapshot newSnapshot() {
        throw new UnsupportedOperationException();
    }

    /**
     * reads an operation at the given position into the given buffer.
     */
    protected void readBytes(ByteBuffer buffer, long position) throws IOException {
        try {
            if (position >= sizeInBytes()) {
                throw new EOFException("read requested past EOF. pos [" + position + "] end: [" + sizeInBytes() + "], generation: [" +
                    getGeneration() + "], path: [" + path + "]");
            }
            if (position < getFirstOperationOffset()) {
                throw new IOException("read requested before position of first ops. pos [" + position + "] first op on: [" +
                    getFirstOperationOffset() + "], generation: [" + getGeneration() + "], path: [" + path + "]");
            }
            Channels.readFromFileChannelWithEofException(channel, position, buffer);
        } catch (EOFException e) {
            throw new TranslogCorruptedException(path.toString(), "translog truncated", e);
        }
    }

    @Override
    public String toString() {
        return "TranslogSnapshot{" +
                "readOperations=" + readOperations +
                ", position=" + position +
                ", estimateTotalOperations=" + totalOperations() +
                ", length=" + sizeInBytes() +
                ", generation=" + generation +
                ", reusableBuffer=" + reusableBuffer +
                '}';
    }
}
