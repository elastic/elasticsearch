/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.translog;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.Assertions;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

public class TranslogWriter extends BaseTranslogReader implements Closeable {

    public static final String TRANSLOG_CODEC = "translog";
    public static final int VERSION_CHECKSUMS = 1;
    public static final int VERSION_CHECKPOINTS = 2; // since 2.0 we have checkpoints?
    public static final int VERSION = VERSION_CHECKPOINTS;

    private final ShardId shardId;
    private final ChannelFactory channelFactory;
    // the last checkpoint that was written when the translog was last synced
    private volatile Checkpoint lastSyncedCheckpoint;
    /* the number of translog operations written to this file */
    private volatile int operationCounter;
    /* if we hit an exception that we can't recover from we assign it to this var and ship it with every AlreadyClosedException we throw */
    private volatile Exception tragedy;
    /* A buffered outputstream what writes to the writers channel */
    private final OutputStream outputStream;
    /* the total offset of this file including the bytes written to the file as well as into the buffer */
    private volatile long totalOffset;

    private volatile long minSeqNo;
    private volatile long maxSeqNo;

    private final LongSupplier globalCheckpointSupplier;
    private final LongSupplier minTranslogGenerationSupplier;

    protected final AtomicBoolean closed = new AtomicBoolean(false);
    // lock order synchronized(syncLock) -> synchronized(this)
    private final Object syncLock = new Object();

    private final Map<Long, Tuple<BytesReference, Exception>> seenSequenceNumbers;

    private TranslogWriter(
        final ChannelFactory channelFactory,
        final ShardId shardId,
        final Checkpoint initialCheckpoint,
        final FileChannel channel,
        final Path path,
        final ByteSizeValue bufferSize,
        final LongSupplier globalCheckpointSupplier, LongSupplier minTranslogGenerationSupplier) throws IOException {
        super(initialCheckpoint.generation, channel, path, channel.position());
        assert initialCheckpoint.offset == channel.position() :
            "initial checkpoint offset [" + initialCheckpoint.offset + "] is different than current channel poistion ["
                + channel.position() + "]";
        this.shardId = shardId;
        this.channelFactory = channelFactory;
        this.minTranslogGenerationSupplier = minTranslogGenerationSupplier;
        this.outputStream = new BufferedChannelOutputStream(java.nio.channels.Channels.newOutputStream(channel), bufferSize.bytesAsInt());
        this.lastSyncedCheckpoint = initialCheckpoint;
        this.totalOffset = initialCheckpoint.offset;
        assert initialCheckpoint.minSeqNo == SequenceNumbers.NO_OPS_PERFORMED : initialCheckpoint.minSeqNo;
        this.minSeqNo = initialCheckpoint.minSeqNo;
        assert initialCheckpoint.maxSeqNo == SequenceNumbers.NO_OPS_PERFORMED : initialCheckpoint.maxSeqNo;
        this.maxSeqNo = initialCheckpoint.maxSeqNo;
        this.globalCheckpointSupplier = globalCheckpointSupplier;
        this.seenSequenceNumbers = Assertions.ENABLED ? new HashMap<>() : null;
    }

    static int getHeaderLength(String translogUUID) {
        return getHeaderLength(new BytesRef(translogUUID).length);
    }

    static int getHeaderLength(int uuidLength) {
        return CodecUtil.headerLength(TRANSLOG_CODEC) + uuidLength + Integer.BYTES;
    }

    static void writeHeader(OutputStreamDataOutput out, BytesRef ref) throws IOException {
        CodecUtil.writeHeader(out, TRANSLOG_CODEC, VERSION);
        out.writeInt(ref.length);
        out.writeBytes(ref.bytes, ref.offset, ref.length);
    }

    public static TranslogWriter create(ShardId shardId, String translogUUID, long fileGeneration, Path file, ChannelFactory channelFactory,
                                        ByteSizeValue bufferSize, final LongSupplier globalCheckpointSupplier,
                                        final long initialMinTranslogGen, final LongSupplier minTranslogGenerationSupplier)
        throws IOException {
        final BytesRef ref = new BytesRef(translogUUID);
        final int firstOperationOffset = getHeaderLength(ref.length);
        final FileChannel channel = channelFactory.open(file);
        try {
            // This OutputStreamDataOutput is intentionally not closed because
            // closing it will close the FileChannel
            final OutputStreamDataOutput out = new OutputStreamDataOutput(java.nio.channels.Channels.newOutputStream(channel));
            writeHeader(out, ref);
            channel.force(true);
            final Checkpoint checkpoint = Checkpoint.emptyTranslogCheckpoint(firstOperationOffset, fileGeneration,
                globalCheckpointSupplier.getAsLong(), initialMinTranslogGen);
            writeCheckpoint(channelFactory, file.getParent(), checkpoint);
            return new TranslogWriter(channelFactory, shardId, checkpoint, channel, file, bufferSize,
                globalCheckpointSupplier, minTranslogGenerationSupplier);
        } catch (Exception exception) {
            // if we fail to bake the file-generation into the checkpoint we stick with the file and once we recover and that
            // file exists we remove it. We only apply this logic to the checkpoint.generation+1 any other file with a higher generation is an error condition
            IOUtils.closeWhileHandlingException(channel);
            throw exception;
        }
    }

    /**
     * If this {@code TranslogWriter} was closed as a side-effect of a tragic exception,
     * e.g. disk full while flushing a new segment, this returns the root cause exception.
     * Otherwise (no tragic exception has occurred) it returns null.
     */
    public Exception getTragicException() {
        return tragedy;
    }

    private synchronized void closeWithTragicEvent(Exception exception) throws IOException {
        assert exception != null;
        if (tragedy == null) {
            tragedy = exception;
        } else if (tragedy != exception) {
            // it should be safe to call closeWithTragicEvents on multiple layers without
            // worrying about self suppression.
            tragedy.addSuppressed(exception);
        }
        close();
    }

    /**
     * add the given bytes to the translog and return the location they were written at
     */

    /**
     * Add the given bytes to the translog with the specified sequence number; returns the location the bytes were written to.
     *
     * @param data  the bytes to write
     * @param seqNo the sequence number associated with the operation
     * @return the location the bytes were written to
     * @throws IOException if writing to the translog resulted in an I/O exception
     */
    public synchronized Translog.Location add(final BytesReference data, final long seqNo) throws IOException {
        ensureOpen();
        final long offset = totalOffset;
        try {
            data.writeTo(outputStream);
        } catch (final Exception ex) {
            try {
                closeWithTragicEvent(ex);
            } catch (final Exception inner) {
                ex.addSuppressed(inner);
            }
            throw ex;
        }
        totalOffset += data.length();

        if (minSeqNo == SequenceNumbers.NO_OPS_PERFORMED) {
            assert operationCounter == 0;
        }
        if (maxSeqNo == SequenceNumbers.NO_OPS_PERFORMED) {
            assert operationCounter == 0;
        }

        minSeqNo = SequenceNumbers.min(minSeqNo, seqNo);
        maxSeqNo = SequenceNumbers.max(maxSeqNo, seqNo);

        operationCounter++;

        assert assertNoSeqNumberConflict(seqNo, data);

        return new Translog.Location(generation, offset, data.length());
    }

    private synchronized boolean assertNoSeqNumberConflict(long seqNo, BytesReference data) throws IOException {
        if (seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO) {
            // nothing to do
        } else if (seenSequenceNumbers.containsKey(seqNo)) {
            final Tuple<BytesReference, Exception> previous = seenSequenceNumbers.get(seqNo);
            if (previous.v1().equals(data) == false) {
                Translog.Operation newOp = Translog.readOperation(new BufferedChecksumStreamInput(data.streamInput()));
                Translog.Operation prvOp = Translog.readOperation(new BufferedChecksumStreamInput(previous.v1().streamInput()));
                throw new AssertionError(
                    "seqNo [" + seqNo + "] was processed twice in generation [" + generation + "], with different data. " +
                        "prvOp [" + prvOp + "], newOp [" + newOp + "]", previous.v2());
            }
        } else {
            seenSequenceNumbers.put(seqNo,
                new Tuple<>(new BytesArray(data.toBytesRef(), true), new RuntimeException("stack capture previous op")));
        }
        return true;
    }

    /**
     * write all buffered ops to disk and fsync file.
     *
     * Note: any exception during the sync process will be interpreted as a tragic exception and the writer will be closed before
     * raising the exception.
     */
    public void sync() throws IOException {
        syncUpTo(Long.MAX_VALUE);
    }

    /**
     * Returns <code>true</code> if there are buffered operations that have not been flushed and fsynced to disk or if the latest global
     * checkpoint has not yet been fsynced
     */
    public boolean syncNeeded() {
        return totalOffset != lastSyncedCheckpoint.offset ||
            globalCheckpointSupplier.getAsLong() != lastSyncedCheckpoint.globalCheckpoint ||
            minTranslogGenerationSupplier.getAsLong() != lastSyncedCheckpoint.minTranslogGeneration;
    }

    @Override
    public int totalOperations() {
        return operationCounter;
    }

    @Override
    synchronized Checkpoint getCheckpoint() {
        return new Checkpoint(totalOffset, operationCounter, generation, minSeqNo, maxSeqNo,
            globalCheckpointSupplier.getAsLong(), minTranslogGenerationSupplier.getAsLong());
    }

    @Override
    public long sizeInBytes() {
        return totalOffset;
    }

    /**
     * Closes this writer and transfers its underlying file channel to a new immutable {@link TranslogReader}
     * @return a new {@link TranslogReader}
     * @throws IOException if any of the file operations resulted in an I/O exception
     */
    public TranslogReader closeIntoReader() throws IOException {
        // make sure to acquire the sync lock first, to prevent dead locks with threads calling
        // syncUpTo() , where the sync lock is acquired first, following by the synchronize(this)
        //
        // Note: While this is not strictly needed as this method is called while blocking all ops on the translog,
        //       we do this to for correctness and preventing future issues.
        synchronized (syncLock) {
            synchronized (this) {
                try {
                    sync(); // sync before we close..
                } catch (IOException e) {
                    try {
                        closeWithTragicEvent(e);
                    } catch (Exception inner) {
                        e.addSuppressed(inner);
                    }
                    throw e;
                }
                if (closed.compareAndSet(false, true)) {
                    return new TranslogReader(getLastSyncedCheckpoint(), channel, path, getFirstOperationOffset());
                } else {
                    throw new AlreadyClosedException("translog [" + getGeneration() + "] is already closed (path [" + path + "]", tragedy);
                }
            }
        }
    }


    @Override
    public TranslogSnapshot newSnapshot() {
        // make sure to acquire the sync lock first, to prevent dead locks with threads calling
        // syncUpTo() , where the sync lock is acquired first, following by the synchronize(this)
        synchronized (syncLock) {
            synchronized (this) {
                ensureOpen();
                try {
                    sync();
                } catch (IOException e) {
                    throw new TranslogException(shardId, "exception while syncing before creating a snapshot", e);
                }
                return super.newSnapshot();
            }
        }
    }

    private long getWrittenOffset() throws IOException {
        return channel.position();
    }

    /**
     * Syncs the translog up to at least the given offset unless already synced
     *
     * @return <code>true</code> if this call caused an actual sync operation
     */
    public boolean syncUpTo(long offset) throws IOException {
        if (lastSyncedCheckpoint.offset < offset && syncNeeded()) {
            synchronized (syncLock) { // only one sync/checkpoint should happen concurrently but we wait
                if (lastSyncedCheckpoint.offset < offset && syncNeeded()) {
                    // double checked locking - we don't want to fsync unless we have to and now that we have
                    // the lock we should check again since if this code is busy we might have fsynced enough already
                    final Checkpoint checkpointToSync;
                    synchronized (this) {
                        ensureOpen();
                        try {
                            outputStream.flush();
                            checkpointToSync = getCheckpoint();
                        } catch (Exception ex) {
                            try {
                                closeWithTragicEvent(ex);
                            } catch (Exception inner) {
                                ex.addSuppressed(inner);
                            }
                            throw ex;
                        }
                    }
                    // now do the actual fsync outside of the synchronized block such that
                    // we can continue writing to the buffer etc.
                    try {
                        channel.force(false);
                        writeCheckpoint(channelFactory, path.getParent(), checkpointToSync);
                    } catch (Exception ex) {
                        try {
                            closeWithTragicEvent(ex);
                        } catch (Exception inner) {
                            ex.addSuppressed(inner);
                        }
                        throw ex;
                    }
                    assert lastSyncedCheckpoint.offset <= checkpointToSync.offset :
                        "illegal state: " + lastSyncedCheckpoint.offset + " <= " + checkpointToSync.offset;
                    lastSyncedCheckpoint = checkpointToSync; // write protected by syncLock
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    protected void readBytes(ByteBuffer targetBuffer, long position) throws IOException {
        if (position + targetBuffer.remaining() > getWrittenOffset()) {
            synchronized (this) {
                // we only flush here if it's really really needed - try to minimize the impact of the read operation
                // in some cases ie. a tragic event we might still be able to read the relevant value
                // which is not really important in production but some test can make most strict assumptions
                // if we don't fail in this call unless absolutely necessary.
                if (position + targetBuffer.remaining() > getWrittenOffset()) {
                    outputStream.flush();
                }
            }
        }
        // we don't have to have a lock here because we only write ahead to the file, so all writes has been complete
        // for the requested location.
        Channels.readFromFileChannelWithEofException(channel, position, targetBuffer);
    }

    private static Checkpoint writeCheckpoint(
            ChannelFactory channelFactory,
            long syncPosition,
            int numOperations,
            long minSeqNo,
            long maxSeqNo,
            long globalCheckpoint,
            long minTranslogGeneration,
            Path translogFile,
            long generation) throws IOException {
        final Checkpoint checkpoint =
            new Checkpoint(syncPosition, numOperations, generation, minSeqNo, maxSeqNo, globalCheckpoint, minTranslogGeneration);
        writeCheckpoint(channelFactory, translogFile, checkpoint);
        return checkpoint;
    }

    private static void writeCheckpoint(
            final ChannelFactory channelFactory,
            final Path translogFile,
            final Checkpoint checkpoint) throws IOException {
        Checkpoint.write(channelFactory, translogFile.resolve(Translog.CHECKPOINT_FILE_NAME), checkpoint, StandardOpenOption.WRITE);
    }

    /**
     * The last synced checkpoint for this translog.
     *
     * @return the last synced checkpoint
     */
    Checkpoint getLastSyncedCheckpoint() {
        return lastSyncedCheckpoint;
    }

    protected final void ensureOpen() {
        if (isClosed()) {
            throw new AlreadyClosedException("translog [" + getGeneration() + "] is already closed", tragedy);
        }
    }

    @Override
    public final void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            channel.close();
        }
    }

    protected final boolean isClosed() {
        return closed.get();
    }


    private final class BufferedChannelOutputStream extends BufferedOutputStream {

        BufferedChannelOutputStream(OutputStream out, int size) throws IOException {
            super(out, size);
        }

        @Override
        public synchronized void flush() throws IOException {
            if (count > 0) {
                try {
                    ensureOpen();
                    super.flush();
                } catch (Exception ex) {
                    try {
                        closeWithTragicEvent(ex);
                    } catch (Exception inner) {
                        ex.addSuppressed(inner);
                    }
                    throw ex;
                }
            }
        }

        @Override
        public void close() throws IOException {
            // the stream is intentionally not closed because
            // closing it will close the FileChannel
            throw new IllegalStateException("never close this stream");
        }
    }

}
