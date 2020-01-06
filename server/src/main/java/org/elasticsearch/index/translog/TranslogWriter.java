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

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.procedures.LongProcedure;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.core.internal.io.IOUtils;
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
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

public class TranslogWriter extends BaseTranslogReader implements Closeable {
    private final ShardId shardId;
    private final ChannelFactory channelFactory;
    // the last checkpoint that was written when the translog was last synced
    private volatile Checkpoint lastSyncedCheckpoint;
    /* the number of translog operations written to this file */
    private volatile int operationCounter;
    /* if we hit an exception that we can't recover from we assign it to this var and ship it with every AlreadyClosedException we throw */
    private final TragicExceptionHolder tragedy;
    /* A buffered outputstream what writes to the writers channel */
    private final OutputStream outputStream;
    /* the total offset of this file including the bytes written to the file as well as into the buffer */
    private volatile long totalOffset;

    private volatile long minSeqNo;
    private volatile long maxSeqNo;

    private final LongSupplier globalCheckpointSupplier;
    private final LongSupplier minTranslogGenerationSupplier;

    // callback that's called whenever an operation with a given sequence number is successfully persisted.
    private final LongConsumer persistedSequenceNumberConsumer;

    protected final AtomicBoolean closed = new AtomicBoolean(false);
    // lock order synchronized(syncLock) -> synchronized(this)
    private final Object syncLock = new Object();

    private LongArrayList nonFsyncedSequenceNumbers;

    private final Map<Long, Tuple<BytesReference, Exception>> seenSequenceNumbers;

    private TranslogWriter(
        final ChannelFactory channelFactory,
        final ShardId shardId,
        final Checkpoint initialCheckpoint,
        final FileChannel channel,
        final Path path,
        final ByteSizeValue bufferSize,
        final LongSupplier globalCheckpointSupplier, LongSupplier minTranslogGenerationSupplier, TranslogHeader header,
        TragicExceptionHolder tragedy,
        final LongConsumer persistedSequenceNumberConsumer)
            throws
            IOException {
        super(initialCheckpoint.generation, channel, path, header);
        assert initialCheckpoint.offset == channel.position() :
            "initial checkpoint offset [" + initialCheckpoint.offset + "] is different than current channel position ["
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
        assert initialCheckpoint.trimmedAboveSeqNo == SequenceNumbers.UNASSIGNED_SEQ_NO : initialCheckpoint.trimmedAboveSeqNo;
        this.globalCheckpointSupplier = globalCheckpointSupplier;
        this.nonFsyncedSequenceNumbers = new LongArrayList(64);
        this.persistedSequenceNumberConsumer = persistedSequenceNumberConsumer;
        this.seenSequenceNumbers = Assertions.ENABLED ? new HashMap<>() : null;
        this.tragedy = tragedy;
    }

    public static TranslogWriter create(ShardId shardId, String translogUUID, long fileGeneration, Path file, ChannelFactory channelFactory,
                                        ByteSizeValue bufferSize, final long initialMinTranslogGen, long initialGlobalCheckpoint,
                                        final LongSupplier globalCheckpointSupplier, final LongSupplier minTranslogGenerationSupplier,
                                        final long primaryTerm, TragicExceptionHolder tragedy, LongConsumer persistedSequenceNumberConsumer)
        throws IOException {
        final FileChannel channel = channelFactory.open(file);
        try {
            final TranslogHeader header = new TranslogHeader(translogUUID, primaryTerm);
            header.write(channel);
            final Checkpoint checkpoint = Checkpoint.emptyTranslogCheckpoint(header.sizeInBytes(), fileGeneration,
                initialGlobalCheckpoint, initialMinTranslogGen);
            writeCheckpoint(channelFactory, file.getParent(), checkpoint);
            final LongSupplier writerGlobalCheckpointSupplier;
            if (Assertions.ENABLED) {
                writerGlobalCheckpointSupplier = () -> {
                    long gcp = globalCheckpointSupplier.getAsLong();
                    assert gcp >= initialGlobalCheckpoint :
                        "global checkpoint [" + gcp + "] lower than initial gcp [" + initialGlobalCheckpoint + "]";
                    return gcp;
                };
            } else {
                writerGlobalCheckpointSupplier = globalCheckpointSupplier;
            }
            return new TranslogWriter(channelFactory, shardId, checkpoint, channel, file, bufferSize,
                writerGlobalCheckpointSupplier, minTranslogGenerationSupplier, header, tragedy, persistedSequenceNumberConsumer);
        } catch (Exception exception) {
            // if we fail to bake the file-generation into the checkpoint we stick with the file and once we recover and that
            // file exists we remove it. We only apply this logic to the checkpoint.generation+1 any other file with a higher generation
            // is an error condition
            IOUtils.closeWhileHandlingException(channel);
            throw exception;
        }
    }

    private synchronized void closeWithTragicEvent(final Exception ex) {
        tragedy.setTragicException(ex);
        try {
            close();
        } catch (final IOException | RuntimeException e) {
            ex.addSuppressed(e);
        }
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
            closeWithTragicEvent(ex);
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

        nonFsyncedSequenceNumbers.add(seqNo);

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
                Translog.Operation newOp = Translog.readOperation(
                        new BufferedChecksumStreamInput(data.streamInput(), "assertion"));
                Translog.Operation prvOp = Translog.readOperation(
                        new BufferedChecksumStreamInput(previous.v1().streamInput(), "assertion"));
                // TODO: We haven't had timestamp for Index operations in Lucene yet, we need to loosen this check without timestamp.
                final boolean sameOp;
                if (newOp instanceof Translog.Index && prvOp instanceof Translog.Index) {
                    final Translog.Index o1 = (Translog.Index) prvOp;
                    final Translog.Index o2 = (Translog.Index) newOp;
                    sameOp = Objects.equals(o1.id(), o2.id())
                        && Objects.equals(o1.source(), o2.source()) && Objects.equals(o1.routing(), o2.routing())
                        && o1.primaryTerm() == o2.primaryTerm() && o1.seqNo() == o2.seqNo()
                        && o1.version() == o2.version();
                } else if (newOp instanceof Translog.Delete && prvOp instanceof Translog.Delete) {
                    final Translog.Delete o1 = (Translog.Delete) newOp;
                    final Translog.Delete o2 = (Translog.Delete) prvOp;
                    sameOp = Objects.equals(o1.id(), o2.id())
                        && o1.primaryTerm() == o2.primaryTerm() && o1.seqNo() == o2.seqNo() && o1.version() == o2.version();
                } else {
                    sameOp = false;
                }
                if (sameOp == false) {
                    throw new AssertionError(
                        "seqNo [" + seqNo + "] was processed twice in generation [" + generation + "], with different data. " +
                            "prvOp [" + prvOp + "], newOp [" + newOp + "]", previous.v2());
                }
            }
        } else {
            seenSequenceNumbers.put(seqNo,
                new Tuple<>(new BytesArray(data.toBytesRef(), true), new RuntimeException("stack capture previous op")));
        }
        return true;
    }

    synchronized boolean assertNoSeqAbove(long belowTerm, long aboveSeqNo) {
        seenSequenceNumbers.entrySet().stream().filter(e -> e.getKey().longValue() > aboveSeqNo)
            .forEach(e -> {
                final Translog.Operation op;
                try {
                    op = Translog.readOperation(
                            new BufferedChecksumStreamInput(e.getValue().v1().streamInput(), "assertion"));
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
                long seqNo = op.seqNo();
                long primaryTerm = op.primaryTerm();
                if (primaryTerm < belowTerm) {
                    throw new AssertionError("current should not have any operations with seq#:primaryTerm ["
                        + seqNo + ":" + primaryTerm + "] > " + aboveSeqNo + ":" + belowTerm);
                }
            });
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
            globalCheckpointSupplier.getAsLong(), minTranslogGenerationSupplier.getAsLong(),
            SequenceNumbers.UNASSIGNED_SEQ_NO);
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
                } catch (final Exception ex) {
                    closeWithTragicEvent(ex);
                    throw ex;
                }
                if (closed.compareAndSet(false, true)) {
                    return new TranslogReader(getLastSyncedCheckpoint(), channel, path, header);
                } else {
                    throw new AlreadyClosedException("translog [" + getGeneration() + "] is already closed (path [" + path + "]",
                            tragedy.get());
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
    final boolean syncUpTo(long offset) throws IOException {
        if (lastSyncedCheckpoint.offset < offset && syncNeeded()) {
            synchronized (syncLock) { // only one sync/checkpoint should happen concurrently but we wait
                if (lastSyncedCheckpoint.offset < offset && syncNeeded()) {
                    // double checked locking - we don't want to fsync unless we have to and now that we have
                    // the lock we should check again since if this code is busy we might have fsynced enough already
                    final Checkpoint checkpointToSync;
                    final LongArrayList flushedSequenceNumbers;
                    synchronized (this) {
                        ensureOpen();
                        try {
                            outputStream.flush();
                            checkpointToSync = getCheckpoint();
                            flushedSequenceNumbers = nonFsyncedSequenceNumbers;
                            nonFsyncedSequenceNumbers = new LongArrayList(64);
                        } catch (final Exception ex) {
                            closeWithTragicEvent(ex);
                            throw ex;
                        }
                    }
                    // now do the actual fsync outside of the synchronized block such that
                    // we can continue writing to the buffer etc.
                    try {
                        channel.force(false);
                        writeCheckpoint(channelFactory, path.getParent(), checkpointToSync);
                    } catch (final Exception ex) {
                        closeWithTragicEvent(ex);
                        throw ex;
                    }
                    flushedSequenceNumbers.forEach((LongProcedure) persistedSequenceNumberConsumer::accept);
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
        try {
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
        } catch (final Exception ex) {
            closeWithTragicEvent(ex);
            throw ex;
        }
        // we don't have to have a lock here because we only write ahead to the file, so all writes has been complete
        // for the requested location.
        Channels.readFromFileChannelWithEofException(channel, position, targetBuffer);
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
            throw new AlreadyClosedException("translog [" + getGeneration() + "] is already closed", tragedy.get());
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
                } catch (final Exception ex) {
                    closeWithTragicEvent(ex);
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
