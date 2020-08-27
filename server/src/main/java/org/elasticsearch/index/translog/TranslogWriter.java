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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.Assertions;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.io.DirectPool;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

@SuppressForbidden(reason = "Channel#write")
public class TranslogWriter extends BaseTranslogReader implements Closeable {

    private static final long FORCE_WRITE_THRESHOLD = 1024 * 1024 * 4;

    private final ShardId shardId;
    private final ChannelFactory channelFactory;
    private final FileChannel channel;
    private final FileChannel checkPointChannel;
    // the last checkpoint that was written when the translog was last synced
    private volatile Checkpoint lastSyncedCheckpoint;
    // the checkpoint of the operations that have been written
    private volatile Checkpoint lastWrittenCheckpoint;
    /* the number of translog operations written to this file */
    private final AtomicInteger operationCounter = new AtomicInteger(0);
    /* if we hit an exception that we can't recover from we assign it to this var and ship it with every AlreadyClosedException we throw */
    private final TragicExceptionHolder tragedy;

    private final ConcurrentLinkedDeque<Operation> buffer = new ConcurrentLinkedDeque<>();
    /* the total offset of this file including the bytes written to the file as well as into the buffer */
    private final AtomicLong totalOffset;
    private final AtomicLong writtenOffset;

    private final LongSupplier globalCheckpointSupplier;
    private final LongSupplier minTranslogGenerationSupplier;

    // callback that's called whenever an operation with a given sequence number is successfully persisted.
    private final LongConsumer persistedSequenceNumberConsumer;

    protected final AtomicBoolean closed = new AtomicBoolean(false);
    // lock order synchronized(syncLock) -> writeLock.acquire()
    private final Object syncLock = new Object();
    private final ReleasableLock writeLock = new ReleasableLock(new ReentrantLock());

    private LongArrayList nonFsyncedSequenceNumbers;
    private final PriorityQueue<Operation> operationSorter = new PriorityQueue<>(Comparator.comparingLong(o -> o.translogLocation));

    private final Map<Long, Tuple<BytesReference, Exception>> seenSequenceNumbers;

    private TranslogWriter(
        final ChannelFactory channelFactory,
        final ShardId shardId,
        final Checkpoint initialCheckpoint,
        final FileChannel channel,
        final FileChannel checkPointChannel,
        final Path path,
        final LongSupplier globalCheckpointSupplier, LongSupplier minTranslogGenerationSupplier, TranslogHeader header,
        TragicExceptionHolder tragedy,
        final LongConsumer persistedSequenceNumberConsumer)
            throws
            IOException {
        super(initialCheckpoint.generation, channel, path, header);
        this.channel = channel;
        this.checkPointChannel = checkPointChannel;
        assert initialCheckpoint.offset == channel.position() :
            "initial checkpoint offset [" + initialCheckpoint.offset + "] is different than current channel position ["
                + channel.position() + "]";
        this.shardId = shardId;
        this.channelFactory = channelFactory;
        this.minTranslogGenerationSupplier = minTranslogGenerationSupplier;
        this.lastWrittenCheckpoint = initialCheckpoint;
        this.lastSyncedCheckpoint = initialCheckpoint;
        this.totalOffset = new AtomicLong(initialCheckpoint.offset);
        this.writtenOffset = new AtomicLong(initialCheckpoint.offset);
        assert initialCheckpoint.minSeqNo == SequenceNumbers.NO_OPS_PERFORMED : initialCheckpoint.minSeqNo;
        assert initialCheckpoint.maxSeqNo == SequenceNumbers.NO_OPS_PERFORMED : initialCheckpoint.maxSeqNo;
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
        boolean checkpointChannelOpened = false;
        try {
            Path parent = file.getParent();
            Path checkpointFile = parent.resolve(Translog.CHECKPOINT_FILE_NAME);
            final FileChannel checkpointChannel = channelFactory.open(checkpointFile, StandardOpenOption.WRITE);
            checkpointChannelOpened = true;

            try {
                final TranslogHeader header = new TranslogHeader(translogUUID, primaryTerm);
                header.write(channel);
                final Checkpoint checkpoint = Checkpoint.emptyTranslogCheckpoint(header.sizeInBytes(), fileGeneration,
                    initialGlobalCheckpoint, initialMinTranslogGen);
                writeCheckpoint(checkpointChannel, parent, checkpoint);
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
                return new TranslogWriter(channelFactory, shardId, checkpoint, channel, checkpointChannel, file,
                    writerGlobalCheckpointSupplier, minTranslogGenerationSupplier, header, tragedy, persistedSequenceNumberConsumer);
            } catch (Exception exception) {
                // if we fail to bake the file-generation into the checkpoint we stick with the file and once we recover and that
                // file exists we remove it. We only apply this logic to the checkpoint.generation+1 any other file with a higher generation
                // is an error condition
                IOUtils.closeWhileHandlingException(channel, checkpointChannel);
                throw exception;
            }
        } finally {
            if (checkpointChannelOpened == false) {
                IOUtils.closeWhileHandlingException(channel);
            }
        }
    }

    private void closeWithTragicEvent(final Exception ex) {
        try (Releasable lock = writeLock.acquire()) {
            tragedy.setTragicException(ex);
            try {
                close();
            } catch (final IOException | RuntimeException e) {
                ex.addSuppressed(e);
            }
        }
    }

    /**
     * Add the given bytes to the translog with the specified sequence number; returns the location the bytes were written to.
     *
     * @param data  the bytes to write
     * @param seqNo the sequence number associated with the operation
     * @return the location the bytes were written to
     * @throws IOException if writing to the translog resulted in an I/O exception
     */
    public Translog.Location add(final ReleasableBytesReference data, final long seqNo) throws IOException {
        int dataLength = data.length();
        long translogLocation = totalOffset.getAndAdd(dataLength);
        long newTotalOffset = translogLocation + dataLength;

        Operation operation = new Operation(seqNo, translogLocation, data);
        buffer.addLast(operation);

        final boolean isClosed = isClosed();
        if (isClosed) {
            if (buffer.removeLastOccurrence(operation)) {
                operation.close();
                throwAlreadyClosedException();
            }
        }

        operationCounter.getAndIncrement();

        assert assertNoSeqNumberConflict(seqNo, data);

        final long bufferSize = newTotalOffset - writtenOffset.get();
        // Do not try to write if this writer is closed. Someone else will handle it.
        if (bufferSize > FORCE_WRITE_THRESHOLD && isClosed == false) {
            // Block if the buffer is twice the size of the force write threshold
            writeUpTo(newTotalOffset, bufferSize > FORCE_WRITE_THRESHOLD * 2);
        }

        return new Translog.Location(generation, translogLocation, dataLength);
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
        seenSequenceNumbers.entrySet().stream().filter(e -> e.getKey() > aboveSeqNo)
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
        return totalOffset.get() != lastSyncedCheckpoint.offset ||
            globalCheckpointSupplier.getAsLong() != lastSyncedCheckpoint.globalCheckpoint ||
            minTranslogGenerationSupplier.getAsLong() != lastSyncedCheckpoint.minTranslogGeneration;
    }

    @Override
    public int totalOperations() {
        return operationCounter.get();
    }

    @Override
    Checkpoint getCheckpoint() {
        // This can be called concurrently with `put` because we will just read all available ops from the concurrent buffer
        try (ReleasableLock lock = writeLock.acquire()) {
            final Checkpoint lastWrittenCheckpoint = this.lastWrittenCheckpoint;
            long minSeqNo = lastWrittenCheckpoint.minSeqNo;
            long maxSeqNo = lastWrittenCheckpoint.maxSeqNo;
            long offset = lastWrittenCheckpoint.offset;
            int numOps = lastWrittenCheckpoint.numOps;
            for (Operation operation : buffer) {
                long seqNo = operation.seqNo;
                minSeqNo = SequenceNumbers.min(minSeqNo, seqNo);
                maxSeqNo = SequenceNumbers.max(maxSeqNo, seqNo);
                offset = Math.max(offset, operation.translogLocation + operation.data.length());
                ++numOps;
            }
            return new Checkpoint(offset, numOps, generation, minSeqNo, maxSeqNo, globalCheckpointSupplier.getAsLong(),
                minTranslogGenerationSupplier.getAsLong(), SequenceNumbers.UNASSIGNED_SEQ_NO);
        }
    }

    @Override
    public long sizeInBytes() {
        return totalOffset.get();
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
            try (ReleasableLock lock = writeLock.acquire()) {
                try {
                    sync(); // sync before we close..
                } catch (final Exception ex) {
                    closeWithTragicEvent(ex);
                    throw ex;
                }
                if (closed.compareAndSet(false, true)) {
                    // TODO: Exception handling
                    checkPointChannel.close();
                    releaseQueuedOperations();
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
            try (ReleasableLock lock = writeLock.acquire()) {
                ensureOpen();
                try {
                    sync();
                } catch (IOException e) {
                    throw new TranslogException(shardId, "exception while syncing before creating a snapshot", e);
                }
                return new TranslogSnapshot(generation, channel, path, header, lastSyncedCheckpoint, getFirstOperationOffset());
            }
        }
    }

    /**
     * Syncs the translog up to at least the given offset unless already synced
     *
     * @return <code>true</code> if this call caused an actual sync operation
     */
    final boolean syncUpTo(long offset) throws IOException {
        if (lastSyncedCheckpoint.offset < offset && syncNeeded()) {
            synchronized (syncLock) { // only one sync/checkpoint should happen concurrently but we wait
                ensureOpen();
                if (lastSyncedCheckpoint.offset < offset && syncNeeded()) {
                    final LongArrayList flushedSequenceNumbers = nonFsyncedSequenceNumbers;
                    try (ReleasableLock toRelease = writeLock.acquire()) {
                        writeUpTo(Long.MAX_VALUE, true);
                        nonFsyncedSequenceNumbers = new LongArrayList(64);
                    }

                    final Checkpoint checkpointToSync = lastWrittenCheckpoint;
                    nonFsyncedSequenceNumbers = new LongArrayList(64);
                    // now do the actual fsync outside of the synchronized block such that
                    // we can continue writing to the buffer etc.
                    try {
                        channel.force(false);
                        writeCheckpoint(checkPointChannel, path.getParent(), checkpointToSync);
                    } catch (final Exception ex) {
                        closeWithTragicEvent(ex);
                        throw ex;
                    }
                    flushedSequenceNumbers.forEach((LongProcedure) persistedSequenceNumberConsumer::accept);
                    assert lastSyncedCheckpoint.offset <= checkpointToSync.offset :
                        "illegal state: " + lastSyncedCheckpoint.offset + " <= " + checkpointToSync.offset;
                    lastSyncedCheckpoint = checkpointToSync;
                    return true;
                }
            }
        }
        return false;
    }

    private void writeUpTo(long offset, boolean block) throws IOException {
        if (writtenOffset.get() < offset) {
            ReleasableLock lock;
            if (block) {
                lock = writeLock.acquire();
            } else {
                lock = writeLock.tryAcquire();
                if (lock == null) {
                    return;
                }
            }

            try (ReleasableLock toRelease = lock) {
                if (writtenOffset.get() < offset) {
                    ensureOpen();

                    long minSeqNo = lastWrittenCheckpoint.minSeqNo;
                    long maxSeqNo = lastWrittenCheckpoint.maxSeqNo;
                    int numOps = lastWrittenCheckpoint.numOps;

                    try {
                        ByteBuffer ioBuffer = DirectPool.getIoBuffer();
                        Operation operation;
                        long expectedLocation = lastWrittenCheckpoint.offset;
                        while ((operation = nextOperation(expectedLocation)) != null) {
                            long seqNo = operation.seqNo;
                            minSeqNo = SequenceNumbers.min(minSeqNo, seqNo);
                            maxSeqNo = SequenceNumbers.max(maxSeqNo, seqNo);
                            ++numOps;

                            try (Releasable toClose = operation) {
                                nonFsyncedSequenceNumbers.add(operation.seqNo);
                                ReleasableBytesReference data = operation.data;
                                BytesRefIterator iterator = data.iterator();
                                BytesRef current;
                                while ((current = iterator.next()) != null) {
                                    int currentBytesConsumed = 0;
                                    while (currentBytesConsumed != current.length) {
                                        int nBytesToWrite = Math.min(current.length - currentBytesConsumed, ioBuffer.remaining());
                                        ioBuffer.put(current.bytes, current.offset + currentBytesConsumed, nBytesToWrite);
                                        currentBytesConsumed += nBytesToWrite;
                                        if (ioBuffer.hasRemaining() == false) {
                                            ioBuffer.flip();
                                            final int bytesToWrite = ioBuffer.remaining();
                                            writeToFile(ioBuffer);
                                            writtenOffset.getAndAdd(bytesToWrite);
                                            ioBuffer.clear();
                                        }
                                    }
                                }
                                expectedLocation += operation.data.length();
                            }
                        }

                        ioBuffer.flip();
                        final int bytesToWrite = ioBuffer.remaining();
                        writeToFile(ioBuffer);
                        writtenOffset.getAndAdd(bytesToWrite);
                        lastWrittenCheckpoint = new Checkpoint(writtenOffset.get(), numOps, generation, minSeqNo, maxSeqNo,
                            globalCheckpointSupplier.getAsLong(), minTranslogGenerationSupplier.getAsLong(),
                            SequenceNumbers.UNASSIGNED_SEQ_NO);

                        drainSorterToBuffer();
                    } finally {
                        Releasables.closeWhileHandlingException(operationSorter);
                        operationSorter.clear();
                    }
                }
            }
        }
    }

    private void drainSorterToBuffer() {
        buffer.addAll(operationSorter);
        operationSorter.clear();
    }

    private Operation nextOperation(long expectedLocation) {
        Operation peeked = operationSorter.peek();
        if (peeked != null && peeked.translogLocation == expectedLocation) {
            return operationSorter.poll();
        } else {
            Operation operation;
            while ((operation = this.buffer.pollFirst()) != null) {
                if (operation.translogLocation == expectedLocation) {
                    return operation;
                } else {
                    operationSorter.add(operation);
                }
            }
            return null;
        }
    }

    private void writeToFile(ByteBuffer ioBuffer) throws IOException {
        try {
            while (ioBuffer.remaining() > 0) {
                channel.write(ioBuffer);
            }
        } catch (final Exception ex) {
            closeWithTragicEvent(ex);
            throw ex;
        }
    }

    @Override
    protected void readBytes(ByteBuffer targetBuffer, long position) throws IOException {
        try {
            // we only flush here if it's really really needed - try to minimize the impact of the read operation
            // in some cases ie. a tragic event we might still be able to read the relevant value
            // which is not really important in production but some test can make most strict assumptions
            // if we don't fail in this call unless absolutely necessary.
            writeUpTo(position + targetBuffer.remaining(), true);
        } catch (final Exception ex) {
            closeWithTragicEvent(ex);
            throw ex;
        }
        // we don't have to have a lock here because we only write ahead to the file, so all writes has been complete
        // for the requested location.
        Channels.readFromFileChannelWithEofException(channel, position, targetBuffer);
    }

    private static void writeCheckpoint(
        final FileChannel fileChannel,
        final Path translogFile,
        final Checkpoint checkpoint) throws IOException {
        fileChannel.position(0);
        Checkpoint.write(fileChannel, translogFile.resolve(Translog.CHECKPOINT_FILE_NAME), checkpoint);
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
            throwAlreadyClosedException();
        }
    }

    private void throwAlreadyClosedException() {
        throw new AlreadyClosedException("translog [" + getGeneration() + "] is already closed", tragedy.get());
    }

    @Override
    public final void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            releaseQueuedOperations();
            // TODO: Exception handling
            checkPointChannel.close();
            channel.close();
        }
    }

    // TODO: Bugs
    private void releaseQueuedOperations() {
        Operation operation;
        while ((operation = buffer.pollFirst()) != null) {
            Releasables.closeWhileHandlingException(operation);
        }
    }

    protected final boolean isClosed() {
        return closed.get();
    }


    private static final class Operation implements Releasable {

        private final long seqNo;
        private final long translogLocation;
        private final ReleasableBytesReference data;

        private Operation(long seqNo, long translogLocation, ReleasableBytesReference data) {
            this.seqNo = seqNo;
            this.translogLocation = translogLocation;
            this.data = data;
        }

        @Override
        public void close() {
            data.close();
        }
    }
}
