/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.translog;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.BufferedChecksum;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.io.DiskIoBufferPool;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

public class TranslogWriter extends BaseTranslogReader implements Closeable {

    private final ShardId shardId;
    private final FileChannel checkpointChannel;
    private final Path checkpointPath;

    // the last checkpoint that was written when the translog was last synced
    private volatile Checkpoint lastSyncedCheckpoint;
    /* the number of translog operations written to this file */
    private volatile int operationCounter;
    /* if we hit an exception that we can't recover from we assign it to this var and ship it with every AlreadyClosedException we throw */
    private final TragicExceptionHolder tragedy;
    /* the total offset of this file including the bytes written to the file as well as into the buffer */
    private volatile long totalOffset;

    private volatile long minSeqNo;
    private volatile long maxSeqNo;

    private final LongSupplier globalCheckpointSupplier;
    private final LongSupplier minTranslogGenerationSupplier;

    // callback that's called whenever an operation with a given sequence number is successfully persisted.
    private final LongConsumer persistedSequenceNumberConsumer;
    private final OperationListener operationListener;

    protected final AtomicBoolean closed = new AtomicBoolean(false);
    // lock order try(Releasable lock = writeLock.acquire()) -> synchronized(this)
    private final ReleasableLock writeLock = new ReleasableLock(new ReentrantLock());
    // lock order synchronized(syncLock) -> try(Releasable lock = writeLock.acquire()) -> synchronized(this)
    private final Object syncLock = new Object();

    private List<Long> nonFsyncedSequenceNumbers = new ArrayList<>(64);
    private final int forceWriteThreshold;
    private volatile long bufferedBytes;
    private ArrayList<Translog.SizedWriteable> buffer = new ArrayList<>();

    private final Checksum digest = new BufferedChecksum(new CRC32());

    private final Map<Long, Tuple<BytesReference, Exception>> seenSequenceNumbers;

    private final DiskIoBufferPool diskIoBufferPool;

    // fallback buffer to use when a direct buffer for writes is not available to the current thread
    private final ByteBuffer localWriteBuffer = ByteBuffer.allocate(DiskIoBufferPool.BUFFER_SIZE);

    // package private for testing
    LastModifiedTimeCache lastModifiedTimeCache;

    // package private for testing
    record LastModifiedTimeCache(long lastModifiedTime, long totalOffset, long syncedOffset) {}

    private TranslogWriter(
        final ShardId shardId,
        final Checkpoint initialCheckpoint,
        final FileChannel channel,
        final FileChannel checkpointChannel,
        final Path path,
        final Path checkpointPath,
        final ByteSizeValue bufferSize,
        final LongSupplier globalCheckpointSupplier,
        LongSupplier minTranslogGenerationSupplier,
        TranslogHeader header,
        final TragicExceptionHolder tragedy,
        final LongConsumer persistedSequenceNumberConsumer,
        final BigArrays bigArrays,
        final DiskIoBufferPool diskIoBufferPool,
        final OperationListener operationListener
    ) throws IOException {
        super(initialCheckpoint.generation, channel, path, header);
        assert initialCheckpoint.offset == channel.position()
            : "initial checkpoint offset ["
                + initialCheckpoint.offset
                + "] is different than current channel position ["
                + channel.position()
                + "]";
        this.forceWriteThreshold = Math.toIntExact(bufferSize.getBytes());
        this.shardId = shardId;
        this.checkpointChannel = checkpointChannel;
        this.checkpointPath = checkpointPath;
        this.minTranslogGenerationSupplier = minTranslogGenerationSupplier;
        this.lastSyncedCheckpoint = initialCheckpoint;
        this.totalOffset = initialCheckpoint.offset;
        assert initialCheckpoint.minSeqNo == SequenceNumbers.NO_OPS_PERFORMED : initialCheckpoint.minSeqNo;
        this.minSeqNo = initialCheckpoint.minSeqNo;
        assert initialCheckpoint.maxSeqNo == SequenceNumbers.NO_OPS_PERFORMED : initialCheckpoint.maxSeqNo;
        this.maxSeqNo = initialCheckpoint.maxSeqNo;
        assert initialCheckpoint.trimmedAboveSeqNo == SequenceNumbers.UNASSIGNED_SEQ_NO : initialCheckpoint.trimmedAboveSeqNo;
        this.globalCheckpointSupplier = globalCheckpointSupplier;
        this.persistedSequenceNumberConsumer = persistedSequenceNumberConsumer;
        this.diskIoBufferPool = diskIoBufferPool;
        this.seenSequenceNumbers = Assertions.ENABLED ? new HashMap<>() : null;
        this.tragedy = tragedy;
        this.operationListener = operationListener;
        this.lastModifiedTimeCache = new LastModifiedTimeCache(-1, -1, -1);
    }

    public static TranslogWriter create(
        ShardId shardId,
        String translogUUID,
        long fileGeneration,
        Path file,
        ChannelFactory channelFactory,
        ByteSizeValue bufferSize,
        final long initialMinTranslogGen,
        long initialGlobalCheckpoint,
        final LongSupplier globalCheckpointSupplier,
        final LongSupplier minTranslogGenerationSupplier,
        final long primaryTerm,
        TragicExceptionHolder tragedy,
        final LongConsumer persistedSequenceNumberConsumer,
        final BigArrays bigArrays,
        DiskIoBufferPool diskIoBufferPool,
        final OperationListener operationListener

    ) throws IOException {
        final Path checkpointFile = file.getParent().resolve(Translog.CHECKPOINT_FILE_NAME);

        final FileChannel channel = channelFactory.open(file);
        FileChannel checkpointChannel = null;
        try {
            checkpointChannel = channelFactory.open(checkpointFile, StandardOpenOption.WRITE);
            final TranslogHeader header = new TranslogHeader(translogUUID, primaryTerm);
            header.write(channel);
            final Checkpoint checkpoint = Checkpoint.emptyTranslogCheckpoint(
                header.sizeInBytes(),
                fileGeneration,
                initialGlobalCheckpoint,
                initialMinTranslogGen
            );
            Checkpoint.write(checkpointChannel, checkpointFile, checkpoint);
            final LongSupplier writerGlobalCheckpointSupplier;
            if (Assertions.ENABLED) {
                writerGlobalCheckpointSupplier = () -> {
                    long gcp = globalCheckpointSupplier.getAsLong();
                    assert gcp >= initialGlobalCheckpoint
                        : "global checkpoint [" + gcp + "] lower than initial gcp [" + initialGlobalCheckpoint + "]";
                    return gcp;
                };
            } else {
                writerGlobalCheckpointSupplier = globalCheckpointSupplier;
            }
            return new TranslogWriter(
                shardId,
                checkpoint,
                channel,
                checkpointChannel,
                file,
                checkpointFile,
                bufferSize,
                writerGlobalCheckpointSupplier,
                minTranslogGenerationSupplier,
                header,
                tragedy,
                persistedSequenceNumberConsumer,
                bigArrays,
                diskIoBufferPool,
                operationListener
            );
        } catch (Exception exception) {
            // if we fail to bake the file-generation into the checkpoint we stick with the file and once we recover and that
            // file exists we remove it. We only apply this logic to the checkpoint.generation+1 any other file with a higher generation
            // is an error condition
            IOUtils.closeWhileHandlingException(channel, checkpointChannel);
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

    private static void writeOperationWithSize(StreamOutput out, Translog.SizedWriteable op, Checksum digest) throws IOException {
        final long start = out.position();
        final int operationSize = op.getSizeInBytes() + Integer.BYTES; // op_size + checksum
        out.writeInt(operationSize);

        digest.reset();
        op.writeTo(out);
        long checksum = digest.getValue();
        out.writeInt((int) checksum);

        final long end = out.position();
        final int writtenOperationSize = (int) (end - Integer.BYTES - start);

        assert writtenOperationSize == operationSize;
    }

    /**
     * Add the given operation to the translog with the specified sequence number; returns the location the bytes were written to.
     *
     * @param data the operation data to write to the translog
     * @return the location the bytes were written to
     * @throws IOException if writing to the translog resulted in an I/O exception
     */
    public Translog.Location add(final Translog.SizedWriteable data, final long seqNo) throws IOException {

        long bufferedBytesBeforeAdd = this.bufferedBytes;
        if (bufferedBytesBeforeAdd >= forceWriteThreshold) {
            writeBufferedOps(Long.MAX_VALUE, bufferedBytesBeforeAdd >= forceWriteThreshold * 4L);
        }

        final Translog.Location location;
        synchronized (this) {
            ensureOpen();

            var dataLength = data.getSizeInBytes() + 2 * Integer.BYTES; // size + checksum
            buffer.add(data);

            final long offset = totalOffset;
            totalOffset += dataLength;

            assert minSeqNo != SequenceNumbers.NO_OPS_PERFORMED || operationCounter == 0;
            assert maxSeqNo != SequenceNumbers.NO_OPS_PERFORMED || operationCounter == 0;

            minSeqNo = SequenceNumbers.min(minSeqNo, seqNo);
            maxSeqNo = SequenceNumbers.max(maxSeqNo, seqNo);

            nonFsyncedSequenceNumbers.add(seqNo);

            operationCounter++;

            assert assertNoSeqNumberConflict(seqNo, data);

            location = new Translog.Location(generation, offset, dataLength);
            // operationListener.operationAdded(data, seqNo, location);
            bufferedBytes += dataLength;
        }

        return location;
    }

    private synchronized boolean assertNoSeqNumberConflict(long seqNo, Translog.SizedWriteable operation) throws IOException {
        if (seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO) {
            // nothing to do
            return true;
        }

        var dataLength = operation.getSizeInBytes() + 2 * Integer.BYTES; // size + checksum
        var digest = new BufferedChecksum(new CRC32());
        BytesArray data = new BytesArray(new byte[dataLength]);
        writeOperationWithSize(new BytesArrayStreamOutput(data, digest), operation, digest);

        if (seenSequenceNumbers.containsKey(seqNo)) {
            final Tuple<BytesReference, Exception> previous = seenSequenceNumbers.get(seqNo);
            if (previous.v1().equals(data) == false) {
                Translog.Operation newOp = (Translog.Operation) operation;
                Translog.Operation prvOp = Translog.readOperation(
                    new BufferedChecksumStreamInput(previous.v1().streamInput(), "assertion")
                );
                // TODO: We haven't had timestamp for Index operations in Lucene yet, we need to loosen this check without timestamp.
                final boolean sameOp;
                if (newOp instanceof final Translog.Index o2 && prvOp instanceof final Translog.Index o1) {
                    sameOp = Objects.equals(o1.id(), o2.id())
                        && Objects.equals(o1.source(), o2.source())
                        && Objects.equals(o1.routing(), o2.routing())
                        && o1.primaryTerm() == o2.primaryTerm()
                        && o1.seqNo() == o2.seqNo()
                        && o1.version() == o2.version();
                } else if (newOp instanceof final Translog.Delete o1 && prvOp instanceof final Translog.Delete o2) {
                    sameOp = Objects.equals(o1.id(), o2.id())
                        && o1.primaryTerm() == o2.primaryTerm()
                        && o1.seqNo() == o2.seqNo()
                        && o1.version() == o2.version();
                } else {
                    sameOp = false;
                }
                if (sameOp == false) {
                    throw new AssertionError(
                        "seqNo ["
                            + seqNo
                            + "] was processed twice in generation ["
                            + generation
                            + "], with different data. "
                            + "prvOp ["
                            + prvOp
                            + "], newOp ["
                            + newOp
                            + "]",
                        previous.v2()
                    );
                }
            }
        } else {
            seenSequenceNumbers.put(seqNo, new Tuple<>(data, new RuntimeException("stack capture previous op")));
        }
        return true;
    }

    synchronized boolean assertNoSeqAbove(long belowTerm, long aboveSeqNo) {
        seenSequenceNumbers.entrySet().stream().filter(e -> e.getKey().longValue() > aboveSeqNo).forEach(e -> {
            final Translog.Operation op;
            try {
                op = Translog.readOperation(new BufferedChecksumStreamInput(e.getValue().v1().streamInput(), "assertion"));
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
            long seqNo = op.seqNo();
            long primaryTerm = op.primaryTerm();
            if (primaryTerm < belowTerm) {
                throw new AssertionError(
                    "current should not have any operations with seq#:primaryTerm ["
                        + seqNo
                        + ":"
                        + primaryTerm
                        + "] > "
                        + aboveSeqNo
                        + ":"
                        + belowTerm
                );
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
        syncUpTo(Long.MAX_VALUE, SequenceNumbers.UNASSIGNED_SEQ_NO);
    }

    /**
     * Returns <code>true</code> if there are buffered operations that have not been flushed and fsynced to disk or if the latest global
     * checkpoint has not yet been fsynced
     */
    public boolean syncNeeded() {
        return totalOffset != lastSyncedCheckpoint.offset
            || globalCheckpointSupplier.getAsLong() != lastSyncedCheckpoint.globalCheckpoint
            || minTranslogGenerationSupplier.getAsLong() != lastSyncedCheckpoint.minTranslogGeneration;
    }

    @Override
    public int totalOperations() {
        return operationCounter;
    }

    @Override
    synchronized Checkpoint getCheckpoint() {
        return new Checkpoint(
            totalOffset,
            operationCounter,
            generation,
            minSeqNo,
            maxSeqNo,
            globalCheckpointSupplier.getAsLong(),
            minTranslogGenerationSupplier.getAsLong(),
            SequenceNumbers.UNASSIGNED_SEQ_NO
        );
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
        // After the sync lock we acquire the write lock to avoid deadlocks with threads writing where
        // the write lock is acquired first followed by synchronize(this).
        //
        // Note: While this is not strictly needed as this method is called while blocking all ops on the translog,
        // we do this to for correctness and preventing future issues.
        synchronized (syncLock) {
            try (ReleasableLock toClose = writeLock.acquire()) {
                synchronized (this) {
                    try {
                        sync(); // sync before we close..
                    } catch (final Exception ex) {
                        closeWithTragicEvent(ex);
                        throw ex;
                    }
                    // If we reached this point, all of the buffered ops should have been flushed successfully.
                    assert buffer.isEmpty();
                    assert checkChannelPositionWhileHandlingException(totalOffset);
                    assert totalOffset == lastSyncedCheckpoint.offset;
                    if (closed.compareAndSet(false, true)) {
                        try {
                            checkpointChannel.close();
                        } catch (final Exception ex) {
                            closeWithTragicEvent(ex);
                            throw ex;
                        }
                        return new TranslogReader(getLastSyncedCheckpoint(), channel, path, header);
                    } else {
                        throw new AlreadyClosedException(
                            "translog [" + getGeneration() + "] is already closed (path [" + path + "]",
                            tragedy.get()
                        );
                    }
                }
            }
        }
    }

    @Override
    public TranslogSnapshot newSnapshot() {
        // make sure to acquire the sync lock first, to prevent dead locks with threads calling
        // syncUpTo() , where the sync lock is acquired first, following by the synchronize(this)
        // After the sync lock we acquire the write lock to avoid deadlocks with threads writing where
        // the write lock is acquired first followed by synchronize(this).
        synchronized (syncLock) {
            try (ReleasableLock toClose = writeLock.acquire()) {
                synchronized (this) {
                    ensureOpen();
                    try {
                        sync();
                    } catch (IOException e) {
                        throw new TranslogException(shardId, "exception while syncing before creating a snapshot", e);
                    }
                    // If we reached this point, all of the buffered ops should have been flushed successfully.
                    assert buffer.isEmpty();
                    assert checkChannelPositionWhileHandlingException(totalOffset);
                    assert totalOffset == lastSyncedCheckpoint.offset;
                    return super.newSnapshot();
                }
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
    final boolean syncUpTo(long offset, long globalCheckpointToPersist) throws IOException {
        if ((lastSyncedCheckpoint.offset < offset || lastSyncedCheckpoint.globalCheckpoint < globalCheckpointToPersist) && syncNeeded()) {
            assert globalCheckpointToPersist <= globalCheckpointSupplier.getAsLong()
                : "globalCheckpointToPersist ["
                    + globalCheckpointToPersist
                    + "] greater than global checkpoint ["
                    + globalCheckpointSupplier.getAsLong()
                    + "]";
            synchronized (syncLock) { // only one sync/checkpoint should happen concurrently but we wait
                if ((lastSyncedCheckpoint.offset < offset || lastSyncedCheckpoint.globalCheckpoint < globalCheckpointToPersist)
                    && syncNeeded()) {
                    // double checked locking - we don't want to fsync unless we have to and now that we have
                    // the lock we should check again since if this code is busy we might have fsynced enough already
                    final Checkpoint checkpointToSync;
                    final List<Long> flushedSequenceNumbers;
                    final List<Translog.SizedWriteable> toWrite;
                    try (ReleasableLock toClose = writeLock.acquire()) {
                        synchronized (this) {
                            ensureOpen();
                            checkpointToSync = getCheckpoint();
                            toWrite = pollOpsToWrite();
                            if (nonFsyncedSequenceNumbers.isEmpty()) {
                                flushedSequenceNumbers = null;
                            } else {
                                flushedSequenceNumbers = nonFsyncedSequenceNumbers;
                                nonFsyncedSequenceNumbers = new ArrayList<>(64);
                            }
                        }

                        try {
                            // Write ops will release operations.
                            writeAndReleaseOps(toWrite);
                            assert channel.position() == checkpointToSync.offset;
                        } catch (final Exception ex) {
                            closeWithTragicEvent(ex);
                            throw ex;
                        }
                    }
                    // now do the actual fsync outside of the synchronized block such that
                    // we can continue writing to the buffer etc.
                    try {
                        assert lastSyncedCheckpoint.offset != checkpointToSync.offset || toWrite.isEmpty();
                        if (lastSyncedCheckpoint.offset != checkpointToSync.offset) {
                            channel.force(false);
                        }
                        Checkpoint.write(checkpointChannel, checkpointPath, checkpointToSync);
                    } catch (final Exception ex) {
                        closeWithTragicEvent(ex);
                        throw ex;
                    }
                    if (flushedSequenceNumbers != null) {
                        flushedSequenceNumbers.forEach(persistedSequenceNumberConsumer::accept);
                    }
                    assert lastSyncedCheckpoint.offset <= checkpointToSync.offset
                        : "illegal state: " + lastSyncedCheckpoint.offset + " <= " + checkpointToSync.offset;
                    lastSyncedCheckpoint = checkpointToSync; // write protected by syncLock
                    return true;
                }
            }
        }
        return false;
    }

    private void writeBufferedOps(long offset, boolean blockOnExistingWriter) throws IOException {
        try (ReleasableLock locked = blockOnExistingWriter ? writeLock.acquire() : writeLock.tryAcquire()) {
            try {
                if (locked != null && offset > getWrittenOffset()) {
                    writeAndReleaseOps(pollOpsToWrite());
                }
            } catch (Exception e) {
                closeWithTragicEvent(e);
                throw e;
            }
        }
    }

    private synchronized List<Translog.SizedWriteable> pollOpsToWrite() {
        ensureOpen();
        if (this.buffer.isEmpty()) {
            return List.of();
        } else {
            var toWrite = this.buffer;
            this.buffer = new ArrayList<>();
            this.bufferedBytes = 0;
            return toWrite;
        }
    }

    private void writeAndReleaseOps(List<Translog.SizedWriteable> toWrite) throws IOException {
        assert writeLock.isHeldByCurrentThread();
        if (toWrite.isEmpty()) {
            return;
        }

        ByteBuffer ioBuffer = diskIoBufferPool.maybeGetDirectIOBuffer();
        if (ioBuffer == null) {
            // not using a direct buffer for writes from the current thread, falling back to a regular buffer
            ioBuffer = localWriteBuffer;
            ioBuffer.clear();
        }

        try (StreamOutput streamOutput = new ByteBufferStreamOutput(ioBuffer, channel, digest)) {
            for (final var sizedWriteable : toWrite) {
                writeOperationWithSize(streamOutput, sizedWriteable, digest);
            }
        }
    }

    @Override
    protected void readBytes(ByteBuffer targetBuffer, long position) throws IOException {
        try {
            if (position + targetBuffer.remaining() > getWrittenOffset()) {
                // we only flush here if it's really really needed - try to minimize the impact of the read operation
                // in some cases ie. a tragic event we might still be able to read the relevant value
                // which is not really important in production but some test can make most strict assumptions
                // if we don't fail in this call unless absolutely necessary.
                writeBufferedOps(position + targetBuffer.remaining(), true);
            }
        } catch (final Exception ex) {
            closeWithTragicEvent(ex);
            throw ex;
        }
        // we don't have to have a lock here because we only write ahead to the file, so all writes has been complete
        // for the requested location.
        Channels.readFromFileChannelWithEofException(channel, position, targetBuffer);
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

    private boolean checkChannelPositionWhileHandlingException(long expectedOffset) {
        try {
            return expectedOffset == channel.position();
        } catch (IOException e) {
            return true;
        }
    }

    @Override
    public final void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            synchronized (this) {
                buffer = new ArrayList<>();
                bufferedBytes = 0;
            }
            IOUtils.close(checkpointChannel, channel);
        }
    }

    protected final boolean isClosed() {
        return closed.get();
    }

    @Override
    public long getLastModifiedTime() throws IOException {
        if (lastModifiedTimeCache.totalOffset() != totalOffset || lastModifiedTimeCache.syncedOffset() != lastSyncedCheckpoint.offset) {
            long mtime = super.getLastModifiedTime();
            lastModifiedTimeCache = new LastModifiedTimeCache(mtime, totalOffset, lastSyncedCheckpoint.offset);
        }
        return lastModifiedTimeCache.lastModifiedTime();
    }

    private static class ByteBufferStreamOutput extends StreamOutput {
        private final ByteBuffer ioBuffer;
        private final WritableByteChannel channel;
        private final Checksum digest;
        private long position;

        ByteBufferStreamOutput(ByteBuffer byteBuffer, WritableByteChannel channel, Checksum digest) {
            this.ioBuffer = byteBuffer;
            this.channel = channel;
            this.digest = digest;
        }

        @Override
        public long position() {
            return position;
        }

        @Override
        public void writeByte(byte b) throws IOException {
            if (ioBuffer.hasRemaining() == false) {
                ioBuffer.flip();
                writeToFile(ioBuffer);
                ioBuffer.clear();
            }
            ioBuffer.put(b);
            digest.update(b);
            position += 1;
        }

        @Override
        public void writeBytes(byte[] bytes, int offset, int length) throws IOException {
            int currentBytesConsumed = 0;
            while (currentBytesConsumed != length) {
                int nBytesToWrite = Math.min(length - currentBytesConsumed, ioBuffer.remaining());
                ioBuffer.put(bytes, offset + currentBytesConsumed, nBytesToWrite);
                currentBytesConsumed += nBytesToWrite;
                if (ioBuffer.hasRemaining() == false) {
                    ioBuffer.flip();
                    writeToFile(ioBuffer);
                    ioBuffer.clear();
                }
            }
            digest.update(bytes, offset, length);
            position += length;
        }

        @Override
        public void flush() {}

        @Override
        public void close() throws IOException {
            ioBuffer.flip();
            writeToFile(ioBuffer);
        }

        @SuppressForbidden(reason = "Channel#write")
        private void writeToFile(ByteBuffer ioBuffer) throws IOException {
            while (ioBuffer.remaining() > 0) {
                channel.write(ioBuffer);
            }
        }
    }

    private static class BytesArrayStreamOutput extends BytesStream {
        private final BytesArray bytes;
        private final Checksum digest;
        private int count;

        BytesArrayStreamOutput(BytesArray bytes, Checksum digest) {
            super();
            this.bytes = bytes;
            this.digest = digest;
        }

        @Override
        public BytesReference bytes() {
            return bytes;
        }

        @Override
        public long position() {
            return count;
        }

        @Override
        public void writeByte(byte b) {
            bytes.array()[count] = b;
            digest.update(b);
            count++;
        }

        @Override
        public void writeBytes(byte[] b, int offset, int length) {
            // nothing to copy
            if (length == 0) {
                return;
            }

            Objects.checkFromIndexSize(offset, length, b.length);
            System.arraycopy(b, offset, bytes.array(), count, length);
            digest.update(b, offset, length);
            count += length;
        }

        @Override
        public void flush() {}

        @Override
        public void close() {}
    }
}
