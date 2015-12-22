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
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.Callback;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.index.shard.ShardId;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TranslogWriter extends TranslogReader {

    public static final String TRANSLOG_CODEC = "translog";
    public static final int VERSION_CHECKSUMS = 1;
    public static final int VERSION_CHECKPOINTS = 2; // since 2.0 we have checkpoints?
    public static final int VERSION = VERSION_CHECKPOINTS;

    private final ShardId shardId;
    private final ReleasableLock readLock;
    private final ReleasableLock writeLock;
    /* the offset in bytes that was written when the file was last synced*/
    private volatile long lastSyncedOffset;
    /* the number of translog operations written to this file */
    private volatile int operationCounter;
    /* the offset in bytes written to the file */
    private volatile long writtenOffset;
    /* if we hit an exception that we can't recover from we assign it to this var and ship it with every AlreadyClosedException we throw */
    private volatile Throwable tragedy;

    private final byte[] buffer;
    private int bufferCount;
    private WrapperOutputStream bufferOs = new WrapperOutputStream();

    /* the total offset of this file including the bytes written to the file as well as into the buffer */
    private volatile long totalOffset;


    public TranslogWriter(ShardId shardId, long generation, ChannelReference channelReference, ByteSizeValue bufferSize) throws IOException {
        super(generation, channelReference, channelReference.getChannel().position());
        this.shardId = shardId;
        ReadWriteLock rwl = new ReentrantReadWriteLock();
        readLock = new ReleasableLock(rwl.readLock());
        writeLock = new ReleasableLock(rwl.writeLock());
        this.writtenOffset = channelReference.getChannel().position();
        this.totalOffset = writtenOffset;
        this.buffer = new byte[bufferSize.bytesAsInt()];
        this.lastSyncedOffset = channelReference.getChannel().position();
    }

    public static TranslogWriter create(ShardId shardId, String translogUUID, long fileGeneration, Path file, Callback<ChannelReference> onClose, ChannelFactory channelFactory, ByteSizeValue bufferSize) throws IOException {
        final BytesRef ref = new BytesRef(translogUUID);
        final int headerLength = CodecUtil.headerLength(TRANSLOG_CODEC) + ref.length + RamUsageEstimator.NUM_BYTES_INT;
        final FileChannel channel = channelFactory.open(file);
        try {
            // This OutputStreamDataOutput is intentionally not closed because
            // closing it will close the FileChannel
            final OutputStreamDataOutput out = new OutputStreamDataOutput(java.nio.channels.Channels.newOutputStream(channel));
            CodecUtil.writeHeader(out, TRANSLOG_CODEC, VERSION);
            out.writeInt(ref.length);
            out.writeBytes(ref.bytes, ref.offset, ref.length);
            channel.force(false);
            writeCheckpoint(headerLength, 0, file.getParent(), fileGeneration, StandardOpenOption.WRITE);
            final TranslogWriter writer = new TranslogWriter(shardId, fileGeneration, new ChannelReference(file, fileGeneration, channel, onClose), bufferSize);
            return writer;
        } catch (Throwable throwable){
            IOUtils.closeWhileHandlingException(channel);
            try {
                Files.delete(file); // remove the file as well
            } catch (IOException ex) {
                throwable.addSuppressed(ex);
            }
            throw throwable;
        }
    }
    /** If this {@code TranslogWriter} was closed as a side-effect of a tragic exception,
     *  e.g. disk full while flushing a new segment, this returns the root cause exception.
     *  Otherwise (no tragic exception has occurred) it returns null. */
    public Throwable getTragicException() {
        return tragedy;
    }

    private final void closeWithTragicEvent(Throwable throwable) throws IOException {
        try (ReleasableLock lock = writeLock.acquire()) {
            if (tragedy == null) {
                tragedy = throwable;
            } else {
                tragedy.addSuppressed(throwable);
            }
            close();
        }
    }

    /**
     * add the given bytes to the translog and return the location they were written at
     */
    public Translog.Location add(BytesReference data) throws IOException {
        try (ReleasableLock lock = writeLock.acquire()) {
            ensureOpen();
            final long offset = totalOffset;
            if (data.length() >= buffer.length) {
                flush();
                // we use the channel to write, since on windows, writing to the RAF might not be reflected
                // when reading through the channel
                try {
                    data.writeTo(channel);
                } catch (Throwable ex) {
                    closeWithTragicEvent(ex);
                    throw ex;
                }
                writtenOffset += data.length();
                totalOffset += data.length();
            } else {
                if (data.length() > buffer.length - bufferCount) {
                    flush();
                }
                data.writeTo(bufferOs);
                totalOffset += data.length();
            }
            operationCounter++;
            return new Translog.Location(generation, offset, data.length());
        }
    }

    /**
     * write all buffered ops to disk and fsync file
     */
    public synchronized void sync() throws IOException {
        if (syncNeeded()) {
            ensureOpen(); // this call gives a better exception that the incRef if we are closed by a tragic event
            channelReference.incRef();
            try {
                final long offsetToSync;
                final int opsCounter;
                try (ReleasableLock lock = writeLock.acquire()) {
                    flush();
                    offsetToSync = totalOffset;
                    opsCounter = operationCounter;
                }
                // we can do this outside of the write lock but we have to protect from
                // concurrent syncs
                ensureOpen(); // just for kicks - the checkpoint happens or not either way
                try {
                    checkpoint(offsetToSync, opsCounter, channelReference);
                } catch (Throwable ex) {
                    closeWithTragicEvent(ex);
                    throw ex;
                }
                lastSyncedOffset = offsetToSync;
            } finally {
                channelReference.decRef();
            }
        }
    }

    /**
     * returns true if there are buffered ops
     */
    public boolean syncNeeded() { return totalOffset != lastSyncedOffset; }

    @Override
    public int totalOperations() {
        return operationCounter;
    }

    @Override
    public long sizeInBytes() {
        return totalOffset;
    }


    /**
     * Flushes the buffer if the translog is buffered.
     */
    private final void flush() throws IOException {
        assert writeLock.isHeldByCurrentThread();
        if (bufferCount > 0) {
            ensureOpen();
            // we use the channel to write, since on windows, writing to the RAF might not be reflected
            // when reading through the channel
            final int bufferSize = bufferCount;
            try {
                Channels.writeToChannel(buffer, 0, bufferSize, channel);
            } catch (Throwable ex) {
                closeWithTragicEvent(ex);
                throw ex;
            }
            writtenOffset += bufferSize;
            bufferCount = 0;
        }
    }

    /**
     * returns a new reader that follows the current writes (most importantly allows making
     * repeated snapshots that includes new content)
     */
    public TranslogReader newReaderFromWriter() {
        ensureOpen();
        channelReference.incRef();
        boolean success = false;
        try {
            TranslogReader reader = new InnerReader(this.generation, firstOperationOffset, channelReference);
            success = true;
            return reader;
        } finally {
            if (!success) {
                channelReference.decRef();
            }
        }
    }

    /**
     * returns a new immutable reader which only exposes the current written operation *
     */
    public ImmutableTranslogReader immutableReader() throws TranslogException {
        if (channelReference.tryIncRef()) {
            try (ReleasableLock lock = writeLock.acquire()) {
                ensureOpen();
                flush();
                ImmutableTranslogReader reader = new ImmutableTranslogReader(this.generation, channelReference, firstOperationOffset, writtenOffset, operationCounter);
                channelReference.incRef(); // for new reader
                return reader;
            } catch (Exception e) {
                throw new TranslogException(shardId, "exception while creating an immutable reader", e);
            } finally {
                channelReference.decRef();
            }
        } else {
            throw new TranslogException(shardId, "can't increment channel [" + channelReference + "] ref count");
        }
    }

    boolean assertBytesAtLocation(Translog.Location location, BytesReference expectedBytes) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(location.size);
        readBytes(buffer, location.translogLocation);
        return new BytesArray(buffer.array()).equals(expectedBytes);
    }

    /**
     * this class is used when one wants a reference to this file which exposes all recently written operation.
     * as such it needs access to the internals of the current reader
     */
    final class InnerReader extends TranslogReader {

        public InnerReader(long generation, long fistOperationOffset, ChannelReference channelReference) {
            super(generation, channelReference, fistOperationOffset);
        }

        @Override
        public long sizeInBytes() {
            return TranslogWriter.this.sizeInBytes();
        }

        @Override
        public int totalOperations() {
            return TranslogWriter.this.totalOperations();
        }

        @Override
        protected void readBytes(ByteBuffer buffer, long position) throws IOException {
            TranslogWriter.this.readBytes(buffer, position);
        }
    }

    /**
     * Syncs the translog up to at least the given offset unless already synced
     *
     * @return <code>true</code> if this call caused an actual sync operation
     */
    public boolean syncUpTo(long offset) throws IOException {
        if (lastSyncedOffset < offset) {
            sync();
            return true;
        }
        return false;
    }

    @Override
    protected void readBytes(ByteBuffer targetBuffer, long position) throws IOException {
        try (ReleasableLock lock = readLock.acquire()) {
            if (position >= writtenOffset) {
                assert targetBuffer.hasArray() : "buffer must have array";
                final int sourcePosition = (int) (position - writtenOffset);
                System.arraycopy(buffer, sourcePosition,
                    targetBuffer.array(), targetBuffer.position(), targetBuffer.limit());
                targetBuffer.position(targetBuffer.limit());
                return;
            }
        }
        // we don't have to have a read lock here because we only write ahead to the file, so all writes has been complete
        // for the requested location.
        Channels.readFromFileChannelWithEofException(channel, position, targetBuffer);
    }

    private synchronized void checkpoint(long lastSyncPosition, int operationCounter, ChannelReference channelReference) throws IOException {
        channelReference.getChannel().force(false);
        writeCheckpoint(lastSyncPosition, operationCounter, channelReference.getPath().getParent(), channelReference.getGeneration(), StandardOpenOption.WRITE);
    }

    private static void writeCheckpoint(long syncPosition, int numOperations, Path translogFile, long generation, OpenOption... options) throws IOException {
        final Path checkpointFile = translogFile.resolve(Translog.CHECKPOINT_FILE_NAME);
        Checkpoint checkpoint = new Checkpoint(syncPosition, numOperations, generation);
        Checkpoint.write(checkpointFile, checkpoint, options);
    }

    static class ChannelFactory {

        static final ChannelFactory DEFAULT = new ChannelFactory();

        // only for testing until we have a disk-full FileSystemt
        public FileChannel open(Path file) throws IOException {
            return FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE_NEW);
        }
    }

    protected final void ensureOpen() {
        if (isClosed()) {
            throw new AlreadyClosedException("translog [" + getGeneration() + "] is already closed", tragedy);
        }
    }

    class WrapperOutputStream extends OutputStream {

        @Override
        public void write(int b) throws IOException {
            buffer[bufferCount++] = (byte) b;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            // we do safety checked when we decide to use this stream...
            System.arraycopy(b, off, buffer, bufferCount, len);
            bufferCount += len;
        }
    }
}
