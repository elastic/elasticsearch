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
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A base class for all classes that allows reading ops from translog files
 */
public abstract class TranslogReader implements Closeable, Comparable<TranslogReader> {
    public static final int UNKNOWN_OP_COUNT = -1;
    private static final byte LUCENE_CODEC_HEADER_BYTE = 0x3f;
    private static final byte UNVERSIONED_TRANSLOG_HEADER_BYTE = 0x00;

    protected final long generation;
    protected final ChannelReference channelReference;
    protected final FileChannel channel;
    protected final AtomicBoolean closed = new AtomicBoolean(false);
    protected final long firstOperationOffset;

    public TranslogReader(long generation, ChannelReference channelReference, long firstOperationOffset) {
        this.generation = generation;
        this.channelReference = channelReference;
        this.channel = channelReference.getChannel();
        this.firstOperationOffset = firstOperationOffset;
    }

    public long getGeneration() {
        return this.generation;
    }

    public abstract long sizeInBytes();

    abstract public int totalOperations();

    public final long getFirstOperationOffset() {
        return firstOperationOffset;
    }

    public Translog.Operation read(Translog.Location location) throws IOException {
        assert location.generation == generation : "read location's translog generation [" + location.generation + "] is not [" + generation + "]";
        ByteBuffer buffer = ByteBuffer.allocate(location.size);
        try (BufferedChecksumStreamInput checksumStreamInput = checksummedStream(buffer, location.translogLocation, location.size, null)) {
            return read(checksumStreamInput);
        }
    }

    /** read the size of the op (i.e., number of bytes, including the op size) written at the given position */
    private final int readSize(ByteBuffer reusableBuffer, long position) {
        // read op size from disk
        assert reusableBuffer.capacity() >= 4 : "reusable buffer must have capacity >=4 when reading opSize. got [" + reusableBuffer.capacity() + "]";
        try {
            reusableBuffer.clear();
            reusableBuffer.limit(4);
            readBytes(reusableBuffer, position);
            reusableBuffer.flip();
            // Add an extra 4 to account for the operation size integer itself
            final int size = reusableBuffer.getInt() + 4;
            final long maxSize = sizeInBytes() - position;
            if (size < 0 || size > maxSize) {
                throw new TranslogCorruptedException("operation size is corrupted must be [0.." + maxSize + "] but was: " + size);
            }

            return size;
        } catch (IOException e) {
            throw new ElasticsearchException("unexpected exception reading from translog snapshot of " + this.channelReference.getPath(), e);
        }
    }

    public Translog.Snapshot newSnapshot() {
        final ByteBuffer reusableBuffer = ByteBuffer.allocate(1024);
        final int totalOperations = totalOperations();
        channelReference.incRef();
        return newReaderSnapshot(totalOperations, reusableBuffer);
    }

    /**
     * reads an operation at the given position and returns it. The buffer length is equal to the number
     * of bytes reads.
     */
    private final BufferedChecksumStreamInput checksummedStream(ByteBuffer reusableBuffer, long position, int opSize, BufferedChecksumStreamInput reuse) throws IOException {
        final ByteBuffer buffer;
        if (reusableBuffer.capacity() >= opSize) {
            buffer = reusableBuffer;
        } else {
            buffer = ByteBuffer.allocate(opSize);
        }
        buffer.clear();
        buffer.limit(opSize);
        readBytes(buffer, position);
        buffer.flip();
        return new BufferedChecksumStreamInput(new ByteBufferStreamInput(buffer), reuse);
    }

    protected Translog.Operation read(BufferedChecksumStreamInput inStream) throws IOException {
        return Translog.readOperation(inStream);
    }

    /**
     * reads bytes at position into the given buffer, filling it.
     */
    abstract protected void readBytes(ByteBuffer buffer, long position) throws IOException;

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            doClose();
        }
    }

    protected void doClose() throws IOException {
        channelReference.decRef();
    }

    protected void ensureOpen() {
        if (closed.get()) {
            throw new AlreadyClosedException("translog [" + getGeneration() + "] is already closed");
        }
    }

    @Override
    public String toString() {
        return "translog [" + generation + "][" + channelReference.getPath() + "]";
    }

    @Override
    public int compareTo(TranslogReader o) {
        return Long.compare(getGeneration(), o.getGeneration());
    }


    /**
     * Given a file, return a VersionedTranslogStream based on an
     * optionally-existing header in the file. If the file does not exist, or
     * has zero length, returns the latest version. If the header does not
     * exist, assumes Version 0 of the translog file format.
     * <p/>
     *
     * @throws IOException
     */
    public static ImmutableTranslogReader open(ChannelReference channelReference, Checkpoint checkpoint, String translogUUID) throws IOException {
        final FileChannel channel = channelReference.getChannel();
        final Path path = channelReference.getPath();
        assert channelReference.getGeneration() == checkpoint.generation : "expected generation: " + channelReference.getGeneration() + " but got: " + checkpoint.generation;

        try {
            if (checkpoint.offset == 0 && checkpoint.numOps == TranslogReader.UNKNOWN_OP_COUNT) { // only old files can be empty
                return new LegacyTranslogReader(channelReference.getGeneration(), channelReference, 0);
            }

            InputStreamStreamInput headerStream = new InputStreamStreamInput(Channels.newInputStream(channel)); // don't close
            // Lucene's CodecUtil writes a magic number of 0x3FD76C17 with the
            // header, in binary this looks like:
            //
            // binary: 0011 1111 1101 0111 0110 1100 0001 0111
            // hex   :    3    f    d    7    6    c    1    7
            //
            // With version 0 of the translog, the first byte is the
            // Operation.Type, which will always be between 0-4, so we know if
            // we grab the first byte, it can be:
            // 0x3f => Lucene's magic number, so we can assume it's version 1 or later
            // 0x00 => version 0 of the translog
            //
            // otherwise the first byte of the translog is corrupted and we
            // should bail
            byte b1 = headerStream.readByte();
            if (b1 == LUCENE_CODEC_HEADER_BYTE) {
                // Read 3 more bytes, meaning a whole integer has been read
                byte b2 = headerStream.readByte();
                byte b3 = headerStream.readByte();
                byte b4 = headerStream.readByte();
                // Convert the 4 bytes that were read into an integer
                int header = ((b1 & 0xFF) << 24) + ((b2 & 0xFF) << 16) + ((b3 & 0xFF) << 8) + ((b4 & 0xFF) << 0);
                // We confirm CodecUtil's CODEC_MAGIC number (0x3FD76C17)
                // ourselves here, because it allows us to read the first
                // byte separately
                if (header != CodecUtil.CODEC_MAGIC) {
                    throw new TranslogCorruptedException("translog looks like version 1 or later, but has corrupted header");
                }
                // Confirm the rest of the header using CodecUtil, extracting
                // the translog version
                int version = CodecUtil.checkHeaderNoMagic(new InputStreamDataInput(headerStream), TranslogWriter.TRANSLOG_CODEC, 1, Integer.MAX_VALUE);
                switch (version) {
                    case TranslogWriter.VERSION_CHECKSUMS:
                        assert checkpoint.numOps == TranslogReader.UNKNOWN_OP_COUNT : "expected unknown op count but got: " + checkpoint.numOps;
                        assert checkpoint.offset == Files.size(path) : "offset(" + checkpoint.offset + ") != file_size(" + Files.size(path) + ") for: " + path;
                        // legacy - we still have to support it somehow
                        return new LegacyTranslogReaderBase(channelReference.getGeneration(), channelReference, CodecUtil.headerLength(TranslogWriter.TRANSLOG_CODEC), checkpoint.offset);
                    case TranslogWriter.VERSION_CHECKPOINTS:
                        assert path.getFileName().toString().endsWith(Translog.TRANSLOG_FILE_SUFFIX) : "new file ends with old suffix: " + path;
                        assert checkpoint.numOps > TranslogReader.UNKNOWN_OP_COUNT: "expected at least 0 operatin but got: " + checkpoint.numOps;
                        assert checkpoint.offset <= channel.size() : "checkpoint is inconsistent with channel length: " + channel.size() + " " + checkpoint;
                        int len = headerStream.readInt();
                        if (len > channel.size()) {
                            throw new TranslogCorruptedException("uuid length can't be larger than the translog");
                        }
                        BytesRef ref = new BytesRef(len);
                        ref.length = len;
                        headerStream.read(ref.bytes, ref.offset, ref.length);
                        BytesRef uuidBytes = new BytesRef(translogUUID);
                        if (uuidBytes.bytesEquals(ref) == false) {
                            throw new TranslogCorruptedException("expected shard UUID [" + uuidBytes + "] but got: [" + ref + "] this translog file belongs to a different translog");
                        }
                        return new ImmutableTranslogReader(channelReference.getGeneration(), channelReference, ref.length + CodecUtil.headerLength(TranslogWriter.TRANSLOG_CODEC) + RamUsageEstimator.NUM_BYTES_INT, checkpoint.offset, checkpoint.numOps);
                    default:
                        throw new TranslogCorruptedException("No known translog stream version: " + version + " path:" + path);
                }
            } else if (b1 == UNVERSIONED_TRANSLOG_HEADER_BYTE) {
                assert checkpoint.numOps == TranslogReader.UNKNOWN_OP_COUNT : "expected unknown op count but got: " + checkpoint.numOps;
                assert checkpoint.offset == Files.size(path) : "offset(" + checkpoint.offset + ") != file_size(" + Files.size(path) + ") for: " + path;
                return new LegacyTranslogReader(channelReference.getGeneration(), channelReference, checkpoint.offset);
            } else {
                throw new TranslogCorruptedException("Invalid first byte in translog file, got: " + Long.toHexString(b1) + ", expected 0x00 or 0x3f");
            }
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException e) {
            throw new TranslogCorruptedException("Translog header corrupted", e);
        }
    }

    public Path path() {
        return channelReference.getPath();
    }

    protected Translog.Snapshot newReaderSnapshot(int totalOperations, ByteBuffer reusableBuffer) {
        return new ReaderSnapshot(totalOperations, reusableBuffer);
    }

    class ReaderSnapshot implements Translog.Snapshot {
        private final AtomicBoolean closed;
        private final int totalOperations;
        private final ByteBuffer reusableBuffer;
        long position;
        int readOperations;
        private BufferedChecksumStreamInput reuse;

        public ReaderSnapshot(int totalOperations, ByteBuffer reusableBuffer) {
            this.totalOperations = totalOperations;
            this.reusableBuffer = reusableBuffer;
            closed = new AtomicBoolean(false);
            position = firstOperationOffset;
            readOperations = 0;
            reuse = null;
        }

        @Override
        public final int estimatedTotalOperations() {
            return totalOperations;
        }

        @Override
        public Translog.Operation next() throws IOException {
            if (readOperations < totalOperations) {
                assert readOperations < totalOperations : "readOpeartions must be less than totalOperations";
                return readOperation();
            } else {
                return null;
            }
        }

        protected final Translog.Operation readOperation() throws IOException {
            final int opSize = readSize(reusableBuffer, position);
            reuse = checksummedStream(reusableBuffer, position, opSize, reuse);
            Translog.Operation op = read(reuse);
            position += opSize;
            readOperations++;
            return op;
        }

        @Override
        public  void close() {
            if (closed.compareAndSet(false, true)) {
                channelReference.decRef();
            }
        }
    }
}
