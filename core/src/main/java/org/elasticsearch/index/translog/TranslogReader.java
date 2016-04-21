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
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * an immutable translog filereader
 */
public class TranslogReader extends BaseTranslogReader implements Closeable {
    private static final byte LUCENE_CODEC_HEADER_BYTE = 0x3f;
    private static final byte UNVERSIONED_TRANSLOG_HEADER_BYTE = 0x00;


    private final int totalOperations;
    protected final long length;
    protected final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Create a reader of translog file channel. The length parameter should be consistent with totalOperations and point
     * at the end of the last operation in this snapshot.
     */
    public TranslogReader(long generation, FileChannel channel, Path path, long firstOperationOffset, long length, int totalOperations) {
        super(generation, channel, path, firstOperationOffset);
        this.length = length;
        this.totalOperations = totalOperations;
    }

    /**
     * Given a file, opens an {@link TranslogReader}, taking of checking and validating the file header.
     */
    public static TranslogReader open(FileChannel channel, Path path, Checkpoint checkpoint, String translogUUID) throws IOException {

        try {
            InputStreamStreamInput headerStream = new InputStreamStreamInput(java.nio.channels.Channels.newInputStream(channel)); // don't close
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
                    throw new TranslogCorruptedException("translog looks like version 1 or later, but has corrupted header. path:" + path);
                }
                // Confirm the rest of the header using CodecUtil, extracting
                // the translog version
                int version = CodecUtil.checkHeaderNoMagic(new InputStreamDataInput(headerStream), TranslogWriter.TRANSLOG_CODEC, 1, Integer.MAX_VALUE);
                switch (version) {
                    case TranslogWriter.VERSION_CHECKSUMS:
                        throw new IllegalStateException("pre-2.0 translog found [" + path + "]");
                    case TranslogWriter.VERSION_CHECKPOINTS:
                        assert path.getFileName().toString().endsWith(Translog.TRANSLOG_FILE_SUFFIX) : "new file ends with old suffix: " + path;
                        assert checkpoint.numOps >= 0 : "expected at least 0 operatin but got: " + checkpoint.numOps;
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
                            throw new TranslogCorruptedException("expected shard UUID [" + uuidBytes + "] but got: [" + ref + "] this translog file belongs to a different translog. path:" + path);
                        }
                        return new TranslogReader(checkpoint.generation, channel, path, ref.length + CodecUtil.headerLength(TranslogWriter.TRANSLOG_CODEC) + Integer.BYTES, checkpoint.offset, checkpoint.numOps);
                    default:
                        throw new TranslogCorruptedException("No known translog stream version: " + version + " path:" + path);
                }
            } else if (b1 == UNVERSIONED_TRANSLOG_HEADER_BYTE) {
                throw new IllegalStateException("pre-1.4 translog found [" + path + "]");
            } else {
                throw new TranslogCorruptedException("Invalid first byte in translog file, got: " + Long.toHexString(b1) + ", expected 0x00 or 0x3f. path:" + path);
            }
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException e) {
            throw new TranslogCorruptedException("Translog header corrupted. path:" + path, e);
        }
    }

    public long sizeInBytes() {
        return length;
    }

    public int totalOperations() {
        return totalOperations;
    }

    /**
     * reads an operation at the given position into the given buffer.
     */
    protected void readBytes(ByteBuffer buffer, long position) throws IOException {
        if (position >= length) {
            throw new EOFException("read requested past EOF. pos [" + position + "] end: [" + length + "]");
        }
        if (position < firstOperationOffset) {
            throw new IOException("read requested before position of first ops. pos [" + position + "] first op on: [" + firstOperationOffset + "]");
        }
        Channels.readFromFileChannelWithEofException(channel, position, buffer);
    }

    public Checkpoint getInfo() {
        return new Checkpoint(length, totalOperations, getGeneration());
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

    protected void ensureOpen() {
        if (isClosed()) {
            throw new AlreadyClosedException(toString() + " is already closed");
        }
    }
}
