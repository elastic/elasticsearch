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
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

/**
 * Each translog file is started with a translog header then followed by translog operations.
 */
final class TranslogHeader {
    public static final String TRANSLOG_CODEC = "translog";

    public static final int VERSION_CHECKSUMS    = 1; // pre-2.0 - unsupported
    public static final int VERSION_CHECKPOINTS  = 2; // added checkpoints
    public static final int VERSION_PRIMARY_TERM = 3; // added primary term
    public static final int CURRENT_VERSION = VERSION_PRIMARY_TERM;

    private final String translogUUID;
    private final long primaryTerm;
    private final int headerSizeInBytes;

    /**
     * Creates a new translog header with the given uuid and primary term.
     *
     * @param translogUUID this UUID is used to prevent accidental recovery from a transaction log that belongs to a
     *                     different engine
     * @param primaryTerm  the primary term of the owning index shard when creating (eg. rolling) this translog file.
     *                     All operations' terms in this translog file are enforced to be at most this term.
     */
    TranslogHeader(String translogUUID, long primaryTerm) {
        this(translogUUID, primaryTerm, headerSizeInBytes(translogUUID));
        assert primaryTerm >= 0 : "Primary term must be non-negative; term [" + primaryTerm + "]";
    }

    private TranslogHeader(String translogUUID, long primaryTerm, int headerSizeInBytes) {
        this.translogUUID = translogUUID;
        this.primaryTerm = primaryTerm;
        this.headerSizeInBytes = headerSizeInBytes;
    }

    public String getTranslogUUID() {
        return translogUUID;
    }

    /**
     * Returns the primary term stored in this translog header.
     * All operations in a translog file are expected to have their primary terms at most this term.
     */
    public long getPrimaryTerm() {
        return primaryTerm;
    }

    /**
     * Returns the header size in bytes. This value can be used as the offset of the first translog operation.
     * See {@link BaseTranslogReader#getFirstOperationOffset()}
     */
    public int sizeInBytes() {
        return headerSizeInBytes;
    }

    static int headerSizeInBytes(String translogUUID) {
        return headerSizeInBytes(CURRENT_VERSION, new BytesRef(translogUUID).length);
    }

    private static int headerSizeInBytes(int version, int uuidLength) {
        int size = CodecUtil.headerLength(TRANSLOG_CODEC);
        size += Integer.BYTES + uuidLength; // uuid
        if (version >= VERSION_PRIMARY_TERM) {
            size += Long.BYTES;    // primary term
            size += Integer.BYTES; // checksum
        }
        return size;
    }

    static int readHeaderVersion(final Path path, final FileChannel channel, final StreamInput in) throws IOException {
        final int version;
        try {
            version = CodecUtil.checkHeader(new InputStreamDataInput(in), TRANSLOG_CODEC, VERSION_CHECKSUMS, VERSION_PRIMARY_TERM);
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException e) {
            tryReportOldVersionError(path, channel);
            throw new TranslogCorruptedException(path.toString(), "translog header corrupted", e);
        }
        if (version == VERSION_CHECKSUMS) {
            throw new IllegalStateException("pre-2.0 translog found [" + path + "]");
        }
        if (version == VERSION_CHECKPOINTS) {
            throw new IllegalStateException("pre-6.3 translog found [" + path + "]");
        }
        return version;
    }

    /**
     * Read a translog header from the given path and file channel
     */
    static TranslogHeader read(final String translogUUID, final Path path, final FileChannel channel) throws IOException {
        try {
            // This input is intentionally not closed because closing it will close the FileChannel.
            final BufferedChecksumStreamInput in =
                new BufferedChecksumStreamInput(
                    new InputStreamStreamInput(java.nio.channels.Channels.newInputStream(channel), channel.size()),
                    path.toString());
            final int version = readHeaderVersion(path, channel, in);
            // Read the translogUUID
            final int uuidLen = in.readInt();
            if (uuidLen > channel.size()) {
                throw new TranslogCorruptedException(path.toString(), "UUID length can't be larger than the translog");
            }
            if (uuidLen <= 0) {
                throw new TranslogCorruptedException(path.toString(), "UUID length must be positive");
            }
            final BytesRef uuid = new BytesRef(uuidLen);
            uuid.length = uuidLen;
            in.read(uuid.bytes, uuid.offset, uuid.length);
            // Read the primary term
            assert version == VERSION_PRIMARY_TERM;
            final long primaryTerm = in.readLong();
            // Verify the checksum
            Translog.verifyChecksum(in);
            assert primaryTerm >= 0 : "Primary term must be non-negative [" + primaryTerm + "]; translog path [" + path + "]";

            final int headerSizeInBytes = headerSizeInBytes(version, uuid.length);
            assert channel.position() == headerSizeInBytes :
                "Header is not fully read; header size [" + headerSizeInBytes + "], position [" + channel.position() + "]";

            // verify UUID only after checksum, to ensure that UUID is not corrupted
            final BytesRef expectedUUID = new BytesRef(translogUUID);
            if (uuid.bytesEquals(expectedUUID) == false) {
                throw new TranslogCorruptedException(
                    path.toString(),
                    "expected shard UUID " + expectedUUID + " but got: " + uuid +
                        " this translog file belongs to a different translog");
            }

            return new TranslogHeader(translogUUID, primaryTerm, headerSizeInBytes);
        } catch (EOFException e) {
            throw new TranslogCorruptedException(path.toString(), "translog header truncated", e);
        }
    }

    private static void tryReportOldVersionError(final Path path, final FileChannel channel) throws IOException {
        // Lucene's CodecUtil writes a magic number of 0x3FD76C17 with the header, in binary this looks like:
        // binary: 0011 1111 1101 0111 0110 1100 0001 0111
        // hex   :    3    f    d    7    6    c    1    7
        //
        // With version 0 of the translog, the first byte is the Operation.Type, which will always be between 0-4,
        // so we know if we grab the first byte, it can be:
        // 0x3f => Lucene's magic number, so we can assume it's version 1 or later
        // 0x00 => version 0 of the translog
        final byte b1 = Channels.readFromFileChannel(channel, 0, 1)[0];
        if (b1 == 0x3f) { // LUCENE_CODEC_HEADER_BYTE
            throw new TranslogCorruptedException(
                    path.toString(),
                    "translog looks like version 1 or later, but has corrupted header" );
        } else if (b1 == 0x00) { // UNVERSIONED_TRANSLOG_HEADER_BYTE
            throw new IllegalStateException("pre-1.4 translog found [" + path + "]");
        }
    }

    /**
     * Writes this header with the latest format into the file channel
     */
    void write(final FileChannel channel) throws IOException {
        // This output is intentionally not closed because closing it will close the FileChannel.
        @SuppressWarnings({"IOResourceOpenedButNotSafelyClosed", "resource"})
        final BufferedChecksumStreamOutput out = new BufferedChecksumStreamOutput(
            new OutputStreamStreamOutput(java.nio.channels.Channels.newOutputStream(channel)));
        CodecUtil.writeHeader(new OutputStreamDataOutput(out), TRANSLOG_CODEC, CURRENT_VERSION);
        // Write uuid
        final BytesRef uuid = new BytesRef(translogUUID);
        out.writeInt(uuid.length);
        out.writeBytes(uuid.bytes, uuid.offset, uuid.length);
        // Write primary term
        out.writeLong(primaryTerm);
        // Checksum header
        out.writeInt((int) out.getChecksum());
        out.flush();
        channel.force(true);
        assert channel.position() == headerSizeInBytes :
            "Header is not fully written; header size [" + headerSizeInBytes + "], channel position [" + channel.position() + "]";
    }
}
