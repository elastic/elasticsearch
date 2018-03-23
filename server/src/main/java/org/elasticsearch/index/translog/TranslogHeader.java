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
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

/**
 * Each translog file is started with a translog header then followed by translog operations.
 */
final class TranslogHeader {
    public static final String TRANSLOG_CODEC = "translog";

    // public static final int VERSION_CHECKSUMS    = 1; unsupported version
    public static final int VERSION_CHECKPOINTS  = 2; // added checkpoints
    public static final int VERSION_PRIMARY_TERM = 3; // added primary term
    public static final int CURRENT_VERSION = VERSION_PRIMARY_TERM;

    public static final long UNKNOWN_PRIMARY_TERM = -1L;

    private final String translogUUID;
    private final long primaryTerm;
    private final int headerSizeInBytes;

    TranslogHeader(String translogUUID, long primaryTerm) {
        this(translogUUID, primaryTerm, defaultSizeInBytes(translogUUID));
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

    static int defaultSizeInBytes(String translogUUID) {
        return headerSize(CURRENT_VERSION, new BytesRef(translogUUID).length);
    }

    private static int headerSize(int version, int uuidLength) {
        int size = CodecUtil.headerLength(TRANSLOG_CODEC);
        size += Integer.BYTES + uuidLength; // uuid
        if (version >= VERSION_PRIMARY_TERM) {
            size += Long.BYTES;    // primary term
            size += Integer.BYTES; // checksum
        }
        return size;
    }

    /**
     * Read a translog header from the given path and file channel
     */
    static TranslogHeader read(final String translogUUID, final Path path, final FileChannel channel) throws IOException {
        // This input is intentionally not closed because closing it will close the FileChannel.
        final BufferedChecksumStreamInput in =
            new BufferedChecksumStreamInput(new InputStreamStreamInput(Channels.newInputStream(channel), channel.size()));
        final int version;
        try {
            version = CodecUtil.checkHeader(new InputStreamDataInput(in), TRANSLOG_CODEC, VERSION_CHECKPOINTS, VERSION_PRIMARY_TERM);
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException e) {
            throw new TranslogCorruptedException("Translog header corrupted. path:" + path, e);
        }
        // Read the translogUUID
        final int uuidLen = in.readInt();
        if (uuidLen > channel.size()) {
            throw new TranslogCorruptedException("uuid length can't be larger than the translog");
        }
        final BytesRef uuid = new BytesRef(uuidLen);
        uuid.length = uuidLen;
        in.read(uuid.bytes, uuid.offset, uuid.length);
        final BytesRef expectedUUID = new BytesRef(translogUUID);
        if (uuid.bytesEquals(expectedUUID) == false) {
            throw new TranslogCorruptedException("expected shard UUID " + expectedUUID + " but got: " + uuid +
                " this translog file belongs to a different translog. path:" + path);
        }
        // Read the primary term
        final long primaryTerm;
        if (version == VERSION_PRIMARY_TERM) {
            primaryTerm = in.readLong();
            assert primaryTerm >= 0 : "Primary term must be non-negative [" + primaryTerm + "]; translog path [" + path + "]";
        } else {
            assert version == VERSION_CHECKPOINTS : "Unknown header version [" + version + "]";
            primaryTerm = UNKNOWN_PRIMARY_TERM;
        }
        // Verify the checksum
        if (version >= VERSION_PRIMARY_TERM) {
            Translog.verifyChecksum(in);
        }
        final int headerSizeInBytes = headerSize(version, uuid.length);
        assert channel.position() == headerSizeInBytes :
            "Header is not fully read; header size [" + headerSizeInBytes + "], position [" + channel.position() + "]";
        return new TranslogHeader(translogUUID, primaryTerm, headerSizeInBytes);
    }

    /**
     * Writes this header with the latest format into the file channel
     */
    void write(FileChannel channel) throws IOException {
        // This output is intentionally not closed because closing it will close the FileChannel.
        final BufferedChecksumStreamOutput out = new BufferedChecksumStreamOutput(
            new OutputStreamStreamOutput(Channels.newOutputStream(channel)));
        CodecUtil.writeHeader(new OutputStreamDataOutput(out), TRANSLOG_CODEC, CURRENT_VERSION);
        // Write uuid
        final BytesRef uuid = new BytesRef(translogUUID);
        out.writeInt(uuid.length);
        out.writeBytes(uuid.bytes, uuid.offset, uuid.length);
        // Write primary term
        out.writeLong(primaryTerm);
        // Checksum header
        out.writeInt((int) out.getChecksum());
        channel.force(true);
        assert channel.position() == headerSizeInBytes :
            "Header is not fully written; header size [" + headerSizeInBytes + "], channel position [" + channel.position() + "]";
    }
}
