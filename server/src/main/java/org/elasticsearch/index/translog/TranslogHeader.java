/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.translog;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.ByteUtils;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.zip.CRC32;

/**
 * Each translog file is started with a translog header then followed by translog operations.
 */
final class TranslogHeader {
    public static final String TRANSLOG_CODEC = "translog";

    public static final int VERSION_PRIMARY_TERM = 3; // with: checksums, checkpoints and primary term
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
            version = CodecUtil.checkHeader(new InputStreamDataInput(in), TRANSLOG_CODEC, VERSION_PRIMARY_TERM, VERSION_PRIMARY_TERM);
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException e) {
            throw new TranslogCorruptedException(path.toString(), "translog header corrupted", e);
        }
        return version;
    }

    /**
     * Read a translog header from the given path and file channel
     */
    static TranslogHeader read(final String translogUUID, final Path path, final FileChannel channel) throws IOException {
        try {
            // This input is intentionally not closed because closing it will close the FileChannel.
            final BufferedChecksumStreamInput in = new BufferedChecksumStreamInput(
                new InputStreamStreamInput(java.nio.channels.Channels.newInputStream(channel), channel.size()),
                path.toString()
            );
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
            assert channel.position() == headerSizeInBytes
                : "Header is not fully read; header size [" + headerSizeInBytes + "], position [" + channel.position() + "]";

            // verify UUID only after checksum, to ensure that UUID is not corrupted
            final BytesRef expectedUUID = new BytesRef(translogUUID);
            if (uuid.bytesEquals(expectedUUID) == false) {
                throw new TranslogCorruptedException(
                    path.toString(),
                    "expected shard UUID " + expectedUUID + " but got: " + uuid + " this translog file belongs to a different translog"
                );
            }

            return new TranslogHeader(translogUUID, primaryTerm, headerSizeInBytes);
        } catch (EOFException e) {
            throw new TranslogCorruptedException(path.toString(), "translog header truncated", e);
        }
    }

    private static final byte[] TRANSLOG_HEADER;

    static {
        var out = new ByteArrayOutputStream();
        try {
            CodecUtil.writeHeader(new OutputStreamDataOutput(out), TRANSLOG_CODEC, CURRENT_VERSION);
            TRANSLOG_HEADER = out.toByteArray();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Writes this header with the latest format into the file channel
     */
    void write(final FileChannel channel) throws IOException {
        final byte[] buffer = Arrays.copyOf(TRANSLOG_HEADER, headerSizeInBytes);
        // Write uuid and leave 4 bytes for its length
        final int uuidOffset = TRANSLOG_HEADER.length + Integer.BYTES;
        int offset = UnicodeUtil.UTF16toUTF8(translogUUID, 0, translogUUID.length(), buffer, uuidOffset);
        // write uuid length before uuid
        ByteUtils.writeIntBE(offset - uuidOffset, buffer, TRANSLOG_HEADER.length);
        // Write primary term
        ByteUtils.writeLongBE(primaryTerm, buffer, offset);
        offset += Long.BYTES;
        final CRC32 crc32 = new CRC32();
        crc32.update(buffer, 0, offset);
        // Checksum header
        ByteUtils.writeIntBE((int) crc32.getValue(), buffer, offset);
        Channels.writeToChannel(buffer, channel);
        channel.force(true);
        assert channel.position() == headerSizeInBytes
            : "Header is not fully written; header size [" + headerSizeInBytes + "], channel position [" + channel.position() + "]";
    }
}
