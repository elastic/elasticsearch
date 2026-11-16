/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill;

import org.elasticsearch.common.util.ByteUtils;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.zip.CRC32;

/**
 * The byte layout every run the engine writes shares, and the arithmetic between a position in the run's logical
 * stream and a position in the stored object.
 * <p>
 * An object is a fixed header, a body of chunks, an index and a fixed footer. The body is the logical stream cut into
 * chunks of {@code chunkSize} payload bytes, every chunk but the last full, each followed by an eight-byte trailer of
 * payload length and CRC32, so a corrupt chunk is caught before anything in it is decoded and a logical offset maps to
 * a physical one by arithmetic alone. The logical stream is a sequence of records, each a partition tag followed by a
 * payload the index locates by offset and length; records span chunks freely. The index, written once at finish and
 * grouped by partition, is a directory of partitions followed by their entries, each entry the payload's logical
 * offset and length, with a CRC of its own. The footer names the counts and the index's position and carries its own
 * CRC, so a run with a missing or torn footer is seen as truncated before any record is read.
 * <p>
 * Every offset is 64 bits; the format version is the layout's own and is separate from the transport version the
 * payloads were encoded with, which the header pins for readers.
 */
public final class FramedRunLayout {

    public static final byte[] HEADER_MAGIC = "ESQLSPIL".getBytes(StandardCharsets.US_ASCII);
    public static final byte[] FOOTER_MAGIC = "ESQLSPLE".getBytes(StandardCharsets.US_ASCII);
    public static final int FORMAT_VERSION = 1;

    /** magic 8 | format version 4 | kind 4 | transport version 4 | chunk size 4 | reserved 8 */
    public static final int HEADER_BYTES = 32;
    /** payload length 4 | crc32 4 */
    public static final int CHUNK_TRAILER_BYTES = 8;
    /** records 8 | logical bytes 8 | index offset 8 | partitions 4 | crc32 4 | magic 8 */
    public static final int FOOTER_BYTES = 40;
    /** first entry 8 | entry count 8 */
    public static final int DIRECTORY_ENTRY_BYTES = 16;
    /** partition 4 | offset 8 | length 4 */
    public static final int INDEX_ENTRY_BYTES = 16;
    public static final int INDEX_CRC_BYTES = 4;

    public static final int KIND_PAGE = 1;
    public static final int KIND_VALUE = 2;
    public static final int KIND_SLICE = 3;

    private FramedRunLayout() {}

    public record Header(int formatVersion, int kind, int transportVersionId, int chunkSize) {

        public byte[] encode() {
            byte[] bytes = new byte[HEADER_BYTES];
            System.arraycopy(HEADER_MAGIC, 0, bytes, 0, 8);
            ByteUtils.writeIntLE(formatVersion, bytes, 8);
            ByteUtils.writeIntLE(kind, bytes, 12);
            ByteUtils.writeIntLE(transportVersionId, bytes, 16);
            ByteUtils.writeIntLE(chunkSize, bytes, 20);
            return bytes;
        }

        /**
         * @throws FramedRunCorruptionException when the magic is wrong or the version is not ours
         */
        public static Header decode(byte[] bytes, int offset, String run) throws FramedRunCorruptionException {
            if (bytes.length - offset < HEADER_BYTES) {
                throw new FramedRunCorruptionException(
                    run,
                    "header is [" + (bytes.length - offset) + "] bytes, expected [" + HEADER_BYTES + "]"
                );
            }
            if (Arrays.equals(bytes, offset, offset + 8, HEADER_MAGIC, 0, 8) == false) {
                throw new FramedRunCorruptionException(run, "not a spill run: bad header magic");
            }
            int formatVersion = ByteUtils.readIntLE(bytes, offset + 8);
            if (formatVersion != FORMAT_VERSION) {
                throw new FramedRunCorruptionException(run, "format version [" + formatVersion + "] is not [" + FORMAT_VERSION + "]");
            }
            int kind = ByteUtils.readIntLE(bytes, offset + 12);
            int transportVersionId = ByteUtils.readIntLE(bytes, offset + 16);
            int chunkSize = ByteUtils.readIntLE(bytes, offset + 20);
            if (chunkSize <= 0) {
                throw new FramedRunCorruptionException(run, "chunk size [" + chunkSize + "] is not positive");
            }
            return new Header(formatVersion, kind, transportVersionId, chunkSize);
        }
    }

    public record Footer(long records, long logicalBytes, long indexOffset, int partitions) {

        public byte[] encode() {
            byte[] bytes = new byte[FOOTER_BYTES];
            ByteUtils.writeLongLE(records, bytes, 0);
            ByteUtils.writeLongLE(logicalBytes, bytes, 8);
            ByteUtils.writeLongLE(indexOffset, bytes, 16);
            ByteUtils.writeIntLE(partitions, bytes, 24);
            ByteUtils.writeIntLE(crc32(bytes, 0, 28), bytes, 28);
            System.arraycopy(FOOTER_MAGIC, 0, bytes, 32, 8);
            return bytes;
        }

        /**
         * @throws FramedRunCorruptionException when the magic or the CRC is wrong, which is how a truncated run is seen
         */
        public static Footer decode(byte[] bytes, int offset, String run) throws FramedRunCorruptionException {
            if (bytes.length - offset < FOOTER_BYTES) {
                throw new FramedRunCorruptionException(
                    run,
                    "footer is [" + (bytes.length - offset) + "] bytes, expected [" + FOOTER_BYTES + "]"
                );
            }
            if (Arrays.equals(bytes, offset + 32, offset + 40, FOOTER_MAGIC, 0, 8) == false) {
                throw new FramedRunCorruptionException(run, "truncated: bad footer magic");
            }
            int expected = ByteUtils.readIntLE(bytes, offset + 28);
            int actual = crc32(bytes, offset, 28);
            if (expected != actual) {
                throw new FramedRunCorruptionException(run, "footer checksum mismatch");
            }
            long records = ByteUtils.readLongLE(bytes, offset);
            long logicalBytes = ByteUtils.readLongLE(bytes, offset + 8);
            long indexOffset = ByteUtils.readLongLE(bytes, offset + 16);
            int partitions = ByteUtils.readIntLE(bytes, offset + 24);
            if (records < 0 || logicalBytes < 0 || indexOffset < HEADER_BYTES || partitions < 0) {
                throw new FramedRunCorruptionException(run, "footer counts out of range");
            }
            return new Footer(records, logicalBytes, indexOffset, partitions);
        }
    }

    public static int crc32(byte[] bytes, int offset, int length) {
        CRC32 crc = new CRC32();
        crc.update(bytes, offset, length);
        return (int) crc.getValue();
    }

    /**
     * The physical offset in the object of the logical byte {@code logicalOffset} of the body.
     */
    public static long physicalOffset(long logicalOffset, int chunkSize) {
        long chunk = logicalOffset / chunkSize;
        return HEADER_BYTES + chunk * (chunkSize + CHUNK_TRAILER_BYTES) + logicalOffset % chunkSize;
    }

    /**
     * The index of the chunk holding the logical byte.
     */
    public static long chunkOf(long logicalOffset, int chunkSize) {
        return logicalOffset / chunkSize;
    }

    /**
     * The physical offset where chunk {@code chunk} starts.
     */
    public static long chunkStart(long chunk, int chunkSize) {
        return HEADER_BYTES + chunk * (chunkSize + CHUNK_TRAILER_BYTES);
    }

    /**
     * The physical length of the body for a logical length, trailers included: every chunk but the last is full.
     */
    public static long physicalBodyLength(long logicalBytes, int chunkSize) {
        if (logicalBytes == 0) {
            return 0;
        }
        long chunks = (logicalBytes + chunkSize - 1) / chunkSize;
        return logicalBytes + chunks * CHUNK_TRAILER_BYTES;
    }
}
