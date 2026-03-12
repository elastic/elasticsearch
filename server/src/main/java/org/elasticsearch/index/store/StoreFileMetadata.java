/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.text.ParseException;
import java.util.Objects;

public class StoreFileMetadata implements Writeable {

    public static final BytesRef UNAVAILABLE_WRITER_UUID = new BytesRef();

    private final String name;

    // the actual file size on "disk", if compressed, the compressed size
    private final long length;

    private final String checksum;

    private final String writtenBy;

    private final BytesRef hash;

    private final BytesRef writerUuid;

    public StoreFileMetadata(String name, long length, String checksum, String writtenBy) {
        this(name, length, checksum, writtenBy, null, UNAVAILABLE_WRITER_UUID);
    }

    public StoreFileMetadata(String name, long length, String checksum, String writtenBy, BytesRef hash, BytesRef writerUuid) {
        assert assertValidWrittenBy(writtenBy);
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.length = length;
        this.checksum = Objects.requireNonNull(checksum, "checksum must not be null");
        this.writtenBy = Objects.requireNonNull(writtenBy, "writtenBy must not be null");
        this.hash = hash == null ? new BytesRef() : hash;

        assert writerUuid != null && (writerUuid.length > 0 || writerUuid == UNAVAILABLE_WRITER_UUID);
        this.writerUuid = Objects.requireNonNull(writerUuid, "writerUuid must not be null");
    }

    /**
     * Read from a stream.
     */
    public StoreFileMetadata(StreamInput in) throws IOException {
        name = in.readString();
        length = in.readVLong();
        checksum = in.readString();
        writtenBy = in.readString();
        hash = in.readBytesRef();
        writerUuid = StoreFileMetadata.toWriterUuid(in.readBytesRef());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeVLong(length);
        out.writeString(checksum);
        out.writeString(writtenBy);
        out.writeBytesRef(hash);
        out.writeBytesRef(writerUuid);
    }

    /**
     * Returns the name of this file
     */
    public String name() {
        return name;
    }

    /**
     * the actual file size on "disk", if compressed, the compressed size
     */
    public long length() {
        return length;
    }

    /**
     * Returns a string representation of the files checksum. Since Lucene 4.8 this is a CRC32 checksum written
     * by lucene.
     */
    public String checksum() {
        return this.checksum;
    }

    /**
     * Checks if the bytes returned by {@link #hash()} are the contents of the file that this instances refers to.
     *
     * @return {@code true} iff {@link #hash()} will return the actual file contents
     */
    public boolean hashEqualsContents() {
        if (hash.length == length) {
            try {
                final boolean checksumsMatch = Store.digestToString(
                    CodecUtil.retrieveChecksum(new ByteArrayIndexInput("store_file", hash.bytes, hash.offset, hash.length))
                ).equals(checksum);
                assert checksumsMatch : "Checksums did not match for [" + this + "] which has a hash of [" + hash + "]";
                return checksumsMatch;
            } catch (Exception e) {
                // Hash didn't contain any bytes that Lucene could extract a checksum from so we can't verify against the checksum of the
                // original file. We should never see an exception here because lucene files are assumed to always contain the checksum
                // footer.
                assert false : new AssertionError("Saw exception for hash [" + hash + "] but expected it to be Lucene file", e);
                return false;
            }
        }
        return false;
    }

    /**
     * Returns <code>true</code> iff the length and the checksums are the same. otherwise <code>false</code>
     */
    public boolean isSame(StoreFileMetadata other) {
        if (checksum == null || other.checksum == null) {
            // we can't tell if either or is null so we return false in this case! this is why we don't use equals for this!
            return false;
        }

        // If we have the file contents, we directly compare the contents. This is useful to compare segment info
        // files of source-only snapshots where the original segment info file shares the same id as the source-only
        // segment info file but its contents are different.
        if (hashEqualsContents()) {
            return hash.equals(other.hash);
        }

        if (writerUuid.length > 0 && other.writerUuid.length > 0) {
            // if the writer ID is missing on one of the files then we ignore this field and just rely on the checksum and hash, but if
            // it's present on both files then it must be identical
            if (writerUuid.equals(other.writerUuid) == false) {
                return false;
            } else {
                assert name.equals(other.name) && length == other.length && checksum.equals(other.checksum) : this + " vs " + other;
                assert hash.equals(other.hash) : this + " vs " + other + " with hashes " + hash + " vs " + other.hash;
            }
        }
        return length == other.length && checksum.equals(other.checksum) && hash.equals(other.hash);
    }

    @Override
    public String toString() {
        return "name [" + name + "], length [" + length + "], checksum [" + checksum + "], writtenBy [" + writtenBy + "]";
    }

    /**
     * Returns a String representation of the Lucene version this file has been written by or <code>null</code> if unknown
     */
    public String writtenBy() {
        return writtenBy;
    }

    /**
     * Returns a variable length hash of the file represented by this metadata object. This can be the file
     * itself if the file is small enough. If the length of the hash is {@code 0} no hash value is available
     */
    public BytesRef hash() {
        return hash;
    }

    /**
     * Returns the globally-unique ID that was assigned by the {@link IndexWriter} that originally wrote this file:
     *
     * - For `segments_N` files this is {@link SegmentInfos#getId()} which uniquely identifies the commit.
     * - For non-generational segment files this is {@link SegmentInfo#getId()} which uniquely identifies the segment.
     * - For generational segment files (i.e. updated docvalues, liv files etc) this is {@link SegmentCommitInfo#getId()}
     *     which uniquely identifies the generation of the segment.
     *
     * This ID may be {@link StoreFileMetadata#UNAVAILABLE_WRITER_UUID} (i.e. zero-length) if unavailable, e.g.:
     *
     * - The file was written by a version of Lucene prior to 8.6.0.
     * - The file is not one of the files listed above.
     *
     */
    public BytesRef writerUuid() {
        return writerUuid;
    }

    static BytesRef toWriterUuid(BytesRef bytesRef) {
        if (bytesRef.length == 0) {
            return UNAVAILABLE_WRITER_UUID;
        } else {
            return bytesRef;
        }
    }

    static BytesRef toWriterUuid(@Nullable byte[] id) {
        if (id == null) {
            return UNAVAILABLE_WRITER_UUID;
        } else {
            assert id.length > 0;
            return new BytesRef(id);
        }
    }

    private static boolean assertValidWrittenBy(String writtenBy) {
        try {
            Version.parse(writtenBy);
        } catch (ParseException e) {
            throw new AssertionError("invalid writtenBy: " + writtenBy, e);
        }
        return true;
    }
}
