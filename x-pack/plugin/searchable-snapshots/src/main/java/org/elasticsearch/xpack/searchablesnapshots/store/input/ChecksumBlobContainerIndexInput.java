/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.store.input;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.index.store.Store;

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsUtils.toIntBytes;

/**
 * A {@link IndexInput} that can only be used to verify footer checksums.
 */
public class ChecksumBlobContainerIndexInput extends IndexInput {

    private final byte[] checksum;
    private final long length;

    private long position;

    private ChecksumBlobContainerIndexInput(String name, long length, byte[] checksum, IOContext context) {
        super("ChecksumBlobContainerIndexInput(" + name + ')');
        ensureReadOnceChecksumContext(context);
        assert checksum.length == CodecUtil.footerLength();
        this.checksum = Objects.requireNonNull(checksum);
        assert length >= this.checksum.length;
        this.length = length;
        this.position = 0L;
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public long getFilePointer() {
        return position;
    }

    @Override
    public byte readByte() throws IOException {
        if (getFilePointer() >= length()) {
            throw new EOFException("seek past EOF");
        }
        return checksum[checksumPositionOrThrow(position++)];
    }

    @Override
    public void readBytes(final byte[] b, final int off, int len) throws IOException {
        if (getFilePointer() + len > length()) {
            throw new EOFException("seek past EOF");
        }
        System.arraycopy(checksum, checksumPositionOrThrow(position), b, off, len);
        position += len;
    }

    @Override
    public void seek(long pos) throws IOException {
        if (pos < 0) {
            throw new IllegalArgumentException("Seeking to negative position: " + pos);
        } else if (pos > length()) {
            throw new EOFException("seek past EOF");
        }
        checksumPositionOrThrow(pos);
        position = pos;
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) {
        assert false : "unexpected slicing (" + sliceDescription + ") for " + this;
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexInput clone() {
        assert false : "unexpected cloning for " + this;
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {}

    @Override
    public String toString() {
        return "ChecksumBlobContainerIndexInput{"
            + "checksum="
            + Arrays.toString(checksum)
            + ", length="
            + length
            + ", position="
            + position
            + '}';
    }

    private int checksumPositionOrThrow(long pos) {
        final long checksumPosition = length - checksum.length;
        if (pos < checksumPosition) {
            assert false : "unexpected read or seek at position [" + pos + "] but checksum starts at [" + checksumPosition + ']';
            throw new IllegalArgumentException("Can't read or seek before footer checksum");
        }
        return toIntBytes(checksum.length - (length - pos));
    }

    private static void ensureReadOnceChecksumContext(IOContext context) {
        if (context != Store.READONCE_CHECKSUM) {
            assert false : "expected READONCE_CHECKSUM but got " + context;
            throw new IllegalArgumentException("ChecksumBlobContainerIndexInput should only be used with READONCE_CHECKSUM context");
        }
    }

    /**
     * Creates a {@link ChecksumBlobContainerIndexInput} that can be used to verify a Lucene file's footer checksum without opening the
     * file on disk. The checksum verification should be executed using {@link CodecUtil#retrieveChecksum(IndexInput)}.
     *
     * @param name     the physical name of the file
     * @param length   the total length of the file
     * @param checksum the footer checksum provided as a {@link String}
     * @return a {@link ChecksumBlobContainerIndexInput}
     * @throws IOException if something goes wrong when creating the {@link ChecksumBlobContainerIndexInput}
     */
    public static ChecksumBlobContainerIndexInput create(String name, long length, String checksum, IOContext context) throws IOException {
        return new ChecksumBlobContainerIndexInput(name, length, checksumToBytesArray(checksum), context);
    }

    public static byte[] checksumToBytesArray(String checksum) throws IOException {
        final byte[] result = new byte[CodecUtil.footerLength()];
        assert result.length >= Integer.BYTES + Integer.BYTES + Long.BYTES; // ensure that nobody changed the file format under us
        final ByteArrayDataOutput output = new ByteArrayDataOutput(result);
        // reverse CodecUtil.writeFooter()
        CodecUtil.writeBEInt(output, CodecUtil.FOOTER_MAGIC);
        CodecUtil.writeBEInt(output, 0);
        CodecUtil.writeBELong(output, Long.parseLong(checksum, Character.MAX_RADIX));
        assert output.getPosition() == result.length;
        return result;
    }
}
