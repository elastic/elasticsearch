/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.index.store.checksum;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.index.store.Store;

import java.io.EOFException;
import java.io.IOException;
import java.util.Objects;

/**
 * A {@link IndexInput} that can only be used to verify footer checksums.
 */
public class ChecksumBlobContainerIndexInput extends IndexInput {

    private final byte[] checksum;
    private final long length;
    private final long offset;

    private int position;

    private ChecksumBlobContainerIndexInput(String name, long length, byte[] checksum, IOContext context) {
        super("ChecksumBlobContainerIndexInput(" + name + ')');
        ensureReadOnceChecksumContext(context);
        assert checksum.length == CodecUtil.footerLength();
        this.checksum = Objects.requireNonNull(checksum);
        this.length = length;
        this.offset = length - this.checksum.length;
        assert offset > 0;
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public long getFilePointer() {
        return offset + position;
    }

    @Override
    public byte readByte() throws IOException {
        if (getFilePointer() >= length()) {
            throw new EOFException("seek past EOF");
        }
        return checksum[position++];
    }

    @Override
    public void readBytes(final byte[] b, final int off, int len) throws IOException {
        if (getFilePointer() + len > length()) {
            throw new EOFException("seek past EOF");
        }
        System.arraycopy(checksum, position, b, off, len);
        position += len;
    }

    @Override
    public void seek(long pos) throws IOException {
        if (pos < 0) {
            throw new IllegalArgumentException("Seeking to negative position: " + pos);
        } else if (pos > length()) {
            throw new EOFException("seek past EOF");
        } else if (pos < offset) {
            throw new EOFException("Can't read before footer checksum");
        }
        position = Math.toIntExact(pos - offset);
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexInput clone() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {}

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
        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        try (IndexOutput output = new ByteBuffersIndexOutput(out, "tmp", name)) {
            // reverse CodecUtil.writeFooter()
            output.writeInt(CodecUtil.FOOTER_MAGIC);
            output.writeInt(0);
            output.writeLong(Long.parseLong(checksum, Character.MAX_RADIX));
            output.close();
            return new ChecksumBlobContainerIndexInput(name, length, out.toArrayCopy(), context);
        }
    }
}
