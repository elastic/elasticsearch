/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.util.IOUtils;

public class XNIOFSDirectory extends FSDirectory {

    /**
     * Create a new NIOFSDirectory for the named location. The directory is created at the named
     * location if it does not yet exist.
     *
     * @param path the path of the directory
     * @param lockFactory the lock factory to use
     * @throws IOException if there is a low-level I/O error
     */
    public XNIOFSDirectory(Path path, LockFactory lockFactory) throws IOException {
        super(path, lockFactory);
    }

    /**
     * Create a new NIOFSDirectory for the named location and {@link FSLockFactory#getDefault()}. The
     * directory is created at the named location if it does not yet exist.
     *
     * @param path the path of the directory
     * @throws IOException if there is a low-level I/O error
     */
    public XNIOFSDirectory(Path path) throws IOException {
        this(path, FSLockFactory.getDefault());
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        ensureOpen();
        ensureCanRead(name);
        Path path = getDirectory().resolve(name);
        FileChannel fc = FileChannel.open(path, StandardOpenOption.READ);
        boolean success = false;
        try {
            final NIOFSIndexInput indexInput =
                new NIOFSIndexInput("NIOFSIndexInput(path=\"" + path + "\")", fc, 4096);
            success = true;
            return indexInput;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(fc);
            }
        }
    }

    /** Reads bytes with {@link FileChannel#read(ByteBuffer, long)} */
    static final class NIOFSIndexInput extends BufferedIndexInput {
        /** The maximum chunk size for reads of 16384 bytes. */
        private static final int CHUNK_SIZE = 16384;

        /** the file channel we will read from */
        protected final FileChannel channel;

        /** is this instance a clone and hence does not own the file to close it */
        boolean isClone = false;

        /** start offset: non-zero in the slice case */
        protected final long off;

        /** end offset (start+length) */
        protected final long end;

        NIOFSIndexInput(String resourceDesc, FileChannel fc, int bufferSize)
            throws IOException {
            super(resourceDesc, bufferSize);
            this.channel = fc;
            this.off = 0L;
            this.end = fc.size();
        }

        NIOFSIndexInput(
            String resourceDesc, FileChannel fc, long off, long length, int bufferSize) {
            super(resourceDesc, bufferSize);
            this.channel = fc;
            this.off = off;
            this.end = off + length;
            this.isClone = true;
        }

        @Override
        public void close() throws IOException {
            if (isClone == false) {
                channel.close();
            }
        }

        @Override
        public NIOFSIndexInput clone() {
            NIOFSIndexInput clone = (NIOFSIndexInput) super.clone();
            clone.isClone = true;
            return clone;
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            if ((length | offset) < 0 || length > this.length() - offset) {
                throw new IllegalArgumentException(
                    "slice() "
                        + sliceDescription
                        + " out of bounds: offset="
                        + offset
                        + ",length="
                        + length
                        + ",fileLength="
                        + this.length()
                        + ": "
                        + this);
            }
            return new NIOFSIndexInput(
                getFullSliceDescription(sliceDescription),
                channel,
                off + offset,
                length,
                getBufferSize());
        }

        @Override
        public long length() {
            return end - off;
        }

        @Override
        protected void readInternal(ByteBuffer b) throws IOException {
            long pos = getFilePointer() + off;

            if (pos + b.remaining() > end) {
                throw new EOFException("read past EOF: " + this);
            }

            try {
                int readLength = b.remaining();
                while (readLength > 0) {
                    final int toRead = Math.min(CHUNK_SIZE, readLength);
                    b.limit(b.position() + toRead);
                    assert b.remaining() == toRead;
                    final int i = channel.read(b, pos);
                    if (i < 0) {
                        // be defensive here, even though we checked before hand, something could have changed
                        throw new EOFException(
                            "read past EOF: "
                                + this
                                + " buffer: "
                                + b
                                + " chunkLen: "
                                + toRead
                                + " end: "
                                + end);
                    }
                    assert i > 0
                        : "FileChannel.read with non zero-length bb.remaining() must always read at least "
                        + "one byte (FileChannel is in blocking mode, see spec of ReadableByteChannel)";
                    pos += i;
                    readLength -= i;
                }
                assert readLength == 0;
            } catch (IOException ioe) {
                throw new IOException(ioe.getMessage() + ": " + this, ioe);
            }
        }

        @Override
        protected void seekInternal(long pos) throws IOException {
            if (pos > length()) {
                throw new EOFException(
                    "read past EOF: pos=" + pos + " vs length=" + length() + ": " + this);
            }
        }
    }
}
