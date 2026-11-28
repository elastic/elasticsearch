/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.file;

import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.xpack.parquet.write.table.RepositoryFileIO;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * A blob of a repository as the input of the Parquet reader, with ranged reads, so that a compaction reads the uploaded data files
 * without a local copy of them. A data file can be as large as {@code max_file_size}, so it is never read as a whole.
 *
 * <p>The stream asks for a window of the blob and reads on in it while the reads are contiguous. A seek that leaves the window
 * (the footer at the end, then the first row group) opens a new one. The Parquet reader reads the column chunks of a row group in one
 * run of contiguous bytes, so most reads stay in a window.
 */
public final class BlobInputFile implements InputFile {

    /** The size of the window that one ranged read asks for. */
    static final long WINDOW_BYTES = 8L * 1024 * 1024;

    private final BlobContainer container;
    private final String blobName;
    private final long length;

    /**
     * @param length the size of the blob; the table knows it from the manifest, so that the reader needs no request for it
     */
    public BlobInputFile(BlobContainer container, String blobName, long length) {
        this.container = container;
        this.blobName = blobName;
        this.length = length;
    }

    @Override
    public long getLength() {
        return length;
    }

    @Override
    public SeekableInputStream newStream() {
        return new RangedStream();
    }

    private final class RangedStream extends SeekableInputStream {
        private long position;
        private InputStream window;
        /** The position that the next byte of {@code window} has. */
        private long windowPosition;

        @Override
        public long getPos() {
            return position;
        }

        @Override
        public void seek(long newPos) throws IOException {
            if (newPos < 0 || newPos > length) {
                throw new EOFException("cannot seek to [" + newPos + "] in [" + blobName + "] of [" + length + "] bytes");
            }
            position = newPos;
        }

        /** Makes {@code window} start at {@code position}; false at the end of the blob. */
        private boolean ensureWindow() throws IOException {
            if (position >= length) {
                return false;
            }
            if (window != null && windowPosition == position) {
                return true;
            }
            closeWindow();
            final long size = Math.min(WINDOW_BYTES, length - position);
            window = container.readBlob(RepositoryFileIO.PURPOSE, blobName, position, size);
            windowPosition = position;
            return true;
        }

        private void closeWindow() throws IOException {
            if (window != null) {
                window.close();
                window = null;
            }
        }

        @Override
        public int read() throws IOException {
            final byte[] one = new byte[1];
            return read(one, 0, 1) == -1 ? -1 : one[0] & 0xFF;
        }

        @Override
        public int read(byte[] destination, int offset, int count) throws IOException {
            if (count == 0) {
                return 0;
            }
            if (ensureWindow() == false) {
                return -1;
            }
            final int read = window.read(destination, offset, count);
            if (read == -1) {
                // the window ended: the next read opens the next one
                closeWindow();
                return read(destination, offset, count);
            }
            position += read;
            windowPosition += read;
            return read;
        }

        @Override
        public void readFully(byte[] destination) throws IOException {
            readFully(destination, 0, destination.length);
        }

        @Override
        public void readFully(byte[] destination, int start, int count) throws IOException {
            int done = 0;
            while (done < count) {
                final int read = read(destination, start + done, count - done);
                if (read == -1) {
                    throw new EOFException("cannot read [" + count + "] bytes at [" + (position - done) + "] of [" + blobName + "]");
                }
                done += read;
            }
        }

        @Override
        public int read(ByteBuffer destination) throws IOException {
            final byte[] copy = new byte[destination.remaining()];
            final int read = read(copy, 0, copy.length);
            if (read > 0) {
                destination.put(copy, 0, read);
            }
            return read;
        }

        @Override
        public void readFully(ByteBuffer destination) throws IOException {
            final byte[] copy = new byte[destination.remaining()];
            readFully(copy, 0, copy.length);
            destination.put(copy);
        }

        @Override
        public void close() throws IOException {
            closeWindow();
        }
    }
}
