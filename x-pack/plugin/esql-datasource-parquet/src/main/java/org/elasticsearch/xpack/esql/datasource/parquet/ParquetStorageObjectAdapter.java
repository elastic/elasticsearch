/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.io.SeekableInputStream;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.IOException;
import java.io.InputStream;

/**
 * Adapter that wraps a StorageObject to implement Parquet's InputFile interface.
 * This allows using our storage abstraction with Parquet's ParquetFileReader.
 *
 * <p>Key features:
 * <ul>
 *   <li>Converts StorageObject's range-based reads to Parquet's seekable stream interface</li>
 *   <li>Supports efficient random access for columnar format reading</li>
 *   <li>No Hadoop dependencies - uses pure Java InputStream</li>
 * </ul>
 */
public class ParquetStorageObjectAdapter implements org.apache.parquet.io.InputFile {
    private final StorageObject storageObject;

    /**
     * Creates an adapter for the given StorageObject.
     *
     * @param storageObject the storage object to adapt
     */
    public ParquetStorageObjectAdapter(StorageObject storageObject) {
        if (storageObject == null) {
            throw new IllegalArgumentException("storageObject cannot be null");
        }
        this.storageObject = storageObject;
    }

    @Override
    public long getLength() throws IOException {
        return storageObject.length();
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
        return new StorageObjectSeekableInputStream(storageObject);
    }

    /**
     * SeekableInputStream implementation that combines two I/O strategies:
     * <ul>
     *   <li><b>Byte-array reads</b> ({@code read()}, {@code read(byte[])}, {@code readFully(byte[])})
     *       use a cached InputStream for efficient sequential access.</li>
     *   <li><b>ByteBuffer reads</b> ({@code read(ByteBuffer)}, {@code readFully(ByteBuffer)})
     *       use {@link StorageObject#readBytes(long, java.nio.ByteBuffer)} for provider-native
     *       I/O that avoids temporary allocations and data duplication.</li>
     * </ul>
     * After a ByteBuffer read, the cached InputStream is invalidated since the position has
     * advanced independently. It is lazily re-opened on the next byte-array read.
     */
    private static class StorageObjectSeekableInputStream extends SeekableInputStream {
        private final StorageObject storageObject;
        private InputStream currentStream;
        private long position;
        private long streamStartPosition;
        private final long length;

        StorageObjectSeekableInputStream(StorageObject storageObject) throws IOException {
            this.storageObject = storageObject;
            this.length = storageObject.length();
            this.position = 0;
            this.streamStartPosition = 0;
            this.currentStream = storageObject.newStream();
        }

        @Override
        public long getPos() throws IOException {
            return position;
        }

        @Override
        public void seek(long newPos) throws IOException {
            if (newPos < 0) {
                throw new IOException("Cannot seek to negative position: " + newPos);
            }
            if (newPos > length) {
                throw new IOException("Cannot seek beyond end of file: " + newPos + " > " + length);
            }

            if (currentStream != null && newPos >= streamStartPosition && newPos >= position) {
                long skipAmount = newPos - position;
                if (skipAmount > 0) {
                    long skipped = currentStream.skip(skipAmount);
                    if (skipped != skipAmount) {
                        reopenStreamAt(newPos);
                    } else {
                        position = newPos;
                    }
                }
                return;
            }

            // For backward seeks, large forward seeks, or when stream was invalidated
            if (newPos != position) {
                reopenStreamAt(newPos);
            }
        }

        private void reopenStreamAt(long newPos) throws IOException {
            invalidateStream();
            long remainingBytes = length - newPos;
            currentStream = storageObject.newStream(newPos, remainingBytes);
            streamStartPosition = newPos;
            position = newPos;
        }

        /**
         * Ensures a valid InputStream is available at the current position.
         * Re-opens the stream if it was invalidated by a ByteBuffer read.
         */
        private void ensureStream() throws IOException {
            if (currentStream == null) {
                long remainingBytes = length - position;
                currentStream = storageObject.newStream(position, remainingBytes);
                streamStartPosition = position;
            }
        }

        /**
         * Closes the current InputStream. Called before ByteBuffer reads that bypass the
         * stream, since those advance the position independently.
         */
        private void invalidateStream() throws IOException {
            if (currentStream != null) {
                currentStream.close();
                currentStream = null;
            }
        }

        @Override
        public int read() throws IOException {
            ensureStream();
            int b = currentStream.read();
            if (b >= 0) {
                position++;
            }
            return b;
        }

        @Override
        public int read(byte[] b) throws IOException {
            return read(b, 0, b.length);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            ensureStream();
            int bytesRead = currentStream.read(b, off, len);
            if (bytesRead > 0) {
                position += bytesRead;
            }
            return bytesRead;
        }

        @Override
        public long skip(long n) throws IOException {
            ensureStream();
            long skipped = currentStream.skip(n);
            position += skipped;
            return skipped;
        }

        @Override
        public int available() throws IOException {
            if (currentStream == null) {
                return (int) Math.min(length - position, Integer.MAX_VALUE);
            }
            return currentStream.available();
        }

        @Override
        public void close() throws IOException {
            invalidateStream();
        }

        @Override
        public void readFully(byte[] bytes) throws IOException {
            readFully(bytes, 0, bytes.length);
        }

        @Override
        public void readFully(byte[] bytes, int start, int len) throws IOException {
            int offset = start;
            int remaining = len;
            while (remaining > 0) {
                int bytesRead = read(bytes, offset, remaining);
                if (bytesRead < 0) {
                    throw new IOException("Reached end of stream before reading " + len + " bytes");
                }
                offset += bytesRead;
                remaining -= bytesRead;
            }
        }

        /**
         * Reads into a ByteBuffer using {@link StorageObject#readBytes(long, java.nio.ByteBuffer)}.
         * For local files this results in zero-copy FileChannel I/O; for GCS it uses native
         * ReadChannel I/O. For other providers the default avoids allocating a full-size temporary
         * array by using a small chunked transfer buffer for direct ByteBuffers, or reading
         * directly into the backing array for heap ByteBuffers.
         */
        @Override
        public int read(java.nio.ByteBuffer buf) throws IOException {
            if (buf.hasRemaining() == false) {
                return 0;
            }
            invalidateStream();
            int bytesRead = storageObject.readBytes(position, buf);
            if (bytesRead > 0) {
                position += bytesRead;
            }
            return bytesRead;
        }

        @Override
        public void readFully(java.nio.ByteBuffer buf) throws IOException {
            invalidateStream();
            while (buf.hasRemaining()) {
                int bytesRead = storageObject.readBytes(position, buf);
                if (bytesRead <= 0) {
                    throw new IOException("Reached end of stream before reading all bytes at position " + position);
                }
                position += bytesRead;
            }
        }
    }
}
