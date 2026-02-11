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
     * SeekableInputStream implementation that uses StorageObject's range-based reads.
     *
     * <p>This implementation provides efficient random access by:
     * <ul>
     *   <li>Tracking current position in the stream</li>
     *   <li>Using range reads for seek operations</li>
     *   <li>Buffering data from the current stream until a seek is needed</li>
     * </ul>
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
            // Open initial stream from beginning
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

            // If we're seeking within the current stream, try to skip forward
            if (newPos >= streamStartPosition && newPos >= position) {
                long skipAmount = newPos - position;
                if (skipAmount > 0) {
                    long skipped = currentStream.skip(skipAmount);
                    if (skipped != skipAmount) {
                        // Skip failed, need to reopen stream
                        reopenStreamAt(newPos);
                    } else {
                        position = newPos;
                    }
                }
                // If newPos == position, we're already there
                return;
            }

            // For backward seeks or large forward seeks, reopen the stream
            reopenStreamAt(newPos);
        }

        /**
         * Reopens the stream at the specified position using a range read.
         */
        private void reopenStreamAt(long newPos) throws IOException {
            // Close current stream
            if (currentStream != null) {
                currentStream.close();
            }

            // Open new stream from the target position to the end
            long remainingBytes = length - newPos;
            currentStream = storageObject.newStream(newPos, remainingBytes);
            streamStartPosition = newPos;
            position = newPos;
        }

        @Override
        public int read() throws IOException {
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
            int bytesRead = currentStream.read(b, off, len);
            if (bytesRead > 0) {
                position += bytesRead;
            }
            return bytesRead;
        }

        @Override
        public long skip(long n) throws IOException {
            long skipped = currentStream.skip(n);
            position += skipped;
            return skipped;
        }

        @Override
        public int available() throws IOException {
            return currentStream.available();
        }

        @Override
        public void close() throws IOException {
            if (currentStream != null) {
                currentStream.close();
                currentStream = null;
            }
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

        @Override
        public int read(java.nio.ByteBuffer buf) throws IOException {
            if (buf.hasRemaining() == false) {
                return 0;
            }

            int bytesToRead = buf.remaining();
            byte[] temp = new byte[bytesToRead];
            int bytesRead = read(temp, 0, bytesToRead);

            if (bytesRead > 0) {
                buf.put(temp, 0, bytesRead);
            }

            return bytesRead;
        }

        @Override
        public void readFully(java.nio.ByteBuffer buf) throws IOException {
            int remaining = buf.remaining();
            byte[] temp = new byte[remaining];
            readFully(temp, 0, remaining);
            buf.put(temp);
        }
    }
}
