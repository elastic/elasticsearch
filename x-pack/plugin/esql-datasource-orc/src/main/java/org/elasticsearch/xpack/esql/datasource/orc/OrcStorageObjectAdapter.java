/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.datasources.cache.FooterByteCache;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.concurrent.ExecutionException;

/**
 * Adapter that provides a Hadoop {@link FileSystem} backed by a {@link StorageObject}.
 *
 * <p>ORC's reader API requires a Hadoop {@link FileSystem} and {@link Path}. This adapter
 * wraps our storage abstraction to satisfy that contract, enabling ORC file reading from
 * any storage provider (HTTP, S3, local) without a real Hadoop installation.
 */
public class OrcStorageObjectAdapter extends FileSystem {
    private static final Logger logger = LogManager.getLogger(OrcStorageObjectAdapter.class);

    private final StorageObject storageObject;
    private final Path path;
    private final FooterByteCache.Key cacheKey;

    /**
     * Creates an adapter for the given StorageObject.
     *
     * @param storageObject the storage object to adapt
     */
    @SuppressWarnings("this-escape")
    public OrcStorageObjectAdapter(StorageObject storageObject) {
        if (storageObject == null) {
            throw new QlIllegalArgumentException("storageObject cannot be null");
        }
        this.storageObject = storageObject;
        this.path = new Path(storageObject.path().toString());
        try {
            this.cacheKey = FooterByteCache.Key.keyFor(storageObject, storageObject.length());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read storage object length for [" + storageObject.path() + "]", e);
        }
        setConf(new Configuration(false));
    }

    /**
     * Returns the cache key identifying this file by {@code (path, length)}. Shared with
     * {@link FooterByteCache} and the parsed-footer cache held by {@link OrcFormatReader} so that
     * callers reusing this adapter can hit those caches without recomputing the key.
     */
    FooterByteCache.Key cacheKey() {
        return cacheKey;
    }

    @Override
    public URI getUri() {
        return path.toUri();
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        return new FSDataInputStream(new StorageObjectInputStream(storageObject));
    }

    @Override
    public FSDataInputStream open(Path f) throws IOException {
        return open(f, 4096);
    }

    static void clearCacheForTests() {
        FooterByteCache.getInstance().invalidateAll();
        OrcFormatReader.clearParsedFooterCacheForTests();
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        long length = storageObject.length();
        return new FileStatus(length, false, 1, length, 0, path);
    }

    @Override
    public Path getWorkingDirectory() {
        return path.getParent();
    }

    // --- Unsupported write operations ---

    @Override
    public org.apache.hadoop.fs.FSDataOutputStream create(
        Path f,
        FsPermission permission,
        boolean overwrite,
        int bufferSize,
        short replication,
        long blockSize,
        Progressable progress
    ) throws IOException {
        throw new UnsupportedOperationException("Read-only filesystem");
    }

    @Override
    public org.apache.hadoop.fs.FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        throw new UnsupportedOperationException("Read-only filesystem");
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        throw new UnsupportedOperationException("Read-only filesystem");
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        throw new UnsupportedOperationException("Read-only filesystem");
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        throw new UnsupportedOperationException("Read-only filesystem");
    }

    @Override
    public void setWorkingDirectory(Path newDir) {
        // no-op
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        throw new UnsupportedOperationException("Read-only filesystem");
    }

    /**
     * InputStream implementation that bridges {@link StorageObject} to Hadoop's
     * {@link Seekable} and {@link PositionedReadable} interfaces required by
     * {@link FSDataInputStream}.
     */
    static class StorageObjectInputStream extends InputStream implements Seekable, PositionedReadable {
        private final StorageObject storageObject;
        private final FooterByteCache.Key cacheKey;
        private InputStream currentStream;
        private long position;
        private long streamStartPosition;
        private final long length;

        StorageObjectInputStream(StorageObject storageObject) throws IOException {
            this.storageObject = storageObject;
            this.length = storageObject.length();
            this.cacheKey = FooterByteCache.Key.keyFor(storageObject, this.length);
            this.position = 0;
            this.streamStartPosition = 0;
            this.currentStream = storageObject.newStream();
        }

        // --- Seekable ---

        @Override
        public void seek(long pos) throws IOException {
            if (pos < 0) {
                throw new IOException("Cannot seek to negative position: " + pos);
            }
            if (pos > length) {
                throw new IOException("Cannot seek beyond end of file: " + pos + " > " + length);
            }

            if (pos >= streamStartPosition && pos >= position) {
                long skipAmount = pos - position;
                if (skipAmount > 0) {
                    long skipped = currentStream.skip(skipAmount);
                    if (skipped != skipAmount) {
                        reopenStreamAt(pos);
                    } else {
                        position = pos;
                    }
                }
                return;
            }

            reopenStreamAt(pos);
        }

        @Override
        public long getPos() throws IOException {
            return position;
        }

        @Override
        public boolean seekToNewSource(long targetPos) throws IOException {
            return false;
        }

        // --- PositionedReadable ---

        @Override
        public int read(long pos, byte[] buffer, int offset, int len) throws IOException {
            int fromCache = readFromTailCache(pos, buffer, offset, len);
            if (fromCache >= 0) {
                return fromCache;
            }

            int bytesRead;
            try (InputStream stream = storageObject.newStream(pos, len)) {
                bytesRead = stream.read(buffer, offset, len);
            }

            // Opportunistic cache fill: uses pos + bytesRead (actual bytes returned) rather than
            // pos + len (requested bytes) because PositionedReadable.read may return a short read.
            // This caches a smaller suffix [length - bytesRead, length) which readFromTailCache
            // handles correctly — its bounds check rejects requests that span outside the cached region.
            if (bytesRead > 0 && pos + bytesRead == length) {
                byte[] tailBytes = new byte[bytesRead];
                System.arraycopy(buffer, offset, tailBytes, 0, bytesRead);
                FooterByteCache.getInstance().put(cacheKey, tailBytes);
            }
            return bytesRead;
        }

        @Override
        public void readFully(long pos, byte[] buffer, int offset, int len) throws IOException {
            FooterByteCache tailCache = FooterByteCache.getInstance();
            boolean isTailRead = pos + len == length;

            if (isTailRead && len > 0 && len <= tailCache.maxEntryBytes()) {
                try {
                    byte[] tailBytes = tailCache.getOrLoad(cacheKey, k -> {
                        byte[] loaded = new byte[len];
                        readFullyFromStorage(pos, loaded, 0, len);
                        return loaded;
                    });
                    if (tailBytes.length > 0) {
                        long cachedStart = length - tailBytes.length;
                        if (pos >= cachedStart && pos + len <= length) {
                            int from = (int) (pos - cachedStart);
                            System.arraycopy(tailBytes, from, buffer, offset, len);
                            return;
                        }
                    }
                } catch (ExecutionException e) {
                    logger.debug("footer cache load failed; retrying direct I/O", e.getCause());
                }
            }

            int fromCache = readFromTailCache(pos, buffer, offset, len);
            if (fromCache == len) {
                return;
            }

            readFullyFromStorage(pos, buffer, offset, len);

            if (pos + len == length) {
                byte[] tailBytes = new byte[len];
                System.arraycopy(buffer, offset, tailBytes, 0, len);
                tailCache.put(cacheKey, tailBytes);
            }
        }

        @Override
        public void readFully(long pos, byte[] buffer) throws IOException {
            readFully(pos, buffer, 0, buffer.length);
        }

        private int readFromTailCache(long pos, byte[] buffer, int offset, int len) {
            byte[] cached = FooterByteCache.getInstance().get(cacheKey);
            if (cached == null || cached.length == 0) {
                return -1;
            }
            long cachedStart = length - cached.length;
            if (pos >= cachedStart && pos + len <= length) {
                int from = (int) (pos - cachedStart);
                System.arraycopy(cached, from, buffer, offset, len);
                return len;
            }
            return -1;
        }

        private void readFullyFromStorage(long pos, byte[] buffer, int offset, int len) throws IOException {
            try (InputStream stream = storageObject.newStream(pos, len)) {
                int remaining = len;
                int off = offset;
                while (remaining > 0) {
                    int bytesRead = stream.read(buffer, off, remaining);
                    if (bytesRead < 0) {
                        throw new IOException("Reached end of stream before reading " + len + " bytes");
                    }
                    off += bytesRead;
                    remaining -= bytesRead;
                }
            }
        }

        // --- InputStream ---

        @Override
        public int read() throws IOException {
            int b = currentStream.read();
            if (b >= 0) {
                position++;
            }
            return b;
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

        private void reopenStreamAt(long newPos) throws IOException {
            if (currentStream != null) {
                currentStream.close();
            }
            long remainingBytes = length - newPos;
            currentStream = storageObject.newStream(newPos, remainingBytes);
            streamStartPosition = newPos;
            position = newPos;
        }
    }
}
