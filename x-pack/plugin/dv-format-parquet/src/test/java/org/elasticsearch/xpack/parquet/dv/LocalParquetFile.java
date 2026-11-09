/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.dv;

import org.elasticsearch.common.io.Channels;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.StorageIdentity;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;

/**
 * A Parquet file on the local disk of the node. Each stream opens its own channel and reads by position, so concurrent streams over one
 * file share nothing and closing a stream releases its file handle.
 */
final class LocalParquetFile implements StorageObject {

    /** Different files never share footer cache entries, and neither do local files with remote objects. */
    private static final StorageIdentity IDENTITY = new StorageIdentity() {};

    private final Path path;

    LocalParquetFile(Path path) {
        this.path = path;
    }

    @Override
    public StorageIdentity storageIdentity() {
        return IDENTITY;
    }

    /**
     * The reader caches parsed footers under a key that includes the version, and an object without one is never cached, so every
     * forward scan would parse the footer again. The modification time and the size identify the content of a local file.
     */
    @Override
    public String version() {
        try {
            return Files.getLastModifiedTime(path).toMillis() + "-" + Files.size(path);
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public void attachMetrics(ExternalSourceMetrics metrics, String scheme) {}

    @Override
    public InputStream newStream() throws IOException {
        return newStream(0, READ_TO_END);
    }

    @Override
    public InputStream newStream(long position, long length) throws IOException {
        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
        try {
            long size = channel.size();
            long end = length == READ_TO_END ? size : Math.min(size, position + length);
            return new ChannelStream(channel, position, end);
        } catch (IOException | RuntimeException e) {
            IOUtils.closeWhileHandlingException(channel);
            throw e;
        }
    }

    @Override
    public long length() throws IOException {
        return Files.size(path);
    }

    @Override
    public Instant lastModified() throws IOException {
        return Files.getLastModifiedTime(path).toInstant();
    }

    @Override
    public boolean exists() {
        return Files.exists(path);
    }

    @Override
    public StoragePath path() {
        return StoragePath.of(path.toUri().toString());
    }

    private static final class ChannelStream extends InputStream {
        private final FileChannel channel;
        private final long end;
        private long next;

        ChannelStream(FileChannel channel, long start, long end) {
            this.channel = channel;
            this.next = start;
            this.end = end;
        }

        @Override
        public int read() throws IOException {
            byte[] one = new byte[1];
            return read(one, 0, 1) < 0 ? -1 : one[0] & 0xff;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (len == 0) {
                return 0;
            }
            if (next >= end) {
                return -1;
            }
            int read = Channels.readFromFileChannel(channel, next, ByteBuffer.wrap(b, off, (int) Math.min(len, end - next)));
            if (read > 0) {
                next += read;
            }
            return read;
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }
    }
}
