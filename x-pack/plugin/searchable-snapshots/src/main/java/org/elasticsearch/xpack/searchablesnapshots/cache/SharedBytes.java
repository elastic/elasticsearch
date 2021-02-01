/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.internal.io.IOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;

public class SharedBytes extends AbstractRefCounted {

    private static final Logger logger = LogManager.getLogger(SharedBytes.class);

    private static final StandardOpenOption[] OPEN_OPTIONS = new StandardOpenOption[] {
        StandardOpenOption.READ,
        StandardOpenOption.WRITE,
        StandardOpenOption.CREATE };

    final int numRegions;
    final long regionSize;

    private final FileChannel fileChannel;

    private final Path path;

    SharedBytes(int numRegions, long regionSize, Path file) throws IOException {
        super("shared-bytes");
        this.numRegions = numRegions;
        this.regionSize = regionSize;
        this.path = file;
        final long fileSize = numRegions * regionSize;
        if (fileSize > 0) {
            final ByteBuffer fillBytes = ByteBuffer.allocate(Channels.WRITE_CHUNK_SIZE);
            Files.createDirectories(file.getParent());
            this.fileChannel = FileChannel.open(file, OPEN_OPTIONS);
            long written = fileChannel.size();
            while (written < fileSize) {
                final int toWrite = Math.toIntExact(Math.min(fileSize - written, Channels.WRITE_CHUNK_SIZE));
                fillBytes.position(0).limit(toWrite);
                // write one byte at the end of the file to make sure all bytes are allocated
                Channels.writeToChannel(fillBytes, fileChannel);
                written += toWrite;
            }
            if (written > fileChannel.size()) {
                fileChannel.truncate(fileSize);
            }
        } else {
            this.fileChannel = null;
        }
    }

    @Override
    protected void closeInternal() {
        try {
            IOUtils.close(fileChannel, () -> Files.deleteIfExists(path));
        } catch (IOException e) {
            logger.warn("Failed to clean up shared bytes file", e);
        }
    }

    private final Map<Integer, IO> ios = ConcurrentCollections.newConcurrentMap();

    IO getFileChannel(int sharedBytesPos) {
        assert fileChannel != null;
        return ios.compute(sharedBytesPos, (p, io) -> {
            if (io == null || io.tryIncRef() == false) {
                final IO newIO;
                boolean success = false;
                incRef();
                try {
                    newIO = new IO(p);
                    success = true;
                } finally {
                    if (success == false) {
                        decRef();
                    }
                }
                return newIO;
            }
            return io;
        });
    }

    long getPhysicalOffset(long chunkPosition) {
        long physicalOffset = chunkPosition * regionSize;
        assert physicalOffset <= numRegions * regionSize;
        return physicalOffset;
    }

    public final class IO extends AbstractRefCounted {

        private final int sharedBytesPos;
        private final long pageStart;

        private IO(final int sharedBytesPos) {
            super("shared-bytes-io");
            this.sharedBytesPos = sharedBytesPos;
            pageStart = getPhysicalOffset(sharedBytesPos);
        }

        @SuppressForbidden(reason = "Use positional reads on purpose")
        public int read(ByteBuffer dst, long position) throws IOException {
            checkOffsets(position, dst.remaining());
            return fileChannel.read(dst, position);
        }

        @SuppressForbidden(reason = "Use positional writes on purpose")
        public int write(ByteBuffer src, long position) throws IOException {
            checkOffsets(position, src.remaining());
            return fileChannel.write(src, position);
        }

        private void checkOffsets(long position, long length) {
            long pageEnd = pageStart + regionSize;
            if (position < pageStart || position > pageEnd || position + length > pageEnd) {
                assert false;
                throw new IllegalArgumentException("bad access");
            }
        }

        @Override
        protected void closeInternal() {
            ios.remove(sharedBytesPos, this);
            SharedBytes.this.decRef();
        }
    }
}
