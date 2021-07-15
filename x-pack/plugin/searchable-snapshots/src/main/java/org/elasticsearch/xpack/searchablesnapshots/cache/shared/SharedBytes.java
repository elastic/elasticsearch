/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache.shared;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.xpack.searchablesnapshots.preallocate.Preallocate;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.function.IntConsumer;

public class SharedBytes extends AbstractRefCounted {

    private static final Logger logger = LogManager.getLogger(SharedBytes.class);

    public static int PAGE_SIZE = 4096;

    private static final String CACHE_FILE_NAME = "shared_snapshot_cache";

    private static final StandardOpenOption[] OPEN_OPTIONS = new StandardOpenOption[] {
        StandardOpenOption.READ,
        StandardOpenOption.WRITE,
        StandardOpenOption.CREATE };

    final int numRegions;
    final long regionSize;

    // TODO: for systems like Windows without true p-write/read support we should split this up into multiple channels since positional
    // operations in #IO are not contention-free there (https://bugs.java.com/bugdatabase/view_bug.do?bug_id=6265734)
    private final FileChannel fileChannel;
    private final Path path;

    private final IntConsumer writeBytes;
    private final IntConsumer readBytes;

    SharedBytes(int numRegions, long regionSize, NodeEnvironment environment, IntConsumer writeBytes, IntConsumer readBytes)
        throws IOException {
        super("shared-bytes");
        this.numRegions = numRegions;
        this.regionSize = regionSize;
        final long fileSize = numRegions * regionSize;
        Path cacheFile = null;
        if (fileSize > 0) {
            cacheFile = findCacheSnapshotCacheFilePath(environment, fileSize);
            Preallocate.preallocate(cacheFile, fileSize);
            // TODO: maybe make this faster by allocating a larger direct buffer if this is too slow for very large files
            // We fill either the full file or the bytes between its current size and the desired size once with zeros to fully allocate
            // the file up front
            final ByteBuffer fillBytes = ByteBuffer.allocate(Channels.WRITE_CHUNK_SIZE);
            this.fileChannel = FileChannel.open(cacheFile, OPEN_OPTIONS);
            long written = fileChannel.size();
            if (fileSize < written) {
                logger.info("creating shared snapshot cache file [size={}, path={}]", fileSize, cacheFile);
            } else if (fileSize == written) {
                logger.debug("reusing existing shared snapshot cache file [size={}, path={}]", fileSize, cacheFile);
            }
            fileChannel.position(written);
            while (written < fileSize) {
                final int toWrite = Math.toIntExact(Math.min(fileSize - written, Channels.WRITE_CHUNK_SIZE));
                fillBytes.position(0).limit(toWrite);
                Channels.writeToChannel(fillBytes, fileChannel);
                written += toWrite;
            }
            if (written > fileChannel.size()) {
                fileChannel.truncate(fileSize);
            }
        } else {
            this.fileChannel = null;
            Files.deleteIfExists(environment.nodeDataPath().resolve(CACHE_FILE_NAME));
        }
        this.path = cacheFile;
        this.writeBytes = writeBytes;
        this.readBytes = readBytes;
    }

    /**
     * Tries to find a suitable path to a searchable snapshots shared cache file in the data paths founds in the environment.
     *
     * @return path for the cache file or {@code null} if none could be found
     */
    public static Path findCacheSnapshotCacheFilePath(NodeEnvironment environment, long fileSize) throws IOException {
        Path path = environment.nodeDataPath();
        Files.createDirectories(path);
        // TODO: be resilient to this check failing and try next path?
        long usableSpace = Environment.getUsableSpace(path);
        Path p = path.resolve(CACHE_FILE_NAME);
        if (Files.exists(p)) {
            usableSpace += Files.size(p);
        }
        // TODO: leave some margin for error here
        if (usableSpace > fileSize) {
            return p;
        } else {
            throw new IOException("Not enough free space for cache file of size [" + fileSize + "] in path [" + path + "]");
        }
    }

    @Override
    protected void closeInternal() {
        try {
            IOUtils.close(fileChannel, path == null ? null : () -> Files.deleteIfExists(path));
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
            final int bytesRead = fileChannel.read(dst, position);
            readBytes.accept(bytesRead);
            return bytesRead;
        }

        @SuppressForbidden(reason = "Use positional writes on purpose")
        public int write(ByteBuffer src, long position) throws IOException {
            // check if writes are page size aligned for optimal performance
            assert position % PAGE_SIZE == 0;
            assert src.remaining() % PAGE_SIZE == 0;
            checkOffsets(position, src.remaining());
            final int bytesWritten = fileChannel.write(src, position);
            writeBytes.accept(bytesWritten);
            return bytesWritten;
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

    public static ByteSizeValue pageAligned(ByteSizeValue val) {
        final long remainder = val.getBytes() % PAGE_SIZE;
        if (remainder != 0L) {
            return ByteSizeValue.ofBytes(val.getBytes() + PAGE_SIZE - remainder);
        }
        return val;
    }
}
