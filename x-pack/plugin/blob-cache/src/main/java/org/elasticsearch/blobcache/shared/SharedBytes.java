/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.shared;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.blobcache.common.ByteBufferReference;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Streams;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.nativeaccess.NativeAccess;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.IntConsumer;

public class SharedBytes extends AbstractRefCounted {

    /**
     * Thread local direct byte buffer to aggregate multiple positional writes to the cache file.
     */
    public static final int MAX_BYTES_PER_WRITE = StrictMath.toIntExact(
        ByteSizeValue.parseBytesSizeValue(
            System.getProperty("es.searchable.snapshot.shared_cache.write_buffer.size", "2m"),
            "es.searchable.snapshot.shared_cache.write_buffer.size"
        ).getBytes()
    );
    private static final Logger logger = LogManager.getLogger(SharedBytes.class);

    public static int PAGE_SIZE = 4096;

    private static final String CACHE_FILE_NAME = "shared_snapshot_cache";

    private static final StandardOpenOption[] OPEN_OPTIONS = new StandardOpenOption[] {
        StandardOpenOption.READ,
        StandardOpenOption.WRITE,
        StandardOpenOption.CREATE };

    private static final long MAX_BYTES_PER_MAP = ByteSizeValue.ofGb(1).getBytes();

    final int numRegions;

    private final IO[] ios;

    final int regionSize;

    // TODO: for systems like Windows without true p-write/read support we should split this up into multiple channels since positional
    // operations in #IO are not contention-free there (https://bugs.java.com/bugdatabase/view_bug.do?bug_id=6265734)
    private final FileChannel fileChannel;
    private final Path path;

    private final IntConsumer writeBytes;
    private final IntConsumer readBytes;

    private final boolean mmap;

    SharedBytes(int numRegions, int regionSize, NodeEnvironment environment, IntConsumer writeBytes, IntConsumer readBytes, boolean mmap)
        throws IOException {
        this.numRegions = numRegions;
        this.regionSize = regionSize;
        final long fileSize = (long) numRegions * regionSize;
        Path cacheFile = null;
        if (fileSize > 0) {
            cacheFile = findCacheSnapshotCacheFilePath(environment, fileSize);
            preallocate(cacheFile, fileSize);
            this.fileChannel = FileChannel.open(cacheFile, OPEN_OPTIONS);
            assert this.fileChannel.size() == fileSize : "expected file size " + fileSize + " but was " + fileChannel.size();
        } else {
            this.fileChannel = null;
            for (Path path : environment.nodeDataPaths()) {
                Files.deleteIfExists(path.resolve(CACHE_FILE_NAME));
            }
        }
        this.path = cacheFile;
        this.mmap = mmap;
        this.ios = new IO[numRegions];
        if (mmap && fileSize > 0) {
            int regionsPerMmap = Math.toIntExact(MAX_BYTES_PER_MAP / regionSize);
            int mapSize = regionsPerMmap * regionSize;
            int lastMapSize = Math.toIntExact(fileSize % mapSize);
            int mapCount = Math.toIntExact(fileSize / mapSize) + (lastMapSize == 0 ? 0 : 1);
            MappedByteBuffer[] mmaps = new MappedByteBuffer[mapCount];
            for (int i = 0; i < mapCount - 1; i++) {
                mmaps[i] = fileChannel.map(FileChannel.MapMode.READ_ONLY, (long) mapSize * i, mapSize);
            }
            mmaps[mapCount - 1] = fileChannel.map(
                FileChannel.MapMode.READ_ONLY,
                (long) mapSize * (mapCount - 1),
                lastMapSize == 0 ? mapSize : lastMapSize
            );
            for (int i = 0; i < numRegions; i++) {
                ios[i] = new IO(i, mmaps[i / regionsPerMmap].slice((i % regionsPerMmap) * regionSize, regionSize));
            }
        } else {
            for (int i = 0; i < numRegions; i++) {
                ios[i] = new IO(i, null);
            }
        }
        this.writeBytes = writeBytes;
        this.readBytes = readBytes;
    }

    /**
     * Tries to find a suitable path to a searchable snapshots shared cache file in the data paths founds in the environment.
     *
     * @return path for the cache file or {@code null} if none could be found
     */
    public static Path findCacheSnapshotCacheFilePath(NodeEnvironment environment, long fileSize) throws IOException {
        assert environment.nodeDataPaths().length == 1;
        Path path = environment.nodeDataPaths()[0];
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
            throw new IOException(
                "Not enough free space [" + usableSpace + "] for cache file of size [" + fileSize + "] in path [" + path + "]"
            );
        }
    }

    @SuppressForbidden(reason = "random access file needed to set file size")
    static void preallocate(Path cacheFile, long fileSize) throws IOException {
        // first try using native methods to preallocate space in the file
        NativeAccess.instance().tryPreallocate(cacheFile, fileSize);
        // even if allocation was successful above, verify again here
        try (RandomAccessFile raf = new RandomAccessFile(cacheFile.toFile(), "rw")) {
            if (raf.length() != fileSize) {
                logger.info("pre-allocating cache file [{}] ({} bytes) using setLength method", cacheFile, fileSize);
                raf.setLength(fileSize);
                logger.debug("pre-allocated cache file [{}] using setLength method", cacheFile);
            }
        } catch (final Exception e) {
            logger.warn(() -> "failed to pre-allocate cache file [" + cacheFile + "] using setLength method", e);
            // if anything goes wrong, delete the potentially created file to not waste disk space
            Files.deleteIfExists(cacheFile);
        }
    }

    /**
     * Copy {@code length} bytes from {@code input} to {@code fc}, only doing writes aligned along {@link #PAGE_SIZE}.
     *
     * @param fc output cache file reference
     * @param input stream to read from
     * @param fileChannelPos position in {@code fc} to write to
     * @param relativePos relative position in the Lucene file the is read from {@code input}
     * @param length number of bytes to copy
     * @param progressUpdater callback to invoke with the number of copied bytes as they are copied
     * @param buf bytebuffer to use for writing
     * @throws IOException on failure
     */
    public static void copyToCacheFileAligned(
        IO fc,
        InputStream input,
        int fileChannelPos,
        int relativePos,
        int length,
        IntConsumer progressUpdater,
        ByteBuffer buf
    ) throws IOException {
        int bytesCopied = 0;
        long remaining = length;
        while (remaining > 0L) {
            final int bytesRead = BlobCacheUtils.readSafe(input, buf, relativePos, remaining);
            if (buf.hasRemaining()) {
                break;
            }
            bytesCopied += positionalWrite(fc, fileChannelPos + bytesCopied, buf);
            progressUpdater.accept(bytesCopied);
            remaining -= bytesRead;
        }
        if (remaining > 0) {
            // ensure that last write is aligned on 4k boundaries (= page size)
            final int remainder = buf.position() % PAGE_SIZE;
            final int adjustment = remainder == 0 ? 0 : PAGE_SIZE - remainder;
            buf.position(buf.position() + adjustment);
            bytesCopied += positionalWrite(fc, fileChannelPos + bytesCopied, buf);
            final int adjustedBytesCopied = bytesCopied - adjustment; // adjust to not break RangeFileTracker
            assert adjustedBytesCopied == length : adjustedBytesCopied + " vs " + length;
            progressUpdater.accept(adjustedBytesCopied);
        }
    }

    /**
     * Copy all bytes from {@code input} to {@code fc}, only doing writes aligned along {@link #PAGE_SIZE}.
     *
     * @param fc output cache file reference
     * @param input stream to read from
     * @param fileChannelPos position in {@code fc} to write to
     * @param progressUpdater callback to invoke with the number of copied bytes as they are copied
     * @param buffer bytebuffer to use for writing
     * @return the number of bytes copied
     * @throws IOException on failure
     */
    public static int copyToCacheFileAligned(IO fc, InputStream input, int fileChannelPos, IntConsumer progressUpdater, ByteBuffer buffer)
        throws IOException {
        int bytesCopied = 0;
        while (true) {
            final int bytesRead = Streams.read(input, buffer, buffer.remaining());
            if (bytesRead <= 0) {
                break;
            }
            bytesCopied += copyBufferToCacheFileAligned(fc, fileChannelPos + bytesCopied, buffer);
            progressUpdater.accept(bytesCopied);
        }
        return bytesCopied;
    }

    /**
     * Copy all bytes from {@code buffer} to {@code fc}, only doing writes aligned along {@link #PAGE_SIZE}.
     *
     * @param fc output cache file reference
     * @param fileChannelPos position in {@code fc} to write to
     * @param buffer bytebuffer to copy from
     * @return the number of bytes copied
     * @throws IOException on failure
     */
    public static int copyBufferToCacheFileAligned(IO fc, int fileChannelPos, ByteBuffer buffer) throws IOException {
        if (buffer.hasRemaining()) {
            // ensure the write is aligned on 4k boundaries (= page size)
            final int remainder = buffer.position() % PAGE_SIZE;
            final int adjustment = remainder == 0 ? 0 : PAGE_SIZE - remainder;
            buffer.position(buffer.position() + adjustment);
        }
        return positionalWrite(fc, fileChannelPos, buffer);
    }

    private static int positionalWrite(IO fc, int start, ByteBuffer byteBuffer) throws IOException {
        byteBuffer.flip();
        int written = fc.write(byteBuffer, start);
        assert byteBuffer.hasRemaining() == false;
        byteBuffer.clear();
        return written;
    }

    /**
     * Read {@code length} bytes from given shared bytes at given {@code channelPos} into {@code byteBufferReference} at given
     * {@code relativePos}.
     * @param fc shared bytes channel to read from
     * @param channelPos position in {@code fc} to read from
     * @param relativePos position in {@code byteBufferReference}
     * @param length number of bytes to read
     * @param byteBufferReference buffer reference
     * @return number of bytes read
     * @throws IOException on failure
     */
    public static int readCacheFile(final IO fc, int channelPos, int relativePos, int length, final ByteBufferReference byteBufferReference)
        throws IOException {
        if (length == 0L) {
            return 0;
        }
        final int bytesRead;
        final ByteBuffer dup = byteBufferReference.tryAcquire(relativePos, length);
        if (dup != null) {
            try {
                bytesRead = fc.read(dup, channelPos);
                if (bytesRead == -1) {
                    BlobCacheUtils.throwEOF(channelPos, dup.remaining());
                }
            } finally {
                byteBufferReference.release();
            }
        } else {
            // return fake response
            return length;
        }
        return bytesRead;
    }

    @Override
    protected void closeInternal() {
        try {
            IOUtils.close(fileChannel, path == null ? null : () -> Files.deleteIfExists(path));
        } catch (IOException e) {
            logger.warn("Failed to clean up shared bytes file", e);
        }
    }

    public IO getFileChannel(int sharedBytesPos) {
        assert fileChannel != null;
        return ios[sharedBytesPos];
    }

    public final class IO {

        private final long pageStart;

        private final MappedByteBuffer mappedByteBuffer;

        private IO(final int sharedBytesPos, MappedByteBuffer mappedByteBuffer) {
            long physicalOffset = (long) sharedBytesPos * regionSize;
            assert physicalOffset <= (long) numRegions * regionSize;
            this.pageStart = physicalOffset;
            this.mappedByteBuffer = mappedByteBuffer;
        }

        @SuppressForbidden(reason = "Use positional reads on purpose")
        public int read(ByteBuffer dst, int position) throws IOException {
            int remaining = dst.remaining();
            checkOffsets(position, remaining);
            final int bytesRead;
            if (mmap) {
                bytesRead = remaining;
                int startPosition = dst.position();
                dst.put(startPosition, mappedByteBuffer, position, bytesRead).position(startPosition + bytesRead);
            } else {
                bytesRead = fileChannel.read(dst, pageStart + position);
            }
            readBytes.accept(bytesRead);
            return bytesRead;
        }

        @SuppressForbidden(reason = "Use positional writes on purpose")
        public int write(ByteBuffer src, int position) throws IOException {
            // check if writes are page size aligned for optimal performance
            assert position % PAGE_SIZE == 0;
            assert src.remaining() % PAGE_SIZE == 0;
            checkOffsets(position, src.remaining());
            int bytesWritten = fileChannel.write(src, pageStart + position);
            writeBytes.accept(bytesWritten);
            return bytesWritten;
        }

        private void checkOffsets(int position, int length) {
            if (position < 0 || position + length > regionSize) {
                offsetCheckFailed();
            }
        }

        private static void offsetCheckFailed() {
            assert false;
            throw new IllegalArgumentException("bad access");
        }
    }

}
