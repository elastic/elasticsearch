/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.lucene;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.core.Streams;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.Semaphore;

import static org.elasticsearch.blobcache.shared.SharedBytes.MAX_BYTES_PER_WRITE;

public class SearchIndexInput extends BufferedIndexInput {

    private static final Logger logger = LogManager.getLogger(SearchIndexInput.class);

    private final IOContext context;

    private final SharedBlobCacheService<FileCacheKey>.CacheFile cacheFile;

    private final long length;
    private final BlobContainer blobContainer;

    private final SharedBlobCacheService<FileCacheKey> cacheService;
    private final long offset;

    public SearchIndexInput(
        String name,
        SharedBlobCacheService<FileCacheKey>.CacheFile cacheFile,
        IOContext context,
        BlobContainer blobContainer,
        SharedBlobCacheService<FileCacheKey> cacheService,
        long length,
        long offset
    ) {
        super(name, context);
        this.blobContainer = blobContainer;
        this.cacheService = cacheService;
        this.length = length;
        this.offset = offset;
        this.context = context;
        this.cacheFile = cacheFile;
    }

    @Override
    protected void seekInternal(long pos) throws IOException {
        BlobCacheUtils.ensureSeek(pos, this);
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) {
        BlobCacheUtils.ensureSlice(sliceDescription, offset, length, this);
        return new SearchIndexInput(sliceDescription, cacheFile, context, blobContainer, cacheService, length, this.offset + offset);
    }

    private long getAbsolutePosition() {
        return getFilePointer() + offset;
    }

    @Override
    protected void readInternal(ByteBuffer b) throws IOException {
        try {
            doReadInternal(b);
        } catch (IOException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private void doReadInternal(ByteBuffer b) throws Exception {
        final long position = getAbsolutePosition();
        final int length = b.remaining();
        final int originalByteBufPosition = b.position();

        // Semaphore that, when all permits are acquired, ensures that async callbacks (such as those used by readCacheFile) are not
        // accessing the byte buffer anymore that was passed to doReadInternal
        // In particular, it's important to acquire all permits before adapting the ByteBuffer's offset
        final Semaphore luceneByteBufPermits = new Semaphore(Integer.MAX_VALUE);
        boolean bufferWriteLocked = false;
        logger.trace("readInternal: read [{}-{}] ([{}] bytes) from [{}]", position, position + length, length, this);
        try {
            final ByteRange startRangeToWrite = computeRange(position);
            final ByteRange endRangeToWrite = computeRange(position + length - 1);
            assert startRangeToWrite.end() <= endRangeToWrite.end() : startRangeToWrite + " vs " + endRangeToWrite;
            final ByteRange rangeToWrite = startRangeToWrite.minEnvelope(endRangeToWrite);

            assert rangeToWrite.start() <= position && position + length <= rangeToWrite.end()
                : "[" + position + "-" + (position + length) + "] vs " + rangeToWrite;
            final ByteRange rangeToRead = ByteRange.of(position, position + length);

            int bytesRead = 0;
            try {
                bytesRead = cacheFile.populateAndRead(
                    rangeToWrite,
                    rangeToRead,
                    (channel, pos, relativePos, len) -> readCacheFile(
                        channel,
                        pos,
                        relativePos,
                        len,
                        b,
                        rangeToRead.start(),
                        luceneByteBufPermits
                    ),
                    (channel, channelPos, relativePos, len, progressUpdater) -> {
                        final long streamStartPosition = rangeToWrite.start() + relativePos;
                        try (InputStream in = blobContainer.readBlob(this.cacheFile.getCacheKey().fileName(), streamStartPosition, len)) {
                            assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GENERIC);
                            logger.trace(
                                "{}: writing channel {} pos {} length {} (details: {})",
                                cacheFile.getCacheKey().fileName(),
                                channelPos,
                                relativePos,
                                len,
                                cacheFile
                            );
                            SharedBytes.copyToCacheFileAligned(
                                channel,
                                in,
                                channelPos,
                                relativePos,
                                len,
                                progressUpdater,
                                writeBuffer.get().clear(),
                                cacheFile
                            );
                        }
                    },
                    ThreadPool.Names.GENERIC // TODO: figure out a better/correct threadpool for this
                );
            } catch (Exception e) {
                if (e instanceof AlreadyClosedException || e.getCause() instanceof AlreadyClosedException) {
                    int len = length - bytesRead;
                    try (InputStream in = blobContainer.readBlob(this.cacheFile.getCacheKey().fileName(), position, len)) {
                        final int read = Streams.read(in, b, len);
                        if (read == -1) {
                            BlobCacheUtils.throwEOF(position, len, cacheFile);
                        }
                        bytesRead += read;
                    }
                } else {
                    throw e;
                }
            }
            assert bytesRead == length : bytesRead + " vs " + length;
            assert luceneByteBufPermits.availablePermits() == Integer.MAX_VALUE;

            luceneByteBufPermits.acquire(Integer.MAX_VALUE);
            bufferWriteLocked = true;
            b.position(originalByteBufPosition + bytesRead); // mark all bytes as accounted for
        } finally {
            if (bufferWriteLocked == false) {
                luceneByteBufPermits.acquire(Integer.MAX_VALUE);
            }
        }
    }

    private int readCacheFile(
        final SharedBytes.IO fc,
        long channelPos,
        long relativePos,
        long length,
        final ByteBuffer buffer,
        long logicalPos,
        Semaphore luceneByteBufPermits
    ) throws IOException {
        logger.trace(
            "{}: reading cached {} logical {} channel {} pos {} length {} (details: {})",
            cacheFile.getCacheKey().fileName(),
            false,
            logicalPos,
            channelPos,
            relativePos,
            length,
            cacheFile
        );
        if (length == 0L) {
            return 0;
        }
        final int bytesRead;
        if (luceneByteBufPermits.tryAcquire()) {
            try {
                // create slice that is positioned to read the given values
                final ByteBuffer dup = buffer.slice(buffer.position() + Math.toIntExact(relativePos), Math.toIntExact(length));
                bytesRead = fc.read(dup, channelPos);
                if (bytesRead == -1) {
                    BlobCacheUtils.throwEOF(channelPos, dup.remaining(), cacheFile);
                }
            } finally {
                luceneByteBufPermits.release();
            }
        } else {
            // return fake response
            return Math.toIntExact(length);
        }
        return bytesRead;
    }

    private static final ThreadLocal<ByteBuffer> writeBuffer = ThreadLocal.withInitial(
        () -> ByteBuffer.allocateDirect(MAX_BYTES_PER_WRITE)
    );

    private ByteRange computeRange(long position) {
        return BlobCacheUtils.computeRange(64 * 1024, position, cacheFile.getLength());
    }

}
