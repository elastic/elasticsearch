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

import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.cache.StatelessSharedBlobCacheService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.blobcache.common.BlobCacheBufferedIndexInput;
import org.elasticsearch.blobcache.common.ByteBufferReference;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.core.Streams;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

import static org.elasticsearch.blobcache.shared.SharedBytes.MAX_BYTES_PER_WRITE;

public final class SearchIndexInput extends BlobCacheBufferedIndexInput {

    private static final Logger logger = LogManager.getLogger(SearchIndexInput.class);

    private final IOContext context;

    private final StatelessSharedBlobCacheService.CacheFile cacheFile;

    private final long length;
    private final BlobContainer blobContainer;

    private final StatelessSharedBlobCacheService cacheService;
    private final long offset;

    public SearchIndexInput(
        String name,
        StatelessSharedBlobCacheService.CacheFile cacheFile,
        IOContext context,
        BlobContainer blobContainer,
        StatelessSharedBlobCacheService cacheService,
        long length,
        long offset
    ) {
        super(name, context);
        this.blobContainer = blobContainer;
        this.cacheService = cacheService;
        this.length = length;
        this.offset = offset;
        this.context = context;
        this.cacheFile = cacheFile.copy();
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
        return new SearchIndexInput(
            "(" + sliceDescription + ") " + super.toString(),
            cacheFile,
            context,
            blobContainer,
            cacheService,
            length,
            this.offset + offset
        );
    }

    @Override
    public SearchIndexInput clone() {
        SearchIndexInput searchIndexInput = new SearchIndexInput(
            "(clone of) " + super.toString(),
            cacheFile,
            context,
            blobContainer,
            cacheService,
            length,
            offset
        );
        try {
            searchIndexInput.seek(getFilePointer());
        } catch (IOException e) {
            assert false : e;
            throw new UncheckedIOException(e);
        }
        return searchIndexInput;
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
        try {
            if (cacheFile.tryRead(b, position)) {
                return;
            }
        } catch (Exception e) {
            logger.debug("fast-path read failed to acquire cache page", e);
        }
        readInternalSlow(b, position, length);
    }

    private void readInternalSlow(ByteBuffer b, long position, int length) throws Exception {
        // Semaphore that, when all permits are acquired, ensures that async callbacks (such as those used by readCacheFile) are not
        // accessing the byte buffer anymore that was passed to doReadInternal
        // In particular, it's important to acquire all permits before adapting the ByteBuffer's offset
        final ByteBufferReference byteBufferReference = new ByteBufferReference(b);
        try {
            final ByteRange rangeToWrite = BlobCacheUtils.computeRange(
                cacheService.getRangeSize(),
                position,
                length,
                cacheFile.getLength()
            );
            assert rangeToWrite.start() <= position && position + length <= rangeToWrite.end()
                : "[" + position + "-" + (position + length) + "] vs " + rangeToWrite;
            final ByteRange rangeToRead = ByteRange.of(position, position + length);

            int bytesRead = 0;
            try {
                bytesRead = cacheFile.populateAndRead(rangeToWrite, rangeToRead, (channel, channelPos, relativePos, len) -> {
                    logger.trace(
                        "{}: reading cached [{}][{}-{}]",
                        SearchIndexInput.super.toString(),
                        cacheFile.getCacheKey().fileName(),
                        rangeToRead.start(),
                        rangeToRead.start() + len
                    );
                    return SharedBytes.readCacheFile(channel, channelPos, relativePos, len, byteBufferReference);
                }, (channel, channelPos, relativePos, len, progressUpdater) -> {
                    final long streamStartPosition = rangeToWrite.start() + relativePos;
                    try (
                        InputStream in = blobContainer.readBlob(
                            OperationPurpose.INDICES,
                            this.cacheFile.getCacheKey().fileName(),
                            streamStartPosition,
                            len
                        )
                    ) {
                        assert ThreadPool.assertCurrentThreadPool(Stateless.SHARD_READ_THREAD_POOL);
                        logger.debug(
                            "{}: loading [{}][{}-{}] from blobstore",
                            SearchIndexInput.super.toString(),
                            cacheFile.getCacheKey().fileName(),
                            streamStartPosition,
                            streamStartPosition + len
                        );
                        SharedBytes.copyToCacheFileAligned(
                            channel,
                            in,
                            channelPos,
                            relativePos,
                            len,
                            progressUpdater,
                            writeBuffer.get().clear()
                        );
                    }
                });
                byteBufferReference.finish(bytesRead);
            } catch (Exception e) {
                if (e instanceof AlreadyClosedException || e.getCause() instanceof AlreadyClosedException) {
                    assert bytesRead == 0 : "expecting bytes read to be 0 but got: " + bytesRead + " for " + cacheFile.getCacheKey();
                    int len = length - bytesRead;
                    try (
                        InputStream in = blobContainer.readBlob(
                            OperationPurpose.INDICES,
                            this.cacheFile.getCacheKey().fileName(),
                            position,
                            len
                        )
                    ) {
                        final int read = Streams.read(in, b, len);
                        if (read == -1) {
                            BlobCacheUtils.throwEOF(position, len);
                        }
                        bytesRead += read;
                    }
                } else {
                    throw e;
                }
            }
            assert bytesRead == length : bytesRead + " vs " + length;
        } finally {
            byteBufferReference.finish(0);
        }
    }

    private static final ThreadLocal<ByteBuffer> writeBuffer = ThreadLocal.withInitial(
        () -> ByteBuffer.allocateDirect(MAX_BYTES_PER_WRITE)
    );

    // for tests only
    StatelessSharedBlobCacheService.CacheFile cacheFile() {
        return cacheFile;
    }

    // for tests only
    StatelessSharedBlobCacheService getCacheService() {
        return cacheService;
    }

    @Override
    public String toString() {
        return "SearchIndexInput{["
            + super.toString()
            + "], context="
            + context
            + ", cacheFile="
            + cacheFile
            + ", length="
            + length
            + ", offset="
            + offset
            + '}';
    }
}
