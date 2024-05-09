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
 *
 * This file was contributed to by generative AI
 */

package co.elastic.elasticsearch.stateless.lucene;

import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.cache.StatelessSharedBlobCacheService;
import co.elastic.elasticsearch.stateless.cache.reader.CacheBlobReader;

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

    private final CacheBlobReader cacheBlobReader;
    private final long offset;

    public SearchIndexInput(
        String name,
        StatelessSharedBlobCacheService.CacheFile cacheFile,
        IOContext context,
        CacheBlobReader cacheBlobReader,
        long length,
        long offset
    ) {
        super(name, context, length);
        this.cacheBlobReader = cacheBlobReader;
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
    public IndexInput slice(String sliceDescription, long offset, long length) {
        BlobCacheUtils.ensureSlice(sliceDescription, offset, length, this);
        var arraySlice = trySliceBuffer(sliceDescription, offset, length);
        if (arraySlice != null) {
            return arraySlice;
        }
        return new SearchIndexInput(
            "(" + sliceDescription + ") " + super.toString(),
            cacheFile,
            context,
            cacheBlobReader,
            length,
            this.offset + offset
        );
    }

    @Override
    public IndexInput clone() {
        var bufferClone = tryCloneBuffer();
        if (bufferClone != null) {
            return bufferClone;
        }
        SearchIndexInput searchIndexInput = new SearchIndexInput(super.toString(), cacheFile, context, cacheBlobReader, length(), offset);
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
            logger.debug("reading file: {}, position: {}, length: {}", super.toString(), position, length);
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
            // Compute the range of bytes of the blob to fetch and to write to the cache.
            //
            // The range represents one or more full regions to fetch. It can also be larger (in both directions) than the file opened by
            // the current SearchIndexInput instance. The range can also be larger than the real length of the blob in the object store.
            // This is OK, we rely on the object store to return as many bytes as possible without failing.

            // we use the length from `cacheFile` since this allows reading beyond the slice'd portion of the file, important for
            // reading beyond individual files inside CFS.
            long remainingFileLength = cacheFile.getLength() - position;
            assert remainingFileLength >= 0 : remainingFileLength;
            assert length <= remainingFileLength : length + " > " + remainingFileLength;
            assert remainingFileLength >= offset + length() - position
                : "cache file length smaller than file length " + cacheFile.getLength() + " < " + offset + length();
            final ByteRange rangeToWrite = cacheBlobReader.getRange(position, length, remainingFileLength);

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
                        // this length is computed from the rangeToWrite and the sum of "streamStartPosition + len" can exceed the real
                        // length of the blob
                        InputStream in = cacheBlobReader.getRangeInputStream(streamStartPosition, len)
                    ) {
                        assert ThreadPool.assertCurrentThreadPool(Stateless.SHARD_READ_THREAD_POOL);
                        logger.debug(
                            "{}: loading [{}][{}-{}] from blobstore",
                            SearchIndexInput.super.toString(),
                            cacheFile.getCacheKey().fileName(),
                            streamStartPosition,
                            streamStartPosition + len
                        );
                        var bytesCopied = SharedBytes.copyToCacheFileAligned(
                            channel,
                            in,
                            channelPos,
                            progressUpdater,
                            writeBuffer.get().clear()
                        );
                        if (bytesCopied < len) {
                            // TODO we should remove this and allow gap completion in SparseFileTracker even if progress < range end
                            progressUpdater.accept(len);
                        }
                    }
                });
                byteBufferReference.finish(bytesRead);
            } catch (Exception e) {
                if (e instanceof AlreadyClosedException || e.getCause() instanceof AlreadyClosedException) {
                    assert bytesRead == 0 : "expecting bytes read to be 0 but got: " + bytesRead + " for " + cacheFile.getCacheKey();
                    int len = length - bytesRead;
                    try (InputStream in = cacheBlobReader.getRangeInputStream(position, len)) {
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

    @Override
    public String toString() {
        return "SearchIndexInput{["
            + super.toString()
            + "], context="
            + context
            + ", cacheFile="
            + cacheFile
            + ", length="
            + length()
            + ", offset="
            + offset
            + '}';
    }
}
