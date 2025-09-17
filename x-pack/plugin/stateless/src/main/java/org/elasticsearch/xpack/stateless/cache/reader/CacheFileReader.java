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

package co.elastic.elasticsearch.stateless.cache.reader;

import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.cache.StatelessSharedBlobCacheService;
import co.elastic.elasticsearch.stateless.commits.BlobFileRanges;
import co.elastic.elasticsearch.stateless.lucene.BlobCacheIndexInput;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.blobcache.common.ByteBufferReference;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.core.Streams;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import static org.elasticsearch.blobcache.shared.SharedBytes.MAX_BYTES_PER_WRITE;

/**
 * Used by {@link BlobCacheIndexInput} to read data from the cache using a given {@link StatelessSharedBlobCacheService.CacheFile} instance.
 * When bytes are not cached, the reader uses the provided {@link CacheBlobReader} to fetch data from different sources.
 */
public class CacheFileReader {

    private static final Logger logger = LogManager.getLogger(CacheFileReader.class);

    private final StatelessSharedBlobCacheService.CacheFile cacheFile;
    private final CacheBlobReader cacheBlobReader;
    private final BlobFileRanges blobFileRanges;

    public CacheFileReader(
        StatelessSharedBlobCacheService.CacheFile cacheFile,
        CacheBlobReader cacheBlobReader,
        BlobFileRanges blobFileRanges
    ) {
        this.cacheFile = Objects.requireNonNull(cacheFile);
        this.cacheBlobReader = Objects.requireNonNull(cacheBlobReader);
        this.blobFileRanges = Objects.requireNonNull(blobFileRanges);
    }

    /**
     * @return a new instance that is a copy of the current instance
     */
    public CacheFileReader copy() {
        return new CacheFileReader(cacheFile.copy(), cacheBlobReader, blobFileRanges);
    }

    /**
     * Attempts to read byte(s) from the cache using the fast path.
     *
     * @param b the {@link ByteBuffer} to write bytes into
     * @param position the starting position to read from
     * @return true is reading using the fast path succeeded, false otherwise
     * @throws IOException if an I/O error occurs
     */
    public final boolean tryRead(ByteBuffer b, long position) throws IOException {
        return cacheFile.tryRead(b, position);
    }

    /**
     * Reads byte(s) from the cache, potentially fetching the data from a remote source if the data are not present in cache (slow path).
     *
     * @param initiator     the caller (used for debug logging)
     * @param b             the {@link ByteBuffer} to write bytes into
     * @param position      the starting position to read from
     * @param length        the length of bytes to read
     * @param endOfInput    the length of the {@link BlobCacheIndexInput} that triggers the read (used for assertions)
     * @throws Exception    if an error occurs
     */
    public void read(Object initiator, ByteBuffer b, long position, int length, long endOfInput, String resourceDescription)
        throws Exception {
        doRead(initiator, b, blobFileRanges.getPosition(position, length), length, endOfInput, resourceDescription);
    }

    private void doRead(Object initiator, ByteBuffer b, long position, int length, long endOfInput, String resourceDescription)
        throws Exception {
        // Semaphore that, when all permits are acquired, ensures that async callbacks (such as those used by readCacheFile) are not
        // accessing the byte buffer anymore that was passed to doReadInternal
        // In particular, it's important to acquire all permits before adapting the ByteBuffer's offset
        final ByteBufferReference byteBufferReference = new ByteBufferReference(b);
        try {
            // Compute the range of bytes of the blob to fetch and to write to the cache.
            //
            // The range represents one or more full regions to fetch. It can also be larger (in both directions) than the file opened by
            // the current BlobCacheIndexInput instance. The range can also be larger than the real length of the blob in the object store.
            // This is OK, we rely on the object store to return as many bytes as possible without failing.

            // we use the length from `cacheFile` since this allows reading beyond the slice'd portion of the file, important for
            // reading beyond individual files inside CFS.
            long remainingFileLength = cacheFile.getLength() - position;
            assert remainingFileLength >= 0 : remainingFileLength;
            assert length <= remainingFileLength : length + " > " + remainingFileLength;
            assert remainingFileLength >= endOfInput - position
                : "cache file length smaller than file length " + cacheFile.getLength() + " < " + endOfInput;
            final ByteRange rangeToWrite = cacheBlobReader.getRange(position, length, remainingFileLength);

            assert rangeToWrite.start() <= position && position + length <= rangeToWrite.end()
                : "[" + position + "-" + (position + length) + "] vs " + rangeToWrite;
            final ByteRange rangeToRead = ByteRange.of(position, position + length);

            int bytesRead = 0;
            try {
                bytesRead = cacheFile.populateAndRead(rangeToWrite, rangeToRead, (channel, channelPos, relativePos, len) -> {
                    logger.trace(
                        "{}: reading cached [{}][{}-{}]",
                        initiator.toString(),
                        cacheFile.getCacheKey().fileName(),
                        rangeToRead.start(),
                        rangeToRead.start() + len
                    );
                    return SharedBytes.readCacheFile(channel, channelPos, relativePos, len, byteBufferReference);
                },
                    // Can be executed on different thread pool depending on whether we read from
                    // the ObjectStoreCacheBlobReader (SHARD_READ pool) or the IndexingShardCacheBlobReader (VBCC pool)
                    new SequentialRangeMissingHandler(
                        initiator,
                        cacheFile.getCacheKey().fileName(),
                        rangeToWrite,
                        cacheBlobReader,
                        () -> writeBuffer.get().clear(),
                        bytesCopied -> {},
                        Stateless.SHARD_READ_THREAD_POOL,
                        Stateless.FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL
                    ),
                    resourceDescription
                );
                byteBufferReference.finish(bytesRead);
            } catch (Exception e) {
                if (e instanceof AlreadyClosedException || e.getCause() instanceof AlreadyClosedException) {
                    assert bytesRead == 0 : "expecting bytes read to be 0 but got: " + bytesRead + " for " + cacheFile.getCacheKey();
                    int len = length - bytesRead;
                    // TODO ideally we would make it async, but it should be safe
                    // since the future is created on the shard read thread pool or GET_VIRTUAL_BATCHED_COMPOUND_COMMIT_CHUNK_THREAD_POOL.
                    // ObjectStoreCacheBlobReader is completed on the same thread and before actually waiting on the future, and
                    // IndexingShardCacheBlobReader should be completed on the FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL
                    var readFuture = new PlainActionFuture<Integer>();
                    cacheBlobReader.getRangeInputStream(position, len, readFuture.map(in -> {
                        try (in) {
                            final int read = Streams.read(in, b, len);
                            if (read == -1) {
                                BlobCacheUtils.throwEOF(position, len);
                            }
                            return read;
                        }
                    }));
                    bytesRead += FutureUtils.get(readFuture);
                } else {
                    throw e;
                }
            }
            assert bytesRead == length : bytesRead + " vs " + length;
        } finally {
            byteBufferReference.finish(0);
        }
    }

    // package-private for testing
    StatelessSharedBlobCacheService.CacheFile getCacheFile() {
        return cacheFile;
    }

    @Override
    public String toString() {
        return cacheFile.toString();
    }

    private static final ThreadLocal<ByteBuffer> writeBuffer = ThreadLocal.withInitial(
        () -> ByteBuffer.allocateDirect(MAX_BYTES_PER_WRITE)
    );
}
