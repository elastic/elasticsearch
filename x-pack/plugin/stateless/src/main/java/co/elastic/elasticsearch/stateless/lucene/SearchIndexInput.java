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
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.blobcache.common.ByteBufferReference;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.core.Streams;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static org.elasticsearch.blobcache.shared.SharedBytes.MAX_BYTES_PER_WRITE;

public class SearchIndexInput extends StatelessBufferedIndexInput {

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
        return new SearchIndexInput(sliceDescription, cacheFile, context, blobContainer, cacheService, length, this.offset + offset);
    }

    private long getAbsolutePosition() {
        return getFilePointer() + offset;
    }

    @Override
    public void readFloats(float[] dst, int offset, int len) throws IOException {
        int remainingDst = len;
        while (remainingDst > 0) {
            int cnt = Math.min(buffer.remaining() / Float.BYTES, remainingDst);
            buffer.asFloatBuffer().get(dst, offset + len - remainingDst, cnt);
            buffer.position(buffer.position() + Float.BYTES * cnt);
            remainingDst -= cnt;
            if (remainingDst > 0) {
                if (buffer.hasRemaining()) {
                    dst[offset + len - remainingDst] = Float.intBitsToFloat(readInt());
                    --remainingDst;
                } else {
                    refill();
                }
            }
        }
    }

    @Override
    public void readLongs(long[] dst, int offset, int len) throws IOException {
        int remainingDst = len;
        while (remainingDst > 0) {
            int cnt = Math.min(buffer.remaining() / Long.BYTES, remainingDst);
            buffer.asLongBuffer().get(dst, offset + len - remainingDst, cnt);
            buffer.position(buffer.position() + Long.BYTES * cnt);
            remainingDst -= cnt;
            if (remainingDst > 0) {
                if (buffer.hasRemaining()) {
                    dst[offset + len - remainingDst] = readLong();
                    --remainingDst;
                } else {
                    refill();
                }
            }
        }
    }

    @Override
    public void readInts(int[] dst, int offset, int len) throws IOException {
        int remainingDst = len;
        while (remainingDst > 0) {
            int cnt = Math.min(buffer.remaining() / Integer.BYTES, remainingDst);
            buffer.asIntBuffer().get(dst, offset + len - remainingDst, cnt);
            buffer.position(buffer.position() + Integer.BYTES * cnt);
            remainingDst -= cnt;
            if (remainingDst > 0) {
                if (buffer.hasRemaining()) {
                    dst[offset + len - remainingDst] = readInt();
                    --remainingDst;
                } else {
                    refill();
                }
            }
        }
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
        logger.trace("readInternal: read [{}-{}] ([{}] bytes) from [{}]", position, position + length, length, this);
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
                bytesRead = cacheFile.populateAndRead(rangeToWrite, rangeToRead, (channel, pos, relativePos, len) -> {
                    logger.trace(
                        "{}: reading cached {} logical {} channel {} pos {} length {} (details: {})",
                        cacheFile.getCacheKey().fileName(),
                        false,
                        rangeToRead.start(),
                        pos,
                        relativePos,
                        length,
                        cacheFile
                    );
                    return SharedBytes.readCacheFile(channel, pos, relativePos, len, byteBufferReference, cacheFile);
                }, (channel, channelPos, relativePos, len, progressUpdater) -> {
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
                });
                byteBufferReference.finish(bytesRead);
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
        } finally {
            byteBufferReference.finish(0);
        }
    }

    private static final ThreadLocal<ByteBuffer> writeBuffer = ThreadLocal.withInitial(
        () -> ByteBuffer.allocateDirect(MAX_BYTES_PER_WRITE)
    );

}
