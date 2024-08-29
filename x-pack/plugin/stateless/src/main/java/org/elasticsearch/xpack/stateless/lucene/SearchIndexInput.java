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
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyUploadedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.blobcache.common.BlobCacheBufferedIndexInput;
import org.elasticsearch.blobcache.common.ByteBufferReference;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.common.SparseFileTracker;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService.RangeMissingHandler;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService.SourceInputStreamFactory;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.common.io.stream.CountingFilterInputStream;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Streams;
import org.elasticsearch.core.Strings;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntConsumer;
import java.util.function.Supplier;

import static org.elasticsearch.blobcache.shared.SharedBytes.MAX_BYTES_PER_WRITE;
import static org.elasticsearch.common.io.Streams.limitStream;

public final class SearchIndexInput extends BlobCacheBufferedIndexInput {

    private static final Logger logger = LogManager.getLogger(SearchIndexInput.class);

    private final StatelessSharedBlobCacheService.CacheFile cacheFile;
    private final CacheBlobReader cacheBlobReader;
    private final AtomicBoolean closed;
    private final Releasable releasable;
    private final IOContext context;
    private final long offset;

    public SearchIndexInput(
        String name,
        StatelessSharedBlobCacheService.CacheFile cacheFile,
        IOContext context,
        CacheBlobReader cacheBlobReader,
        Releasable releasable,
        long length,
        long offset
    ) {
        super(name, context, length);
        this.cacheFile = cacheFile.copy();
        this.cacheBlobReader = cacheBlobReader;
        this.closed = new AtomicBoolean(false);
        this.releasable = releasable;
        this.context = context;
        this.offset = offset;
    }

    @Override
    protected void seekInternal(long pos) throws IOException {
        BlobCacheUtils.ensureSeek(pos, this);
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            Releasables.close(releasable);
        }
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) {
        BlobCacheUtils.ensureSlice(sliceDescription, offset, length, this);
        var arraySlice = trySliceBuffer(sliceDescription, offset, length);
        if (arraySlice != null) {
            return arraySlice;
        }
        return doSlice(sliceDescription, offset, length);
    }

    IndexInput doSlice(String sliceDescription, long offset, long length) {
        return new SearchIndexInput(
            "(" + sliceDescription + ") " + super.toString(),
            cacheFile,
            context,
            cacheBlobReader,
            null,
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
        var clone = new SearchIndexInput(super.toString(), cacheFile, context, cacheBlobReader, null, length(), offset);
        try {
            clone.seek(getFilePointer());
        } catch (IOException e) {
            assert false : e;
            throw new UncheckedIOException(e);
        }
        return clone;
    }

    private long getAbsolutePosition() {
        return getFilePointer() + offset;
    }

    @Override
    protected void readInternal(ByteBuffer b) throws IOException {
        try {
            doReadInternal(b);
        } catch (IOException | RuntimeException e) {
            if (ExceptionsHelper.unwrap(e, FileNotFoundException.class, NoSuchFileException.class) != null) {
                logger.warn(() -> this + " did not find file", e); // includes the file name of the SearchIndexInput
            }
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
        int positionBeforeRetry = b.position();
        int limitBeforeRetry = b.limit();
        try {
            readInternalSlow(b, position, length);
        } catch (Exception ex) {
            if (ExceptionsHelper.unwrap(ex, ResourceAlreadyUploadedException.class) != null) {
                logger.debug(() -> this + " retrying from object store", ex);
                assert b.position() == positionBeforeRetry : b.position() + " != " + positionBeforeRetry;
                assert b.limit() == limitBeforeRetry : b.limit() + " != " + limitBeforeRetry;
                readInternalSlow(b, position, length);
                return;
            }
            throw ex;
        }
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
                        SearchIndexInput.this.toString(),
                        cacheFile.getCacheKey().fileName(),
                        rangeToRead.start(),
                        rangeToRead.start() + len
                    );
                    return SharedBytes.readCacheFile(channel, channelPos, relativePos, len, byteBufferReference);
                },
                    // Can be executed on different thread pool depending on whether we read from
                    // the ObjectStoreCacheBlobReader (SHARD_READ pool) or the IndexingShardCacheBlobReader (VBCC pool)
                    new SequentialRangeMissingHandler(
                        rangeToWrite,
                        cacheBlobReader,
                        () -> writeBuffer.get().clear(),
                        bytesCopied -> {},
                        Stateless.SHARD_READ_THREAD_POOL,
                        Stateless.FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL
                    ) {
                        @Override
                        void inputStreamFromCacheBlobReader(long streamStartPosition, int len, ActionListener<InputStream> listener) {
                            // this length is computed from the rangeToWrite and the sum of "streamStartPosition + len" can real
                            // length of the blob
                            logger.debug(
                                "{}: loading [{}][{}-{}] from [{}]",
                                SearchIndexInput.this.toString(),
                                cacheFile.getCacheKey().fileName(),
                                streamStartPosition,
                                streamStartPosition + len,
                                cacheBlobReader.getClass().getSimpleName()
                            );
                            super.inputStreamFromCacheBlobReader(streamStartPosition, len, listener);
                        }
                    }
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

    /**
     * A RangeMissingHandler that fills a sorted list of gaps sequentially from a shared {@link SourceInputStreamFactory}.
     */
    public static class SequentialRangeMissingHandler implements RangeMissingHandler {

        private static final Logger logger = LogManager.getLogger(SequentialRangeMissingHandler.class);

        private final ByteRange rangeToWrite;
        private final CacheBlobReader cacheBlobReader;
        private final Supplier<ByteBuffer> writeBufferSupplier;
        private final IntConsumer bytesCopiedConsumer;
        private final String[] expectedThreadPoolNames;

        /**
         * @param rangeToWrite the range to read into the cache, see also {@link CacheBlobReader#getRange}
         * @param cacheBlobReader source reader which is used to fill the range
         * @param writeBufferSupplier returns byte buffer which is used for copying data from blob reader to cache.
         *                            Be aware of threaded usage of writeBufferSupplier. Underlying buffer is used exclusively in
         *                            a gap filling thread, so it should not be shared with other modifying threads.
         * @param expectedThreadPoolNames lists threads which can be used to fill the range
         */
        public SequentialRangeMissingHandler(
            ByteRange rangeToWrite,
            CacheBlobReader cacheBlobReader,
            Supplier<ByteBuffer> writeBufferSupplier,
            IntConsumer bytesCopiedConsumer,
            String... expectedThreadPoolNames
        ) {
            this.rangeToWrite = rangeToWrite;
            this.cacheBlobReader = cacheBlobReader;
            this.writeBufferSupplier = writeBufferSupplier;
            this.bytesCopiedConsumer = bytesCopiedConsumer;
            this.expectedThreadPoolNames = expectedThreadPoolNames;
        }

        @Override
        @Nullable
        public SourceInputStreamFactory sharedInputStreamFactory(List<SparseFileTracker.Gap> gaps) {
            assert gaps.isEmpty() == false;
            assert gaps.equals(gaps.stream().sorted(Comparator.comparingLong(SparseFileTracker.Gap::start)).toList())
                : "gaps not sorted: " + gaps;

            final var numberGaps = gaps.size();
            if (numberGaps == 1) {
                return null; // simple case for filling a single gap
            }

            final var totalGapLength = Math.toIntExact(gaps.get(numberGaps - 1).end() - gaps.get(0).start());
            return new SourceInputStreamFactory() {
                // No volatile or synchronization is needed since these fields are accessed in the same thread
                private InputStream in;
                private int currentRelativePos = 0;
                private final AtomicInteger invocationCount = new AtomicInteger(0);
                private final AtomicReference<Thread> invocationThread = Assertions.ENABLED ? new AtomicReference<>() : null;

                private final SubscribableListener<InputStream> firstListener = new SubscribableListener<>();
                private SubscribableListener<InputStream> chainedListeners = firstListener;

                @Override
                public void create(int relativePos, ActionListener<InputStream> listener) throws IOException {
                    var invocationCount = this.invocationCount.incrementAndGet();
                    assert invocationCount <= numberGaps : invocationCount + " > " + numberGaps;
                    if (invocationCount == 1) {
                        currentRelativePos = relativePos;
                    }
                    chainedListeners = chainedListeners.<InputStream>andThenApply(in -> {
                        assert invocationThread.get() == Thread.currentThread() : invocationThread.get() + " != " + Thread.currentThread();
                        if (currentRelativePos != relativePos) {
                            assert currentRelativePos < relativePos : currentRelativePos + " > " + relativePos;
                            try {
                                in.skipNBytes(relativePos - currentRelativePos); // skip over any already filled range
                            } catch (EOFException e) {
                                // It is possible that the source input stream has less data than what a cache region can store. In this
                                // case, we return a zero-length input stream which allows the gap to be completed.
                                logger.trace(
                                    () -> Strings.format(
                                        "%s encountered EOF trying to advance currentRelativePos from %s to %s",
                                        this,
                                        currentRelativePos,
                                        relativePos
                                    )
                                );
                                assert invocationCount == numberGaps : invocationCount + " != " + numberGaps;
                                return InputStream.nullInputStream();
                            }
                            logger.trace(
                                Strings.format("%s advanced currentRelativePos from %s to %s", this, currentRelativePos, relativePos)
                            );
                            currentRelativePos = relativePos;
                        }
                        return in;
                    }).andThenApply(this::bytesCountingFilterInputStream);
                    chainedListeners.addListener(listener);

                    if (invocationCount == numberGaps) {
                        ActionListener<InputStream> triggerChainOfListeners = firstListener.map(inputStream -> {
                            in = inputStream;
                            assert assertCompareAndSetInvocationThread(null, Thread.currentThread());
                            return inputStream;
                        });
                        ActionListener.run(triggerChainOfListeners, (l) -> {
                            inputStreamFromCacheBlobReader(rangeToWrite.start() + currentRelativePos, totalGapLength, l);
                        });
                    }
                }

                private InputStream bytesCountingFilterInputStream(InputStream in) {
                    return new CountingFilterInputStream(in) {
                        @Override
                        public void close() {
                            currentRelativePos += getBytesRead();
                        }

                        @Override
                        protected boolean assertInvariant() {
                            // Can be executed on different thread pool depending whether we read from
                            assert ThreadPool.assertCurrentThreadPool(expectedThreadPoolNames);
                            return super.assertInvariant();
                        }
                    };
                }

                @Override
                public void close() {
                    IOUtils.closeWhileHandlingException(in);
                    logger.trace(() -> Strings.format("closed %s", this));
                }

                private boolean assertCompareAndSetInvocationThread(Thread current, Thread updated) {
                    final Thread witness = invocationThread.compareAndExchange(current, updated);
                    assert witness == current
                        : "Unable to set invocation thread to ["
                            + updated
                            + "]: expected thread ["
                            + current
                            + "] but got thread ["
                            + witness
                            + "]";
                    return true;
                }
            };
        }

        @Override
        public void fillCacheRange(
            SharedBytes.IO channel,
            int channelPos,
            @Nullable SourceInputStreamFactory streamFactory,
            int relativePos,
            int len,
            IntConsumer progressUpdater,
            ActionListener<Void> completionListener
        ) throws IOException {
            createInputStream(streamFactory, relativePos, len, completionListener.map(in -> {
                try (in) {
                    assert ThreadPool.assertCurrentThreadPool(expectedThreadPoolNames);
                    int bytesCopied = SharedBytes.copyToCacheFileAligned(
                        channel,
                        in,
                        channelPos,
                        progressUpdater,
                        writeBufferSupplier.get()
                    );
                    bytesCopiedConsumer.accept(bytesCopied);
                    return null;
                }
            }));
        }

        private void createInputStream(
            SourceInputStreamFactory streamFactory,
            int relativePos,
            int len,
            ActionListener<InputStream> listener
        ) throws IOException {
            if (streamFactory == null) {
                inputStreamFromCacheBlobReader(rangeToWrite.start() + relativePos, len, listener);
            } else {
                streamFactory.create(relativePos, listener.map(is -> limitStream(is, len)));
            }
        }

        void inputStreamFromCacheBlobReader(long streamStartPosition, int len, ActionListener<InputStream> listener) {
            cacheBlobReader.getRangeInputStream(streamStartPosition, len, listener);
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
