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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.common.SparseFileTracker;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.common.io.stream.CountingFilterInputStream;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntConsumer;
import java.util.function.Supplier;

import static org.elasticsearch.common.io.Streams.limitStream;

/**
 * A RangeMissingHandler that fills a sorted list of gaps sequentially from a shared {@link SharedBlobCacheService.SourceInputStreamFactory}
 */
public class SequentialRangeMissingHandler implements SharedBlobCacheService.RangeMissingHandler {

    private static final Logger logger = LogManager.getLogger(SequentialRangeMissingHandler.class);

    private final Object initiator;
    private final String blobFileName;
    private final ByteRange rangeToWrite;
    private final CacheBlobReader cacheBlobReader;
    private final Supplier<ByteBuffer> writeBufferSupplier;
    private final IntConsumer bytesCopiedConsumer;
    private final String[] expectedThreadPoolNames;

    /**
     * @param initiator               the caller - used for debug logging
     * @param blobFileName            the blob file name read from
     * @param rangeToWrite            the range to read into the cache, see also {@link CacheBlobReader#getRange}
     * @param cacheBlobReader         source reader which is used to fill the range
     * @param writeBufferSupplier     returns byte buffer which is used for copying data from blob reader to cache.
     *                                Be aware of threaded usage of writeBufferSupplier. Underlying buffer is used exclusively in
     *                                a gap filling thread, so it should not be shared with other modifying threads.
     * @param bytesCopiedConsumer     a consumer to be called everytime some bytes are copied to the cache
     * @param expectedThreadPoolNames lists threads which can be used to fill the range
     */
    public SequentialRangeMissingHandler(
        Object initiator,
        String blobFileName,
        ByteRange rangeToWrite,
        CacheBlobReader cacheBlobReader,
        Supplier<ByteBuffer> writeBufferSupplier,
        IntConsumer bytesCopiedConsumer,
        String... expectedThreadPoolNames
    ) {
        this.initiator = initiator;
        this.blobFileName = blobFileName;
        this.rangeToWrite = rangeToWrite;
        this.cacheBlobReader = cacheBlobReader;
        this.writeBufferSupplier = writeBufferSupplier;
        this.bytesCopiedConsumer = bytesCopiedConsumer;
        this.expectedThreadPoolNames = expectedThreadPoolNames;
    }

    @Override
    @Nullable
    public SharedBlobCacheService.SourceInputStreamFactory sharedInputStreamFactory(List<SparseFileTracker.Gap> gaps) {
        assert gaps.isEmpty() == false;
        assert gaps.equals(gaps.stream().sorted(Comparator.comparingLong(SparseFileTracker.Gap::start)).toList())
            : "gaps not sorted: " + gaps;

        final var numberGaps = gaps.size();
        if (numberGaps == 1) {
            return null; // simple case for filling a single gap
        }

        final var totalGapLength = Math.toIntExact(gaps.get(numberGaps - 1).end() - gaps.get(0).start());
        return new SharedBlobCacheService.SourceInputStreamFactory() {
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
                        logger.trace(Strings.format("%s advanced currentRelativePos from %s to %s", this, currentRelativePos, relativePos));
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
        @Nullable SharedBlobCacheService.SourceInputStreamFactory streamFactory,
        int relativePos,
        int len,
        IntConsumer progressUpdater,
        ActionListener<Void> completionListener
    ) throws IOException {
        createInputStream(streamFactory, relativePos, len, completionListener.map(in -> {
            try (in) {
                assert ThreadPool.assertCurrentThreadPool(expectedThreadPoolNames);
                int bytesCopied = SharedBytes.copyToCacheFileAligned(channel, in, channelPos, progressUpdater, writeBufferSupplier.get());
                bytesCopiedConsumer.accept(bytesCopied);
                return null;
            }
        }));
    }

    private void createInputStream(
        SharedBlobCacheService.SourceInputStreamFactory streamFactory,
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

    private void inputStreamFromCacheBlobReader(long streamStartPosition, int len, ActionListener<InputStream> listener) {
        // this length is computed from the rangeToWrite and the sum of "streamStartPosition + len" can exceed real
        // length of the blob
        logger.debug(
            "{}: loading [{}][{}-{}] from [{}]",
            initiator.toString(),
            blobFileName,
            streamStartPosition,
            streamStartPosition + len,
            cacheBlobReader.getClass().getSimpleName()
        );
        cacheBlobReader.getRangeInputStream(streamStartPosition, len, listener);
    }
}
