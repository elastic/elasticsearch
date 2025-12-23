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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.common.SparseFileTracker;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.StatelessPlugin;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;

import static org.elasticsearch.blobcache.shared.SharedBytes.PAGE_SIZE;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LazyRangeMissingHandlerTests extends ESTestCase {

    public void testSequentialRangeMissingHandlerPositionTracking() throws IOException {
        final CacheBlobReader cacheBlobReader = mock(CacheBlobReader.class);
        final long rangeToWriteStart = randomLongBetween(0, 1000);
        final long rangeToWriteEnd = rangeToWriteStart + PAGE_SIZE * between(20, 40);
        final ByteRange rangeToWrite = ByteRange.of(rangeToWriteStart, rangeToWriteEnd);

        AtomicInteger fillCacheRangeCalls = new AtomicInteger(0);
        LazyRangeMissingHandler<SequentialRangeMissingHandler> lazyHandler = new LazyRangeMissingHandler<>(
            () -> new SequentialRangeMissingHandler(
                "__test__",
                randomAlphaOfLength(50),
                rangeToWrite,
                cacheBlobReader,
                () -> null,
                copiedBytes -> {},
                StatelessPlugin.SHARD_READ_THREAD_POOL
            ) {
                @Override
                public void fillCacheRange(
                    SharedBytes.IO channel,
                    int channelPos,
                    SharedBlobCacheService.SourceInputStreamFactory streamFactory,
                    int relativePos,
                    int len,
                    IntConsumer progressUpdater,
                    ActionListener<Void> completionListener
                ) throws IOException {
                    fillCacheRangeCalls.incrementAndGet();
                }
            }
        );

        // the delegate is null before sharedInputStreamFactory is called
        assertThat(lazyHandler.delegate(), is(nullValue()));

        int start = randomNonNegativeInt();
        lazyHandler.sharedInputStreamFactory(List.of(mockGap(start, start + randomNonNegativeInt())));

        SequentialRangeMissingHandler delegateInstance = lazyHandler.delegate();
        assertThat(delegateInstance, is(notNullValue()));

        // calling sharedInputStreamFactory again should not create a new delegate instance
        SharedBlobCacheService.SourceInputStreamFactory sourceInputStreamFactory = lazyHandler.sharedInputStreamFactory(
            List.of(mockGap(start, start + randomNonNegativeInt()))
        );
        SequentialRangeMissingHandler delegateInstanceSecondCall = lazyHandler.delegate();
        assertThat(delegateInstanceSecondCall, is(delegateInstance));

        // fillCacheRange calls are delegated
        lazyHandler.fillCacheRange(null, 0, sourceInputStreamFactory, 0, 1, i -> {}, ActionListener.noop());
        assertThat(fillCacheRangeCalls.get(), is(1));
    }

    private SparseFileTracker.Gap mockGap(long start, long end) {
        final SparseFileTracker.Gap gap = mock(SparseFileTracker.Gap.class);
        when(gap.start()).thenReturn(start);
        when(gap.end()).thenReturn(end);
        return gap;
    }
}
