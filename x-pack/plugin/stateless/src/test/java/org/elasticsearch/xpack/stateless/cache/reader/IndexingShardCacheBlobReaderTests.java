/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.reader;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.blobcache.common.SparseFileTracker;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;
import org.hamcrest.Matchers;
import org.junit.After;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexingShardCacheBlobReaderTests extends ESTestCase {

    private final ThreadPool threadPool = new TestThreadPool(
        getClass().getName(),
        StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, false)
    );

    @After
    public void stop() throws Exception {
        threadPool.shutdown();
    }

    public void testGetRangeInputStreamUsesDedicatedExecutorOnFailurePath() {
        final var indexingShardCacheBlobReader = new IndexingShardCacheBlobReader(
            new ShardId(new Index(randomIdentifier(), randomUUID()), randomNonNegativeInt()),
            new PrimaryTermAndGeneration(randomNonNegativeLong(), randomNonNegativeLong()),
            randomIdentifier(),
            new NoOpNodeClient(threadPool) {
                @Override
                public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                    ActionType<Response> action,
                    Request request,
                    ActionListener<Response> listener
                ) {
                    listener.onFailure(new RuntimeException("simulated"));
                }
            },
            ByteSizeValue.ofBytes(1024),
            threadPool
        );

        final Thread callerThread = Thread.currentThread();

        safeAwaitFailure(
            InputStream.class,
            l -> indexingShardCacheBlobReader.getRangeInputStream(
                randomNonNegativeLong(),
                randomNonNegativeInt(),
                l.delegateResponse((ll, e) -> {
                    final Thread completingThread = Thread.currentThread();
                    assertNotSame(callerThread, completingThread);
                    assertThat(
                        completingThread.getName(),
                        EsExecutors.executorName(completingThread),
                        equalTo(StatelessPlugin.FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL)
                    );
                    ll.onFailure(e);
                })
            )
        );
    }

    public void testChunkRounding() {
        ByteSizeValue chunkSizeValue = ByteSizeValue.ofMb(128);
        int chunkSize = (int) chunkSizeValue.getBytes();
        IndexingShardCacheBlobReader reader = new IndexingShardCacheBlobReader(
            new ShardId(randomAlphaOfLength(10), randomUUID(), between(0, 10)),
            new PrimaryTermAndGeneration(between(0, 10), between(1, 10)),
            "_na_",
            null,
            chunkSizeValue,
            threadPool
        );

        int small = between(1, SharedBytes.PAGE_SIZE);
        verify(reader, 0, between(1, small), small, 0, SharedBytes.PAGE_SIZE);
        verify(reader, between(1, SharedBytes.PAGE_SIZE - 1), 1, 1, 0, SharedBytes.PAGE_SIZE);
        verify(reader, 1, between(1, SharedBytes.PAGE_SIZE - 1), SharedBytes.PAGE_SIZE - 1, 0, SharedBytes.PAGE_SIZE);
        verify(
            reader,
            between(1, SharedBytes.PAGE_SIZE),
            between(1, SharedBytes.PAGE_SIZE),
            SharedBytes.PAGE_SIZE,
            0,
            SharedBytes.PAGE_SIZE * 2L
        );

        long startChunk = randomLongBetween(0, 1000) * chunkSize;
        int chunkOffset = between(1, chunkSize - 1);
        verify(reader, startChunk, between(1, small), small, startChunk, startChunk + SharedBytes.PAGE_SIZE);
        verify(reader, startChunk, 1, between(chunkSize - SharedBytes.PAGE_SIZE + 1, chunkSize * 2), startChunk, startChunk + chunkSize);
        verify(reader, startChunk, chunkSize + chunkOffset, chunkSize * 10L, startChunk, startChunk + chunkSize * 2L);
        verify(reader, startChunk + chunkOffset, chunkSize, chunkSize * 10L, startChunk, startChunk + chunkSize * 2L);
        verify(reader, startChunk + chunkSize - 1, 1, chunkSize * 10L, startChunk, startChunk + chunkSize);

        verify(reader, startChunk, chunkSize + small, chunkSize + small, startChunk, startChunk + chunkSize + SharedBytes.PAGE_SIZE);

        long large = randomLongBetween(chunkSize * 2L, Long.MAX_VALUE - SharedBytes.PAGE_SIZE);
        verify(reader, startChunk, chunkSize + small, large, startChunk, startChunk + chunkSize * 2L);
    }

    public void testMultiGapSharedStreamTriggersClosedAssertion() throws IOException {
        final var directThreadPool = new ThreadPool() {
            @Override
            public ExecutorService executor(String name) {
                return EsExecutors.DIRECT_EXECUTOR_SERVICE;
            }

            @Override
            public ExecutorService generic() {
                return EsExecutors.DIRECT_EXECUTOR_SERVICE;
            }
        };
        final int pageSize = SharedBytes.PAGE_SIZE;
        final long secondGapStart = pageSize * 2L;
        final long rangeEnd = secondGapStart + pageSize;
        final var reader = new IndexingShardCacheBlobReader(
            new ShardId(new Index(randomIdentifier(), randomUUID()), randomNonNegativeInt()),
            new PrimaryTermAndGeneration(randomNonNegativeLong(), randomNonNegativeLong()),
            randomIdentifier(),
            null,
            ByteSizeValue.ofBytes(pageSize),
            directThreadPool
        ) {
            @Override
            protected void getVirtualBatchedCompoundCommitChunk(
                PrimaryTermAndGeneration virtualBccTermAndGen,
                long offset,
                int length,
                String preferredNodeId,
                ActionListener<ReleasableBytesReference> listener
            ) {
                listener.onResponse(ReleasableBytesReference.wrap(new BytesArray(randomByteArrayOfLength(length))));
            }
        };
        final var sequentialRangeMissingHandler = new SequentialRangeMissingHandler(
            "__test__",
            "__blob__",
            ByteRange.of(0L, rangeEnd),
            reader,
            () -> null,
            copiedBytes -> {},
            StatelessPlugin.FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL
        );
        final List<SparseFileTracker.Gap> gaps = List.of(mockGap(0L, pageSize), mockGap(secondGapStart, rangeEnd));
        try (var streamFactory = sequentialRangeMissingHandler.sharedInputStreamFactory(gaps)) {
            assertThat(streamFactory, notNullValue());
            ActionListener<InputStream> closingListener = ActionListener.wrap(in -> {
                try (in) {
                    assertNotEquals(-1, in.read());
                }
            }, e -> fail("unexpected failure"));
            streamFactory.create(0, closingListener);
            streamFactory.create(Math.toIntExact(secondGapStart), closingListener);
        }
    }

    private static void verify(
        IndexingShardCacheBlobReader reader,
        long position,
        int length,
        long remainingFileLength,
        long expectedStart,
        long expectedEnd
    ) {
        ByteRange range = reader.getRange(position, length, remainingFileLength);
        assertThat(range, Matchers.equalTo(ByteRange.of(expectedStart, expectedEnd)));
    }

    private static SparseFileTracker.Gap mockGap(long start, long end) {
        final SparseFileTracker.Gap gap = mock(SparseFileTracker.Gap.class);
        when(gap.start()).thenReturn(start);
        when(gap.end()).thenReturn(end);
        return gap;
    }
}
