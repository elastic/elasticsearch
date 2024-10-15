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
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.blobcache.common.ByteRange;
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
import org.hamcrest.Matchers;
import org.junit.After;

import java.io.InputStream;

import static org.hamcrest.Matchers.equalTo;

public class IndexingShardCacheBlobReaderTests extends ESTestCase {

    private final ThreadPool threadPool = new TestThreadPool(
        getClass().getName(),
        Stateless.statelessExecutorBuilders(Settings.EMPTY, false)
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
                        equalTo(Stateless.FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL)
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
}
