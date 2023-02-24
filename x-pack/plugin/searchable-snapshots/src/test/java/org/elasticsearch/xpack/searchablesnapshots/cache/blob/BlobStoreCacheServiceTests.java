/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache.blob;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_BLOB_CACHE_INDEX;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BlobStoreCacheServiceTests extends ESTestCase {

    private TestThreadPool threadPool;
    private Client mockClient;
    private String repository;
    private SnapshotId snapshotId;
    private IndexId indexId;
    private ShardId shardId;
    private String fileName;
    private ByteRange range;

    @Before
    public void createThreadPool() {
        mockClient = mock(Client.class);
        threadPool = new TestThreadPool(getClass().getSimpleName());
        when(mockClient.threadPool()).thenReturn(threadPool);
        repository = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        snapshotId = new SnapshotId(randomAlphaOfLength(5).toLowerCase(Locale.ROOT), UUIDs.randomBase64UUID());
        indexId = new IndexId(randomAlphaOfLength(5).toLowerCase(Locale.ROOT), UUIDs.randomBase64UUID());
        shardId = new ShardId(randomAlphaOfLength(5).toLowerCase(Locale.ROOT), UUIDs.randomBase64UUID(), randomInt(5));
        fileName = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        range = ByteRange.of(randomLongBetween(0L, 1024L), randomLongBetween(2048L, 4096L));
    }

    @After
    public void shutdownThreadPool() {
        threadPool.shutdown();
    }

    @SuppressWarnings("unchecked")
    public void testGetWhenServiceNotStarted() {
        doAnswer(invocation -> {
            final GetRequest request = (GetRequest) invocation.getArguments()[1];
            final ActionListener<GetResponse> listener = (ActionListener<GetResponse>) invocation.getArguments()[2];
            listener.onResponse(
                new GetResponse(
                    new GetResult(
                        request.index(),
                        request.id(),
                        UNASSIGNED_SEQ_NO,
                        UNASSIGNED_PRIMARY_TERM,
                        request.version(),
                        false,
                        BytesArray.EMPTY,
                        emptyMap(),
                        emptyMap()
                    )
                )
            );
            return null;
        }).when(mockClient).execute(eq(GetAction.INSTANCE), any(GetRequest.class), any(ActionListener.class));

        BlobStoreCacheService blobCacheService = new BlobStoreCacheService(null, mockClient, SNAPSHOT_BLOB_CACHE_INDEX);
        blobCacheService.start();

        PlainActionFuture<CachedBlob> future = PlainActionFuture.newFuture();
        blobCacheService.getAsync(repository, snapshotId, indexId, shardId, fileName, range, future);
        assertThat(future.actionGet(), equalTo(CachedBlob.CACHE_MISS));

        blobCacheService.stop();

        future = PlainActionFuture.newFuture();
        blobCacheService.getAsync(repository, snapshotId, indexId, shardId, fileName, range, future);
        assertThat(future.actionGet(), equalTo(CachedBlob.CACHE_NOT_READY));
    }

    @SuppressWarnings("unchecked")
    public void testPutWhenServiceNotStarted() {
        doAnswer(invocation -> {
            final IndexRequest request = (IndexRequest) invocation.getArguments()[1];
            final ActionListener<IndexResponse> listener = (ActionListener<IndexResponse>) invocation.getArguments()[2];
            listener.onResponse(
                new IndexResponse(
                    new ShardId(request.index(), "_uuid", 0),
                    request.id(),
                    UNASSIGNED_SEQ_NO,
                    UNASSIGNED_PRIMARY_TERM,
                    request.version(),
                    true
                )
            );
            return null;
        }).when(mockClient).execute(eq(IndexAction.INSTANCE), any(IndexRequest.class), any(ActionListener.class));

        BlobStoreCacheService blobCacheService = new BlobStoreCacheService(null, mockClient, SNAPSHOT_BLOB_CACHE_INDEX);
        blobCacheService.start();

        PlainActionFuture<Void> future = PlainActionFuture.newFuture();
        blobCacheService.putAsync(repository, snapshotId, indexId, shardId, fileName, range, BytesArray.EMPTY, 0L, future);
        assertThat(future.actionGet(), nullValue());

        blobCacheService.stop();

        future = PlainActionFuture.newFuture();
        blobCacheService.putAsync(repository, snapshotId, indexId, shardId, fileName, range, BytesArray.EMPTY, 0L, future);
        IllegalStateException exception = expectThrows(IllegalStateException.class, future::actionGet);
        assertThat(exception.getMessage(), containsString("Blob cache service is closed"));
    }

    @SuppressWarnings("unchecked")
    public void testWaitForInFlightCacheFillsToComplete() throws Exception {
        final int nbThreads = randomIntBetween(1, 5);
        final CountDownLatch latch = new CountDownLatch(1);

        doAnswer(invocation -> {
            final IndexRequest request = (IndexRequest) invocation.getArguments()[1];
            final ActionListener<IndexResponse> listener = (ActionListener<IndexResponse>) invocation.getArguments()[2];
            latch.await();
            Thread.sleep(randomLongBetween(100L, 3000L));
            listener.onResponse(
                new IndexResponse(
                    new ShardId(request.index(), "_uuid", 0),
                    request.id(),
                    UNASSIGNED_SEQ_NO,
                    UNASSIGNED_PRIMARY_TERM,
                    request.version(),
                    true
                )
            );
            return null;
        }).when(mockClient).execute(eq(IndexAction.INSTANCE), any(IndexRequest.class), any(ActionListener.class));

        final BlobStoreCacheService blobCacheService = new BlobStoreCacheService(null, mockClient, SNAPSHOT_BLOB_CACHE_INDEX);
        blobCacheService.start();

        assertThat(blobCacheService.getInFlightCacheFills(), equalTo(0));

        final List<PlainActionFuture<Void>> futures = new ArrayList<>(nbThreads);
        for (int i = 0; i < nbThreads; i++) {
            final PlainActionFuture<Void> future = PlainActionFuture.newFuture();
            threadPool.generic()
                .execute(
                    () -> blobCacheService.putAsync(repository, snapshotId, indexId, shardId, fileName, range, BytesArray.EMPTY, 0L, future)
                );
            futures.add(future);
        }

        assertBusy(() -> assertThat(blobCacheService.getInFlightCacheFills(), equalTo(nbThreads)));
        assertFalse(blobCacheService.waitForInFlightCacheFillsToComplete(0L, TimeUnit.SECONDS));
        assertTrue(futures.stream().noneMatch(Future::isDone));

        latch.countDown();

        assertTrue(blobCacheService.waitForInFlightCacheFillsToComplete(30L, TimeUnit.SECONDS));
        assertTrue(futures.stream().allMatch(Future::isDone));
    }
}
