/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobstore.cache;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.CountDownLatch;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants.SNAPSHOT_BLOB_CACHE_INDEX;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BlobStoreCacheServiceTests extends ESTestCase {

    public void testGetWhenServiceNotStarted() {
        BlobStoreCacheService blobCacheService = new BlobStoreCacheService(null, mockClient(), SNAPSHOT_BLOB_CACHE_INDEX, () -> 0L);
        blobCacheService.start();

        PlainActionFuture<CachedBlob> future = PlainActionFuture.newFuture();
        blobCacheService.getAsync("_repository", "_file", "/path", 0L, future);
        assertThat(future.actionGet(), equalTo(CachedBlob.CACHE_MISS));

        blobCacheService.stop();

        future = PlainActionFuture.newFuture();
        blobCacheService.getAsync("_repository", "_file", "/path", 0L, future);
        assertThat(future.actionGet(), equalTo(CachedBlob.CACHE_NOT_READY));
    }

    public void testPutWhenServiceNotStarted() {
        BlobStoreCacheService blobCacheService = new BlobStoreCacheService(null, mockClient(), SNAPSHOT_BLOB_CACHE_INDEX, () -> 0L);
        blobCacheService.start();

        PlainActionFuture<Void> future = PlainActionFuture.newFuture();
        blobCacheService.putAsync("_repository", "_file", "/path", 0L, BytesArray.EMPTY, future);
        assertThat(future.actionGet(), nullValue());

        blobCacheService.stop();

        future = PlainActionFuture.newFuture();
        blobCacheService.putAsync("_repository", "_file", "/path", 0L, BytesArray.EMPTY, future);
        IllegalStateException exception = expectThrows(IllegalStateException.class, future::actionGet);
        assertThat(exception.getMessage(), containsString("Blob cache service is closed"));
    }

    public void testConcurrentPutWhenServiceIsStopping() throws Exception {
        BlobStoreCacheService blobCacheService = new BlobStoreCacheService(null, mockClient(), SNAPSHOT_BLOB_CACHE_INDEX, () -> 0L);
        blobCacheService.start();

        final Thread[] threads = new Thread[randomIntBetween(1, 10)];
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch end = new CountDownLatch(threads.length);

        for (int i = 0; i < threads.length; i++) {
            final int threadId = i;
            if (threadId < threads.length - 1) {
                threads[threadId] = new Thread(() -> {
                    try {
                        start.await();
                        final PlainActionFuture<Void> future = PlainActionFuture.newFuture();
                        blobCacheService.putAsync("_repository", String.valueOf(threadId), "/path", 0L, BytesArray.EMPTY, future);
                        future.actionGet();
                    } catch (IllegalStateException e) {
                        assertThat(e.getMessage(), containsString("Blob cache service is closed"));
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    } finally {
                        end.countDown();
                    }
                });
            } else {
                threads[threadId] = new Thread(() -> {
                    try {
                        start.await();
                        blobCacheService.stop();
                        assertThat(blobCacheService.lifecycleState(), equalTo(Lifecycle.State.STOPPED));
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    } finally {
                        end.countDown();
                    }
                });
            }
            threads[threadId].start();
        }
        start.countDown();
        end.await();

        assertThat(blobCacheService.lifecycleState(), equalTo(Lifecycle.State.STOPPED));
    }

    @SuppressWarnings("unchecked")
    private Client mockClient() {
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        final Client client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);

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
        }).when(client).execute(eq(GetAction.INSTANCE), any(GetRequest.class), any(ActionListener.class));

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
        }).when(client).execute(eq(IndexAction.INSTANCE), any(IndexRequest.class), any(ActionListener.class));
        return client;
    }
}
