/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch.chunk;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BytesTransportRequest;
import org.elasticsearch.transport.TransportResponseHandler;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class TransportFetchPhaseResponseChunkActionTests extends ESTestCase {

    private static final ShardId TEST_SHARD_ID = new ShardId(new Index("test-index", "test-uuid"), 0);

    private ThreadPool threadPool;
    private MockTransportService transportService;
    private ActiveFetchPhaseTasks activeFetchPhaseTasks;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        transportService = MockTransportService.createNewService(
            Settings.EMPTY,
            VersionInformation.CURRENT,
            TransportFetchPhaseCoordinationAction.CHUNKED_FETCH_DOC_ID_ORDER,
            threadPool
        );
        transportService.start();
        transportService.acceptIncomingRequests();

        activeFetchPhaseTasks = new ActiveFetchPhaseTasks();
        new TransportFetchPhaseResponseChunkAction(
            transportService,
            activeFetchPhaseTasks,
            new NamedWriteableRegistry(Collections.emptyList())
        );
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        if (transportService != null) {
            transportService.close();
        }
        if (threadPool != null) {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    public void testProcessChunkWhenWriteChunkThrowsSendsErrorAndReleasesChunk() throws Exception {
        final long coordinatingTaskId = 123L;
        AtomicReference<FetchPhaseResponseChunk> processedChunk = new AtomicReference<>();

        FetchPhaseResponseStream stream = new FetchPhaseResponseStream(0, 1, new NoopCircuitBreaker("test")) {
            @Override
            void writeChunk(FetchPhaseResponseChunk chunk, Releasable releasable) {
                processedChunk.set(chunk);
                try {
                    chunk.getHits();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                throw new IllegalStateException("simulated writeChunk failure");
            }
        };

        Releasable registration = activeFetchPhaseTasks.registerResponseBuilder(coordinatingTaskId, TEST_SHARD_ID, stream);
        SearchHit originalHit = createHit(7);
        FetchPhaseResponseChunk chunk = null;
        try {
            chunk = new FetchPhaseResponseChunk(TEST_SHARD_ID, serializeHits(originalHit), 1, 1, 0L);

            ReleasableBytesReference wireBytes = chunk.toReleasableBytesReference(coordinatingTaskId);
            PlainActionFuture<ActionResponse.Empty> future = new PlainActionFuture<>();

            transportService.sendRequest(
                transportService.getLocalNode(),
                TransportFetchPhaseResponseChunkAction.ZERO_COPY_ACTION_NAME,
                new BytesTransportRequest(wireBytes, TransportVersion.current()),
                new ActionListenerResponseHandler<>(future, in -> ActionResponse.Empty.INSTANCE, TransportResponseHandler.TRANSPORT_WORKER)
            );

            Exception e = expectThrows(Exception.class, () -> future.actionGet(10, TimeUnit.SECONDS));
            assertThat(e.getMessage(), containsString("simulated writeChunk failure"));

            assertBusy(() -> {
                FetchPhaseResponseChunk seen = processedChunk.get();
                assertNotNull("Chunk should have been processed before failure", seen);
                assertEquals("Chunk should be closed on failure", 0L, seen.getBytesLength());
            });
        } finally {
            if (chunk != null) {
                chunk.close();
            }
            registration.close();
            stream.decRef();
            originalHit.decRef();
        }
    }

    public void testProcessChunkSuccessWritesChunkAndReturnsAck() throws Exception {
        final long coordinatingTaskId = 321L;
        FetchPhaseResponseStream stream = new FetchPhaseResponseStream(0, 1, new NoopCircuitBreaker("test"));
        Releasable registration = activeFetchPhaseTasks.registerResponseBuilder(coordinatingTaskId, TEST_SHARD_ID, stream);
        SearchHit originalHit = createHit(9);
        FetchPhaseResponseChunk chunk = null;
        ReleasableBytesReference wireBytes = null;
        try {
            chunk = new FetchPhaseResponseChunk(TEST_SHARD_ID, serializeHits(originalHit), 1, 1, 0L);
            wireBytes = chunk.toReleasableBytesReference(coordinatingTaskId);

            PlainActionFuture<ActionResponse.Empty> future = sendChunk(wireBytes);
            assertSame(ActionResponse.Empty.INSTANCE, future.actionGet(10, TimeUnit.SECONDS));

            FetchSearchResult finalResult = stream.buildFinalResult(
                new ShardSearchContextId("ctx", 1L),
                new SearchShardTarget("node-0", TEST_SHARD_ID, null),
                null
            );
            try {
                SearchHit[] hits = finalResult.hits().getHits();
                assertThat(hits.length, equalTo(1));
                assertThat(hits[0].getSourceRef().utf8ToString(), containsString("\"id\":9"));
            } finally {
                finalResult.decRef();
            }
        } finally {
            if (wireBytes != null) {
                wireBytes.decRef();
            }
            if (chunk != null) {
                chunk.close();
            }
            registration.close();
            stream.decRef();
            originalHit.decRef();
        }
    }

    public void testProcessChunkForUnknownTaskReturnsResourceNotFound() throws Exception {
        final long unknownTaskId = randomLongBetween(10_000L, 20_000L);
        SearchHit originalHit = createHit(1);
        FetchPhaseResponseChunk chunk = null;
        ReleasableBytesReference wireBytes = null;
        try {
            chunk = new FetchPhaseResponseChunk(TEST_SHARD_ID, serializeHits(originalHit), 1, 1, 0L);
            wireBytes = chunk.toReleasableBytesReference(unknownTaskId);

            PlainActionFuture<ActionResponse.Empty> future = sendChunk(wireBytes);
            Exception e = expectThrows(Exception.class, () -> future.actionGet(10, TimeUnit.SECONDS));
            assertThat(e.getMessage(), containsString("fetch task [" + unknownTaskId + "] not found"));
        } finally {
            if (wireBytes != null) {
                wireBytes.decRef();
            }
            if (chunk != null) {
                chunk.close();
            }
            originalHit.decRef();
        }
    }

    public void testProcessChunkForLateChunkReturnsResourceNotFound() throws Exception {
        final long coordinatingTaskId = 777L;
        FetchPhaseResponseStream stream = new FetchPhaseResponseStream(0, 1, new NoopCircuitBreaker("test"));
        Releasable registration = activeFetchPhaseTasks.registerResponseBuilder(coordinatingTaskId, TEST_SHARD_ID, stream);

        registration.close();
        stream.decRef();

        SearchHit originalHit = createHit(3);
        FetchPhaseResponseChunk chunk = null;
        ReleasableBytesReference wireBytes = null;
        try {
            chunk = new FetchPhaseResponseChunk(TEST_SHARD_ID, serializeHits(originalHit), 1, 1, 0L);
            wireBytes = chunk.toReleasableBytesReference(coordinatingTaskId);

            PlainActionFuture<ActionResponse.Empty> future = sendChunk(wireBytes);
            Exception e = expectThrows(Exception.class, () -> future.actionGet(10, TimeUnit.SECONDS));
            assertThat(e.getMessage(), containsString("fetch task [" + coordinatingTaskId + "] not found"));
        } finally {
            if (wireBytes != null) {
                wireBytes.decRef();
            }
            if (chunk != null) {
                chunk.close();
            }
            originalHit.decRef();
        }
    }

    public void testProcessChunkTracksAndReleasesCircuitBreakerBytes() throws Exception {
        final long coordinatingTaskId = 222L;
        var breaker = newLimitedBreaker(ByteSizeValue.ofBytes(Long.MAX_VALUE));
        FetchPhaseResponseStream stream = new FetchPhaseResponseStream(0, 1, breaker);
        Releasable registration = activeFetchPhaseTasks.registerResponseBuilder(coordinatingTaskId, TEST_SHARD_ID, stream);
        SearchHit originalHit = createHit(12);
        FetchPhaseResponseChunk chunk = null;
        ReleasableBytesReference wireBytes = null;
        try {
            chunk = new FetchPhaseResponseChunk(TEST_SHARD_ID, serializeHits(originalHit), 1, 1, 0L);
            long expectedBytes = chunk.getBytesLength();
            wireBytes = chunk.toReleasableBytesReference(coordinatingTaskId);

            PlainActionFuture<ActionResponse.Empty> future = sendChunk(wireBytes);
            future.actionGet(10, TimeUnit.SECONDS);
            assertThat(breaker.getUsed(), equalTo(expectedBytes));
        } finally {
            if (wireBytes != null) {
                wireBytes.decRef();
            }
            if (chunk != null) {
                chunk.close();
            }
            registration.close();
            stream.decRef();
            originalHit.decRef();
        }

        assertThat("breaker bytes should be released when stream is closed", breaker.getUsed(), equalTo(0L));
    }

    private SearchHit createHit(int id) {
        SearchHit hit = new SearchHit(id);
        hit.sourceRef(new BytesArray("{\"id\":" + id + "}"));
        return hit;
    }

    private PlainActionFuture<ActionResponse.Empty> sendChunk(ReleasableBytesReference wireBytes) {
        PlainActionFuture<ActionResponse.Empty> future = new PlainActionFuture<>();
        transportService.sendRequest(
            transportService.getLocalNode(),
            TransportFetchPhaseResponseChunkAction.ZERO_COPY_ACTION_NAME,
            new BytesTransportRequest(wireBytes, TransportVersion.current()),
            new ActionListenerResponseHandler<>(future, in -> ActionResponse.Empty.INSTANCE, TransportResponseHandler.TRANSPORT_WORKER)
        );
        return future;
    }

    private BytesReference serializeHits(SearchHit... hits) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            for (int i = 0; i < hits.length; i++) {
                out.writeVInt(i);
                hits[i].writeTo(out);
            }
            return out.bytes();
        }
    }
}
