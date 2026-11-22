/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.BackoffPolicy;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Retry keeps the coordinating indexing pressure flag with the bulk it resends, and a mixed result does not resend anything.
 */
public class RetryIndexingPressureTests extends ESTestCase {

    /**
     * A whole request rejection is retried as the same bulk, so a flag set by the coordinator is still set when the retry is sent.
     */
    public void testWholeRequestRejectionRetriesTheSameAccountedBulk() {
        DeterministicTaskQueue queue = new DeterministicTaskQueue();
        BulkRequest original = bulk(randomIntBetween(1, 5));
        original.indexingPressureAccounted(true);
        List<BulkRequest> seen = new ArrayList<>();
        AtomicInteger attempts = new AtomicInteger();
        PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();

        new Retry(backoff(1), queue.getThreadPool()).withBackoff((request, listener) -> {
            seen.add(request);
            if (attempts.getAndIncrement() == 0) {
                listener.onFailure(new EsRejectedExecutionException("full"));
            } else {
                listener.onResponse(successes(request));
            }
        }, original, future);

        queue.runAllRunnableTasks();
        assertThat(seen, equalTo(List.of(original)));
        assertTrue(original.indexingPressureAccounted());
        assertTrue(queue.hasDeferredTasks());
        assertFalse(future.isDone());

        queue.advanceTime();
        queue.runAllRunnableTasks();
        BulkResponse response = future.actionGet(TimeValue.timeValueSeconds(1));
        assertFalse(response.hasFailures());
        assertThat(seen, equalTo(List.of(original, original)));
        assertThat(seen.get(1), sameInstance(original));
        assertTrue(seen.get(1).indexingPressureAccounted());
    }

    /**
     * A partial retry is a new bulk. It carries the accounted flag when the original held a charge, and not when it did not.
     */
    public void testRejectedItemsCopyTheAccountedFlagOntoTheRetryBulk() {
        assertRejectedItemsCopyAccountedFlag(true);
        assertRejectedItemsCopyAccountedFlag(false);
    }

    /**
     * One success and one failure that is not a rejection is terminal. The listener runs once and nothing is rescheduled.
     */
    public void testMixedSuccessAndOtherFailureDoesNotRetry() {
        DeterministicTaskQueue queue = new DeterministicTaskQueue();
        BulkRequest original = bulk(2);
        original.indexingPressureAccounted(randomBoolean());
        AtomicInteger attempts = new AtomicInteger();
        AtomicInteger responses = new AtomicInteger();
        AtomicInteger failures = new AtomicInteger();
        PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();

        new Retry(backoff(5), queue.getThreadPool()).withBackoff((request, listener) -> {
            attempts.incrementAndGet();
            IndexRequest first = (IndexRequest) request.requests().get(0);
            IndexRequest second = (IndexRequest) request.requests().get(1);
            listener.onResponse(
                new BulkResponse(
                    new BulkItemResponse[] {
                        BulkItemResponse.success(0, OpType.INDEX, indexResponse(first)),
                        BulkItemResponse.failure(
                            1,
                            OpType.INDEX,
                            new BulkItemResponse.Failure(second.index(), second.id(), new ElasticsearchException("nope"))
                        ) },
                    1L
                )
            );
        }, original, new ActionListener<>() {
            @Override
            public void onResponse(BulkResponse response) {
                responses.incrementAndGet();
                future.onResponse(response);
            }

            @Override
            public void onFailure(Exception e) {
                failures.incrementAndGet();
                future.onFailure(e);
            }
        });

        queue.runAllTasksInTimeOrder();
        BulkResponse response = future.actionGet(TimeValue.timeValueSeconds(1));
        assertThat(attempts.get(), equalTo(1));
        assertThat(responses.get(), equalTo(1));
        assertThat(failures.get(), equalTo(0));
        assertTrue(response.hasFailures());
        assertThat(response.getItems().length, equalTo(2));
        assertFalse(queue.hasAnyTasks());
    }

    /**
     * The flag stays on the coordinating node. A copy read off the wire is not accounted, even when the sender had set it.
     */
    public void testAccountedFlagIsNotWrittenOnTheWire() throws IOException {
        BulkRequest original = bulk(randomIntBetween(1, 3));
        original.indexingPressureAccounted(true);
        assertTrue(original.indexingPressureAccounted());

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        BulkRequest read = new BulkRequest(out.bytes().streamInput());

        assertFalse(read.indexingPressureAccounted());
        assertThat(read.numberOfActions(), equalTo(original.numberOfActions()));
        assertThat(read, not(sameInstance(original)));
    }

    private void assertRejectedItemsCopyAccountedFlag(boolean accounted) {
        DeterministicTaskQueue queue = new DeterministicTaskQueue();
        BulkRequest original = bulk(randomIntBetween(2, 4));
        original.indexingPressureAccounted(accounted);
        List<BulkRequest> seen = new ArrayList<>();
        AtomicInteger attempts = new AtomicInteger();
        PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();

        new Retry(backoff(1), queue.getThreadPool()).withBackoff((request, listener) -> {
            seen.add(request);
            if (attempts.getAndIncrement() == 0) {
                listener.onResponse(allRejected(request));
            } else {
                listener.onResponse(successes(request));
            }
        }, original, future);

        queue.runAllRunnableTasks();
        assertThat(seen.size(), equalTo(1));
        queue.advanceTime();
        queue.runAllRunnableTasks();

        BulkResponse response = future.actionGet(TimeValue.timeValueSeconds(1));
        assertFalse(response.hasFailures());
        assertThat(seen.size(), equalTo(2));
        assertThat(seen.get(1), not(sameInstance(original)));
        assertThat(seen.get(1).indexingPressureAccounted(), equalTo(accounted));
        assertThat(seen.get(1).numberOfActions(), equalTo(original.numberOfActions()));
        assertThat(response.getItems().length, equalTo(original.numberOfActions()));
    }

    private static BackoffPolicy backoff(int retries) {
        return BackoffPolicy.constantBackoff(TimeValue.timeValueMillis(randomIntBetween(1, 50)), retries);
    }

    private BulkRequest bulk(int actions) {
        BulkRequest request = new BulkRequest();
        for (int i = 0; i < actions; i++) {
            request.add(new IndexRequest(randomAlphaOfLength(6)).id(randomAlphaOfLength(4)).source("f", randomAlphaOfLength(8)));
        }
        return request;
    }

    private static BulkResponse successes(BulkRequest request) {
        BulkItemResponse[] items = new BulkItemResponse[request.requests().size()];
        for (int i = 0; i < items.length; i++) {
            items[i] = BulkItemResponse.success(i, OpType.INDEX, indexResponse((IndexRequest) request.requests().get(i)));
        }
        return new BulkResponse(items, 1L);
    }

    private static BulkResponse allRejected(BulkRequest request) {
        BulkItemResponse[] items = new BulkItemResponse[request.requests().size()];
        for (int i = 0; i < items.length; i++) {
            IndexRequest index = (IndexRequest) request.requests().get(i);
            items[i] = BulkItemResponse.failure(
                i,
                OpType.INDEX,
                new BulkItemResponse.Failure(index.index(), index.id(), new EsRejectedExecutionException("full"))
            );
        }
        return new BulkResponse(items, 1L);
    }

    private static IndexResponse indexResponse(IndexRequest index) {
        return new IndexResponse(new ShardId(index.index(), randomAlphaOfLength(4), 0), index.id(), 1L, 1L, 1L, false);
    }
}
