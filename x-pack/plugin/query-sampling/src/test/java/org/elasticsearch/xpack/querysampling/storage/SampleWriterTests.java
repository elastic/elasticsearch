/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.storage;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;
import org.elasticsearch.xpack.querysampling.capture.CapturedSearch;
import org.elasticsearch.xpack.querysampling.dedup.MultiplicityTracker;
import org.elasticsearch.xpack.querysampling.dedup.QueryFingerprint;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class SampleWriterTests extends ESTestCase {

    private static final TimeValue INTERVAL = TimeValue.timeValueSeconds(1);

    private final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
    private final List<BulkRequest> requests = new ArrayList<>();
    private final List<ActionListener<BulkResponse>> listeners = new ArrayList<>();
    private final List<QueryFingerprint> writtenQueries = new ArrayList<>();

    public void testAFullBatchIsWrittenAtOnce() {
        SampleWriter writer = writer(3, 10);

        writer.onSampled(sampled(1));
        writer.onSampled(sampled(2));
        writer.onSampled(sampled(3));
        taskQueue.runAllRunnableTasks();

        assertThat(requests.size(), equalTo(1));
        BulkRequest request = requests.get(0);
        assertThat(request.numberOfActions(), equalTo(3));
        assertThat(request.requests().get(0).index(), equalTo(QuerySamplingIndex.NAME));
        assertThat(request.requests().get(0).id(), equalTo(SampleRecord.documentId("sampler", new QueryFingerprint(1, 1))));
    }

    public void testAPartialBatchWaitsForTheFlushInterval() {
        SampleWriter writer = writer(3, 10);

        writer.onSampled(sampled(1));
        taskQueue.runAllRunnableTasks();
        assertThat(requests.size(), equalTo(0));

        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();
        assertThat(requests.size(), equalTo(1));
        assertThat(requests.get(0).numberOfActions(), equalTo(1));
    }

    public void testOneRequestIsInFlightAtATime() {
        SampleWriter writer = writer(2, 10);
        writer.onSampled(sampled(1));
        writer.onSampled(sampled(2));
        taskQueue.runAllRunnableTasks();
        assertThat(requests.size(), equalTo(1));

        // a full batch is waiting, but the first request has not been answered
        writer.onSampled(sampled(3));
        writer.onSampled(sampled(4));
        taskQueue.runAllTasks();
        assertThat(requests.size(), equalTo(1));

        listeners.get(0).onResponse(new BulkResponse(new BulkItemResponse[0], 1));
        taskQueue.runAllRunnableTasks();
        assertThat(requests.size(), equalTo(2));
        assertThat(requests.get(1).numberOfActions(), equalTo(2));
        assertThat(writer.written(), equalTo(2L));
    }

    public void testTheRefresherIsToldAboutWhatWasWritten() {
        SampleWriter writer = writer(2, 10);
        writer.onSampled(sampled(1));
        writer.onSampled(sampled(2));
        taskQueue.runAllRunnableTasks();

        BulkItemResponse ok = BulkItemResponse.success(
            0,
            DocWriteRequest.OpType.INDEX,
            new IndexResponse(new ShardId(QuerySamplingIndex.NAME, "_na_", 0), "id", 0, 1, 1, true)
        );
        BulkItemResponse rejected = BulkItemResponse.failure(
            1,
            DocWriteRequest.OpType.INDEX,
            new BulkItemResponse.Failure(QuerySamplingIndex.NAME, "id", new IllegalArgumentException("rejected"))
        );
        listeners.get(0).onResponse(new BulkResponse(new BulkItemResponse[] { ok, rejected }, 1));

        assertThat("only what was written", writtenQueries, equalTo(List.of(fp(1))));
    }

    public void testEventsOfTheSameQueryAreDifferentDocumentsAndNeedNoRefreshing() {
        SampleWriter writer = writer(3, 10);
        writer.onSampled(event(1, "e1"));
        writer.onSampled(event(1, "e2"));
        writer.onSampled(sampled(1));
        taskQueue.runAllRunnableTasks();

        BulkRequest request = requests.get(0);
        assertThat(request.requests().get(0).id(), equalTo(SampleRecord.documentId("sampler", fp(1)) + "_e1"));
        assertThat(request.requests().get(1).id(), equalTo(SampleRecord.documentId("sampler", fp(1)) + "_e2"));
        assertThat(request.requests().get(2).id(), equalTo(SampleRecord.documentId("sampler", fp(1))));

        BulkItemResponse ok = BulkItemResponse.success(
            0,
            DocWriteRequest.OpType.INDEX,
            new IndexResponse(new ShardId(QuerySamplingIndex.NAME, "_na_", 0), "id", 0, 1, 1, true)
        );
        listeners.get(0).onResponse(new BulkResponse(new BulkItemResponse[] { ok, ok, ok }, 1));

        assertThat("the weights of an event are final, only the picked query is refreshed", writtenQueries, equalTo(List.of(fp(1))));
        assertThat(writer.written(), equalTo(3L));
    }

    public void testQueriesBeyondWhatCanWaitAreDropped() {
        SampleWriter writer = writer(1, 2);
        writer.onSampled(sampled(1));
        taskQueue.runAllRunnableTasks();
        assertThat(requests.size(), equalTo(1)); // in flight, never answered

        writer.onSampled(sampled(2));
        writer.onSampled(sampled(3));
        writer.onSampled(sampled(4));

        assertThat(writer.dropped(), equalTo(1L));
    }

    public void testFailuresAreCountedAndDoNotStopTheWriter() {
        SampleWriter writer = writer(2, 10);
        writer.onSampled(sampled(1));
        writer.onSampled(sampled(2));
        taskQueue.runAllRunnableTasks();

        listeners.get(0).onFailure(new IllegalStateException("bulk failed"));
        assertThat(writer.failed(), equalTo(2L));

        writer.onSampled(sampled(3));
        writer.onSampled(sampled(4));
        taskQueue.runAllRunnableTasks();
        assertThat(requests.size(), equalTo(2));

        BulkItemResponse rejected = BulkItemResponse.failure(
            0,
            DocWriteRequest.OpType.INDEX,
            new BulkItemResponse.Failure(QuerySamplingIndex.NAME, "id", new IllegalArgumentException("rejected"))
        );
        listeners.get(1).onResponse(new BulkResponse(new BulkItemResponse[] { rejected }, 1));
        assertThat(writer.failed(), equalTo(3L));
        assertThat(writer.written(), equalTo(1L));
    }

    public void testBulkThatThrowsIsCountedAsFailed() {
        SampleWriter writer = new SampleWriter(
            "sampler",
            (request, listener) -> { throw new IllegalStateException("cannot send"); },
            taskQueue.getThreadPool(),
            taskQueue.getThreadPool().generic(),
            () -> 42L,
            (fingerprint, tracked, weights) -> {},
            1,
            10,
            INTERVAL
        );

        writer.onSampled(sampled(1));
        taskQueue.runAllRunnableTasks();

        assertThat(writer.failed(), equalTo(1L));
    }

    private SampleWriter writer(int maxBatch, int maxPending) {
        return new SampleWriter("sampler", (request, listener) -> {
            requests.add(request);
            listeners.add(listener);
        },
            taskQueue.getThreadPool(),
            taskQueue.getThreadPool().generic(),
            () -> 42L,
            (fp, tracked, weights) -> writtenQueries.add(fp),
            maxBatch,
            maxPending,
            INTERVAL
        );
    }

    private static QueryFingerprint fp(long id) {
        return new QueryFingerprint(id, id);
    }

    private static SampledQuery event(long id, String eventId) {
        QueryFingerprint fingerprint = new QueryFingerprint(id, id);
        CapturedQuery query = new CapturedQuery(new String[] { "idx" }, "vec", new float[] { id }, 10, 100, null, null, List.of(), null);
        return SampledQuery.event(fingerprint, new CapturedSearch(query, List.of(), 1, 1.0), TrackedQuery.event(1.0, 0.5), eventId);
    }

    private static SampledQuery sampled(long id) {
        QueryFingerprint fingerprint = new QueryFingerprint(id, id);
        CapturedQuery query = new CapturedQuery(new String[] { "idx" }, "vec", new float[] { id }, 10, 100, null, null, List.of(), null);
        return new SampledQuery(fingerprint, new CapturedSearch(query, List.of(), 1, 1.0), new MultiplicityTracker(10).record(fingerprint));
    }
}
