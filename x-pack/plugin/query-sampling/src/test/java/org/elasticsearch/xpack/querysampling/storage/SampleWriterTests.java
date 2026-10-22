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

    private record Reported(QueryFingerprint fingerprint, TrackedQuery.Weights weights) {}

    private final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();

    private final MultiplicityTracker tracker = new MultiplicityTracker(100);

    private final List<BulkRequest> requests = new ArrayList<>();

    private final List<ActionListener<BulkResponse>> listeners = new ArrayList<>();

    private final List<Reported> reported = new ArrayList<>();

    private SampleWriter writer(int maxBatch, int maxPending) {
        return new SampleWriter("sampler", (request, listener) -> {
            requests.add(request);
            listeners.add(listener);
        },
            taskQueue.getThreadPool(),
            taskQueue.getThreadPool().generic(),
            () -> 42L,
            (fingerprint, tracked, weights) -> reported.add(new Reported(fingerprint, weights)),
            maxBatch,
            maxPending,
            TimeValue.timeValueSeconds(1)
        );
    }

    /** A query seen {@code arrivals} times, so that its weights say which query they belong to. */
    private SampledQuery sampled(long id, int arrivals, String eventId) {
        QueryFingerprint fingerprint = new QueryFingerprint(id, id);
        TrackedQuery tracked = null;
        for (int i = 0; i < arrivals; i++) {
            tracked = tracker.record(fingerprint);
        }
        CapturedQuery query = new CapturedQuery(new String[] { "idx" }, "vec", new float[] { id }, 3, 10, null, null, List.of(), null);
        return new SampledQuery(fingerprint, new CapturedSearch(query, List.of(), 1, 1.0), tracked, eventId);
    }

    private static BulkResponse answer(boolean... succeeded) {
        BulkItemResponse[] items = new BulkItemResponse[succeeded.length];
        for (int i = 0; i < items.length; i++) {
            items[i] = succeeded[i]
                ? BulkItemResponse.success(
                    i,
                    DocWriteRequest.OpType.INDEX,
                    new IndexResponse(new ShardId(QuerySamplingIndex.NAME, "_na_", 0), "id", 0, 1, 1, true)
                )
                : BulkItemResponse.failure(
                    i,
                    DocWriteRequest.OpType.INDEX,
                    new BulkItemResponse.Failure(QuerySamplingIndex.NAME, "id", new IllegalStateException("rejected"))
                );
        }
        return new BulkResponse(items, 1);
    }

    public void testOnlyTheWrittenQueriesAreReportedWithTheWeightsReadForThem() {
        SampleWriter writer = writer(3, 10);
        SampledQuery first = sampled(1, 1, null);
        SampledQuery second = sampled(2, 2, null);
        SampledQuery third = sampled(3, 3, null);
        writer.onSampled(first);
        writer.onSampled(second);
        writer.onSampled(third);
        taskQueue.runAllRunnableTasks();
        assertThat(requests.get(0).numberOfActions(), equalTo(3));
        tracker.record(third.fingerprint()); // changes after the document was built, which the refresher takes care of

        listeners.get(0).onResponse(answer(true, false, true));

        assertThat(reported.size(), equalTo(2));
        assertThat(reported.get(0).fingerprint(), equalTo(first.fingerprint()));
        assertThat(reported.get(0).weights().multiplicity(), equalTo(1L));
        assertThat(reported.get(1).fingerprint(), equalTo(third.fingerprint()));
        assertThat(reported.get(1).weights().multiplicity(), equalTo(3L));
        assertThat(writer.written(), equalTo(2L));
        assertThat(writer.failed(), equalTo(1L));
    }

    public void testEventsAreWrittenButNeverReported() {
        SampleWriter writer = writer(2, 10);
        writer.onSampled(sampled(1, 1, "event"));
        writer.onSampled(sampled(2, 1, null));
        taskQueue.runAllRunnableTasks();

        listeners.get(0).onResponse(answer(true, true));

        assertThat(writer.written(), equalTo(2L));
        assertThat(reported.size(), equalTo(1));
        assertThat(reported.get(0).fingerprint(), equalTo(new QueryFingerprint(2, 2)));
    }

    public void testQueriesBeyondTheMostThatAreWaitingAreDropped() {
        SampleWriter writer = writer(10, 2);

        for (int i = 1; i <= 5; i++) {
            writer.onSampled(sampled(i, 1, null));
        }

        assertThat(writer.dropped(), equalTo(3L));
    }
}
