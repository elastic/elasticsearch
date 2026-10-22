/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.storage;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.querysampling.dedup.MultiplicityTracker;
import org.elasticsearch.xpack.querysampling.dedup.QueryFingerprint;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class WeightsRefresherTests extends ESTestCase {

    private final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();

    private final List<BulkRequest> requests = new ArrayList<>();

    private final List<ActionListener<BulkResponse>> listeners = new ArrayList<>();

    private static final TimeValue REFRESH_INTERVAL = TimeValue.timeValueSeconds(30);

    private final MultiplicityTracker tracker = new MultiplicityTracker(100);

    public void testChangedWeightsAreWrittenAgainAndUnchangedOnesAreNot() {
        WeightsRefresher refresher = refresher(10);
        QueryFingerprint fingerprint = new QueryFingerprint(1, 1);
        TrackedQuery query = tracker.record(fingerprint);
        refresher.written(fingerprint, query, query.weights());

        nextRound();
        assertThat(requests.size(), equalTo(0));

        tracker.record(fingerprint);
        nextRound();

        assertThat(requests.size(), equalTo(1));
        assertThat(requests.get(0).numberOfActions(), equalTo(1));
        assertThat(requests.get(0).requests().get(0).id(), equalTo(SampleRecord.documentId("sampler", fingerprint)));
        listeners.get(0).onResponse(responseOf(1));
        assertThat(refresher.refreshed(), equalTo(1L));

        nextRound();
        assertThat(requests.size(), equalTo(1));
        assertThat(refresher.tracked(), equalTo(1));
    }

    private WeightsRefresher refresher(int maxBatch) {
        return new WeightsRefresher("sampler", (request, listener) -> {
            requests.add(request);
            listeners.add(listener);
        }, taskQueue.getThreadPool(), taskQueue.getThreadPool().generic(), () -> 42L, (fp, query) -> true, maxBatch, REFRESH_INTERVAL);
    }

    private void nextRound() {
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();
    }

    private static BulkResponse responseOf(int items) {
        BulkItemResponse[] responses = new BulkItemResponse[items];
        for (int i = 0; i < items; i++) {
            responses[i] = BulkItemResponse.success(
                i,
                DocWriteRequest.OpType.UPDATE,
                new UpdateResponse(new ShardId(QuerySamplingIndex.NAME, "_na_", 0), "id", 0, 1, 1, DocWriteResponse.Result.UPDATED)
            );
        }
        return new BulkResponse(responses, 1);
    }
}
