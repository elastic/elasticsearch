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
import org.elasticsearch.rest.RestStatus;
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

    private boolean tracking = true;

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

    public void testAQueryTheTrackerForgotHasItsLastChangedStateWrittenOnceAndIsThenDropped() {
        WeightsRefresher refresher = refresher(10);
        QueryFingerprint fingerprint = new QueryFingerprint(1, 1);
        TrackedQuery query = tracker.record(fingerprint);
        refresher.written(fingerprint, query, query.weights());
        tracker.record(fingerprint);
        tracking = false;

        nextRound();
        assertThat("a change is written whether or not the query is tracked", requests.size(), equalTo(1));
        listeners.get(0).onResponse(responseOf(1));
        nextRound();

        assertThat(requests.size(), equalTo(1));
        assertThat(refresher.tracked(), equalTo(0));
    }

    public void testAQueryWhoseDocumentIsGoneIsDropped() {
        WeightsRefresher refresher = changedQuery();

        nextRound();
        listeners.get(0).onResponse(responseOfFailure(RestStatus.NOT_FOUND));

        assertThat(refresher.tracked(), equalTo(0));
        assertFalse("nothing is left to refresh", taskQueue.hasDeferredTasks());
    }

    public void testAQueryWhoseUpdatesKeepFailingIsGivenUpAfterFiveRounds() {
        WeightsRefresher refresher = changedQuery();

        for (int round = 1; round <= 5; round++) {
            nextRound();
            assertThat(requests.size(), equalTo(round));
            listeners.get(round - 1).onResponse(responseOfFailure(RestStatus.TOO_MANY_REQUESTS));
            assertThat(refresher.tracked(), equalTo(round < 5 ? 1 : 0));
        }

        assertFalse("it is not sent again", taskQueue.hasDeferredTasks());
    }

    public void testAFailedRequestIsNotHeldAgainstTheQueries() {
        WeightsRefresher refresher = changedQuery();

        for (int round = 1; round <= 7; round++) {
            nextRound();
            assertThat(requests.size(), equalTo(round));
            listeners.get(round - 1).onFailure(new IllegalStateException("no connection"));
        }
        assertThat(refresher.tracked(), equalTo(1));

        nextRound();
        listeners.get(7).onResponse(responseOf(1));
        assertThat(refresher.refreshed(), equalTo(1L));
    }

    /** A written query whose weights changed since, and that is still tracked. */
    private WeightsRefresher changedQuery() {
        WeightsRefresher refresher = refresher(10);
        QueryFingerprint fingerprint = new QueryFingerprint(1, 1);
        TrackedQuery query = tracker.record(fingerprint);
        refresher.written(fingerprint, query, query.weights());
        tracker.record(fingerprint);
        return refresher;
    }

    private WeightsRefresher refresher(int maxBatch) {
        return new WeightsRefresher("sampler", (request, listener) -> {
            requests.add(request);
            listeners.add(listener);
        }, taskQueue.getThreadPool(), taskQueue.getThreadPool().generic(), () -> 42L, (fp, query) -> tracking, maxBatch, REFRESH_INTERVAL);
    }

    private void nextRound() {
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();
    }

    private static BulkResponse responseOfFailure(RestStatus status) {
        BulkItemResponse failure = BulkItemResponse.failure(
            0,
            DocWriteRequest.OpType.UPDATE,
            new BulkItemResponse.Failure(QuerySamplingIndex.NAME, "id", new IllegalStateException("failed"), status)
        );
        return new BulkResponse(new BulkItemResponse[] { failure }, 1);
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
