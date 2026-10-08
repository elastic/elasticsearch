/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.storage;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchResponse;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchTask;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;

public class SampleRetentionTests extends ESTestCase {

    private static final TimeValue RETENTION = TimeValue.timeValueDays(7);

    private final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
    private final AtomicBoolean master = new AtomicBoolean(true);
    private final List<DeleteByQueryRequest> requests = new ArrayList<>();
    private final List<ActionListener<BulkByPaginatedSearchResponse>> listeners = new ArrayList<>();

    public void testDeletesWhatIsOlderThanTheRetention() {
        SampleRetention retention = retention();

        retention.run();

        assertThat(requests.size(), equalTo(1));
        DeleteByQueryRequest request = requests.get(0);
        assertThat(request.indices(), equalTo(new String[] { QuerySamplingIndex.NAME }));
        // the clock of the test says 10 days
        long cutoff = TimeValue.timeValueDays(10).millis() - RETENTION.millis();
        assertThat(request.getSearchRequest().source().query(), equalTo(QueryBuilders.rangeQuery("picked_at").lt(cutoff)));
        assertFalse(request.isAbortOnVersionConflict());
        assertTrue(request.isRefresh());

        listeners.get(0).onResponse(response(5));
        assertThat(retention.deleted(), equalTo(5L));
    }

    public void testOnlyTheElectedMasterDeletes() {
        SampleRetention retention = retention();
        master.set(false);

        retention.run();

        assertThat(requests.size(), equalTo(0));
    }

    public void testFailuresAreCountedButAMissingIndexIsNot() {
        SampleRetention retention = retention();

        retention.run();
        listeners.get(0).onFailure(new IndexNotFoundException(QuerySamplingIndex.NAME));
        assertThat("nothing was ever sampled", retention.failed(), equalTo(0L));

        retention.run();
        listeners.get(1).onFailure(new IllegalStateException("delete failed"));
        assertThat(retention.failed(), equalTo(1L));
    }

    public void testRunsPeriodicallyUntilStopped() {
        SampleRetention retention = retention();

        var cancellable = retention.start(taskQueue.getThreadPool(), taskQueue.getThreadPool().generic());
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();
        assertThat(requests.size(), equalTo(1));
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();
        assertThat(requests.size(), equalTo(2));

        cancellable.cancel();
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();
        assertThat(requests.size(), equalTo(2));
    }

    public void testRoundsAreOftenForShortRetentionsAndAtMostHourlyForLongOnes() {
        assertThat(SampleRetention.interval(TimeValue.timeValueSeconds(8)), equalTo(TimeValue.timeValueSeconds(2)));
        assertThat(SampleRetention.interval(TimeValue.timeValueDays(7)), equalTo(TimeValue.timeValueHours(1)));
        assertThat(SampleRetention.interval(TimeValue.timeValueHours(2)), equalTo(TimeValue.timeValueMinutes(30)));
    }

    private SampleRetention retention() {
        return new SampleRetention((request, listener) -> {
            requests.add(request);
            listeners.add(listener);
        }, master::get, () -> TimeValue.timeValueDays(10).millis(), RETENTION);
    }

    private static BulkByPaginatedSearchResponse response(long deleted) {
        BulkByPaginatedSearchTask.Status status = new BulkByPaginatedSearchTask.Status(
            null,
            deleted,
            0,
            0,
            deleted,
            1,
            0,
            0,
            0,
            0,
            TimeValue.ZERO,
            0f,
            null,
            TimeValue.ZERO
        );
        return new BulkByPaginatedSearchResponse(TimeValue.ZERO, status, List.of(), List.of(), false);
    }
}
