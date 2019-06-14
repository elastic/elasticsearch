/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.watcher.execution;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.core.watcher.client.WatchSourceBuilder;
import org.elasticsearch.xpack.core.watcher.execution.ActionExecutionMode;
import org.elasticsearch.xpack.core.watcher.transport.actions.delete.DeleteWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchRequest;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchResponse;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.stats.WatcherStatsRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.stats.WatcherStatsResponse;
import org.elasticsearch.xpack.watcher.actions.index.IndexAction;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.trigger.manual.ManualTriggerEvent;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEvent;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.xpack.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.empty;

public class ExecuteWatchQueuedStatsTests extends AbstractWatcherIntegrationTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        // we use a small thread pool to force executions to be queued
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put("xpack.watcher.thread_pool.size", 1).build();
    }

    @Override
    protected boolean timeWarped() {
        return false;
    }

    /*
     * This test is effectively forcing a manually executed watch to end up queued while we simultaneously try to get stats, including
     * queued watches. The reason that we do this is because previously a manually executed watch would be queued as a FutureTask<?> while
     * we try to cast queued watches to WatchExecutionTask. This would previously result in a ClassCastException. This test fails when that
     * happens yet succeeds with the production code change that accompanies this test.
     */
    public void testQueuedStats() throws ExecutionException, InterruptedException {
        final Client client = client();
        new PutWatchRequestBuilder(client, "id")
                .setActive(true)
                .setSource(
                        new WatchSourceBuilder()
                                .input(simpleInput("payload", "yes"))
                                .trigger(schedule(interval("1s")))
                                .addAction(
                                        "action",
                                        TimeValue.timeValueSeconds(1),
                                        IndexAction.builder("test_index", "acknowledgement").setDocId("id")))
                .get();

        final int numberOfIterations = 128 - scaledRandomIntBetween(0, 128);

        final CyclicBarrier barrier = new CyclicBarrier(2);

        final List<ActionFuture<ExecuteWatchResponse>> futures = new ArrayList<>(numberOfIterations);
        final Thread executeWatchThread = new Thread(() -> {
            try {
                barrier.await();
            } catch (final BrokenBarrierException | InterruptedException e) {
                fail(e.toString());
            }
            for (int i = 0; i < numberOfIterations; i++) {
                final ExecuteWatchRequest request = new ExecuteWatchRequest("id");
                try {
                    request.setTriggerEvent(new ManualTriggerEvent(
                            "id-" + i,
                            new ScheduleTriggerEvent(ZonedDateTime.now(ZoneOffset.UTC), ZonedDateTime.now(ZoneOffset.UTC))));
                } catch (final IOException e) {
                    fail(e.toString());
                }
                request.setActionMode("_all", ActionExecutionMode.EXECUTE);
                request.setRecordExecution(true);
                futures.add(client.execute(ExecuteWatchAction.INSTANCE, request));
            }
        });
        executeWatchThread.start();

        final List<FailedNodeException> failures = new ArrayList<>();
        final Thread watcherStatsThread = new Thread(() -> {
            try {
                barrier.await();
            } catch (final BrokenBarrierException | InterruptedException e) {
                fail(e.toString());
            }
            for (int i = 0; i < numberOfIterations; i++) {
                final WatcherStatsResponse response = new WatcherStatsRequestBuilder(client).setIncludeQueuedWatches(true).get();
                failures.addAll(response.failures());
            }
        });
        watcherStatsThread.start();

        executeWatchThread.join();
        watcherStatsThread.join();

        for (final ActionFuture<ExecuteWatchResponse> future : futures) {
            future.get();
        }

        assertThat(failures, empty());

        new DeleteWatchRequestBuilder(client, "id").get();
    }

}
