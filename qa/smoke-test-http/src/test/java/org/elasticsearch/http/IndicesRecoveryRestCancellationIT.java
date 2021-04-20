/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.action.admin.indices.recovery.RecoveryAction;
import org.elasticsearch.action.admin.indices.recovery.TransportRecoveryAction;
import org.elasticsearch.action.admin.indices.recovery.TransportRecoveryActionHelper;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Semaphore;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;

public class IndicesRecoveryRestCancellationIT extends HttpSmokeTestCase {

    public void testIndicesRecoveryRestCancellation() throws Exception {
        runTest(new Request(HttpGet.METHOD_NAME, "/_recovery"));
    }

    public void testCatRecoveryRestCancellation() throws Exception {
        runTest(new Request(HttpGet.METHOD_NAME, "/_cat/recovery"));
    }

    private void runTest(Request request) throws Exception {

        createIndex("test");
        ensureGreen("test");

        final List<Semaphore> operationBlocks = new ArrayList<>();
        for (final TransportRecoveryAction transportRecoveryAction : internalCluster().getInstances(TransportRecoveryAction.class)) {
            final Semaphore operationBlock = new Semaphore(1);
            operationBlocks.add(operationBlock);
            TransportRecoveryActionHelper.setOnShardOperation(transportRecoveryAction, () -> {
                try {
                    operationBlock.acquire();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                operationBlock.release();
            });
        }
        assertThat(operationBlocks, not(empty()));

        final List<Releasable> releasables = new ArrayList<>();
        try {
            for (final Semaphore operationBlock : operationBlocks) {
                operationBlock.acquire();
                releasables.add(operationBlock::release);
            }

            final PlainActionFuture<Void> future = new PlainActionFuture<>();
            logger.info("--> sending request");
            final Cancellable cancellable = getRestClient().performRequestAsync(request, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    future.onResponse(null);
                }

                @Override
                public void onFailure(Exception exception) {
                    future.onFailure(exception);
                }
            });

            awaitTaskWithPrefix(RecoveryAction.NAME);

            logger.info("--> waiting for at least one task to hit a block");
            assertBusy(() -> assertTrue(operationBlocks.stream().anyMatch(Semaphore::hasQueuedThreads)));

            logger.info("--> cancelling request");
            cancellable.cancel();
            expectThrows(CancellationException.class, future::actionGet);

            logger.info("--> checking that all tasks are marked as cancelled");
            assertBusy(() -> {
                boolean foundTask = false;
                for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
                    for (CancellableTask cancellableTask : transportService.getTaskManager().getCancellableTasks().values()) {
                        if (cancellableTask.getAction().startsWith(RecoveryAction.NAME)) {
                            foundTask = true;
                            assertTrue("task " + cancellableTask.getId() + " not cancelled", cancellableTask.isCancelled());
                        }
                    }
                }
                assertTrue("found no cancellable tasks", foundTask);
            });
        } finally {
            Releasables.close(releasables);
        }

        logger.info("--> checking that all tasks have finished");
        assertBusy(() -> {
            final List<TaskInfo> tasks = client().admin().cluster().prepareListTasks().get().getTasks();
            assertTrue(tasks.toString(), tasks.stream().noneMatch(t -> t.getAction().startsWith(RecoveryAction.NAME)));
        });
    }

}
