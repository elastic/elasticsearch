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
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Semaphore;

import static org.elasticsearch.action.support.ActionTestUtils.wrapAsRestResponseListener;
import static org.elasticsearch.test.TaskAssertions.assertAllCancellableTasksAreCancelled;
import static org.elasticsearch.test.TaskAssertions.assertAllTasksHaveFinished;
import static org.elasticsearch.test.TaskAssertions.awaitTaskWithPrefix;
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

            final PlainActionFuture<Response> future = new PlainActionFuture<>();
            logger.info("--> sending request");
            final Cancellable cancellable = getRestClient().performRequestAsync(request, wrapAsRestResponseListener(future));

            awaitTaskWithPrefix(RecoveryAction.NAME);

            logger.info("--> waiting for at least one task to hit a block");
            assertBusy(() -> assertTrue(operationBlocks.stream().anyMatch(Semaphore::hasQueuedThreads)));

            logger.info("--> cancelling request");
            cancellable.cancel();
            expectThrows(CancellationException.class, future::actionGet);

            assertAllCancellableTasksAreCancelled(RecoveryAction.NAME);
        } finally {
            Releasables.close(releasables);
        }

        assertAllTasksHaveFinished(RecoveryAction.NAME);
    }

}
