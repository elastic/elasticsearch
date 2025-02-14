/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.action.admin.cluster.health.TransportClusterHealthAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CyclicBarrier;

import static org.elasticsearch.action.support.ActionTestUtils.wrapAsRestResponseListener;
import static org.elasticsearch.test.TaskAssertions.assertAllCancellableTasksAreCancelled;

public class ClusterHealthRestCancellationIT extends HttpSmokeTestCase {

    public void testClusterHealthRestCancellation() throws Exception {

        final var barrier = new CyclicBarrier(2);

        internalCluster().getCurrentMasterNodeInstance(ClusterService.class)
            .submitUnbatchedStateUpdateTask("blocking", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    safeAwait(barrier);
                    safeAwait(barrier);
                    return currentState;
                }

                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError(e);
                }
            });

        final Request clusterHealthRequest = new Request(HttpGet.METHOD_NAME, "/_cluster/health");
        clusterHealthRequest.addParameter("wait_for_events", Priority.LANGUID.toString());

        final PlainActionFuture<Response> future = new PlainActionFuture<>();
        logger.info("--> sending cluster health request");
        final Cancellable cancellable = getRestClient().performRequestAsync(clusterHealthRequest, wrapAsRestResponseListener(future));

        safeAwait(barrier);

        // wait until the health request is waiting on the (blocked) master service
        assertBusy(
            () -> assertTrue(
                internalCluster().getCurrentMasterNodeInstance(ClusterService.class)
                    .getMasterService()
                    .pendingTasks()
                    .stream()
                    .anyMatch(
                        pendingClusterTask -> pendingClusterTask.source().string().equals("cluster_health (wait_for_events [LANGUID])")
                    )
            )
        );

        logger.info("--> cancelling cluster health request");
        cancellable.cancel();
        expectThrows(CancellationException.class, future::actionGet);

        logger.info("--> checking cluster health task cancelled");
        assertAllCancellableTasksAreCancelled(TransportClusterHealthAction.NAME);

        safeAwait(barrier);
    }

}
