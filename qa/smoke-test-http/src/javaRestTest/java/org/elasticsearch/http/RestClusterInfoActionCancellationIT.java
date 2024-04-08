/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.AckedRequest;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;

import java.util.EnumSet;
import java.util.concurrent.CancellationException;
import java.util.function.Function;

import static org.elasticsearch.action.support.ActionTestUtils.wrapAsRestResponseListener;
import static org.elasticsearch.test.TaskAssertions.assertAllCancellableTasksAreCancelled;
import static org.elasticsearch.test.TaskAssertions.assertAllTasksHaveFinished;
import static org.elasticsearch.test.TaskAssertions.awaitTaskWithPrefixOnMaster;
import static org.hamcrest.core.IsEqual.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class RestClusterInfoActionCancellationIT extends HttpSmokeTestCase {

    public void testGetMappingsCancellation() throws Exception {
        runTest(GetMappingsAction.NAME, "/test/_mappings");
    }

    public void testGetIndicesCancellation() throws Exception {
        runTest(GetIndexAction.NAME, "/test");
    }

    private void runTest(String actionName, String endpoint) throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        createIndex("test");
        ensureGreen("test");
        // Add a retryable cluster block that would block the request execution
        updateClusterState(currentState -> {
            ClusterBlock clusterBlock = new ClusterBlock(
                1000,
                actionName + " cancellation test cluster block",
                true,
                false,
                false,
                RestStatus.BAD_REQUEST,
                EnumSet.of(ClusterBlockLevel.METADATA_READ)
            );

            return ClusterState.builder(currentState).blocks(ClusterBlocks.builder().addGlobalBlock(clusterBlock).build()).build();
        });

        final Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        final PlainActionFuture<Response> future = new PlainActionFuture<>();
        final Cancellable cancellable = getRestClient().performRequestAsync(request, wrapAsRestResponseListener(future));

        assertThat(future.isDone(), equalTo(false));
        awaitTaskWithPrefixOnMaster(actionName);
        // To ensure that the task is executing on master, we wait until the first blocked execution of the task registers its cluster state
        // observer for further retries. This ensures that a task is not cancelled before we have started its execution, which could result
        // in the task being unregistered and the test not being able to find any cancelled tasks.
        assertBusy(
            () -> assertThat(
                internalCluster().getCurrentMasterNodeInstance(ClusterService.class)
                    .getClusterApplierService()
                    .getTimeoutClusterStateListenersSize(),
                Matchers.greaterThan(0)
            )
        );
        cancellable.cancel();
        assertAllCancellableTasksAreCancelled(actionName);

        // Remove the cluster block
        updateClusterState(currentState -> ClusterState.builder(currentState).blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK).build());

        expectThrows(CancellationException.class, future::actionGet);

        assertAllTasksHaveFinished(actionName);
    }

    private void updateClusterState(Function<ClusterState, ClusterState> transformationFn) {
        final TimeValue timeout = TimeValue.timeValueSeconds(10);

        final AckedRequest ackedRequest = new AckedRequest() {
            @Override
            public TimeValue ackTimeout() {
                return timeout;
            }

            @Override
            public TimeValue masterNodeTimeout() {
                return timeout;
            }
        };

        PlainActionFuture<AcknowledgedResponse> future = new PlainActionFuture<>();
        internalCluster().getAnyMasterNodeInstance(ClusterService.class)
            .submitUnbatchedStateUpdateTask("get_mappings_cancellation_test", new AckedClusterStateUpdateTask(ackedRequest, future) {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    return transformationFn.apply(currentState);
                }
            });

        future.actionGet();
    }
}
