/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.desirednodes;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.desirednodes.VersionConflictException;
import org.elasticsearch.cluster.metadata.DesiredNode;
import org.elasticsearch.cluster.metadata.DesiredNodeWithStatus;
import org.elasticsearch.cluster.metadata.DesiredNodes;
import org.elasticsearch.cluster.metadata.DesiredNodesTestCase;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.metadata.DesiredNodesTestCase.randomDesiredNode;
import static org.elasticsearch.common.util.concurrent.EsExecutors.NODE_PROCESSORS_SETTING;
import static org.elasticsearch.node.NodeRoleSettings.NODE_ROLES_SETTING;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class TransportDesiredNodesActionsIT extends ESIntegTestCase {

    @After
    public void cleanDesiredNodes() {
        deleteDesiredNodes();
    }

    public void testUpdateDesiredNodes() {
        final var updateDesiredNodesRequest = randomUpdateDesiredNodesRequest();
        final var response = updateDesiredNodes(updateDesiredNodesRequest);
        assertThat(response.hasReplacedExistingHistoryId(), is(equalTo(false)));
        assertThat(response.dryRun(), is(equalTo(false)));

        final DesiredNodes latestDesiredNodes = getLatestDesiredNodes();
        assertStoredDesiredNodesAreCorrect(updateDesiredNodesRequest, latestDesiredNodes);
    }

    public void testDryRunUpdateDoesNotUpdateEmptyDesiredNodes() {
        UpdateDesiredNodesResponse dryRunResponse = updateDesiredNodes(randomDryRunUpdateDesiredNodesRequest(Settings.EMPTY));
        assertThat(dryRunResponse.dryRun(), is(equalTo(true)));

        expectThrows(ResourceNotFoundException.class, this::getLatestDesiredNodes);
    }

    public void testDryRunUpdateDoesNotUpdateExistingDesiredNodes() {
        UpdateDesiredNodesResponse response = updateDesiredNodes(randomUpdateDesiredNodesRequest(Settings.EMPTY));
        assertThat(response.dryRun(), is(equalTo(false)));

        DesiredNodes desiredNodes = getLatestDesiredNodes();

        UpdateDesiredNodesResponse dryRunResponse = updateDesiredNodes(randomDryRunUpdateDesiredNodesRequest(Settings.EMPTY));
        assertThat(dryRunResponse.dryRun(), is(equalTo(true)));

        assertEquals(getLatestDesiredNodes(), desiredNodes);
    }

    public void testUpdateDesiredNodesIsIdempotent() {
        final var updateDesiredNodesRequest = randomUpdateDesiredNodesRequest();
        updateDesiredNodes(updateDesiredNodesRequest);

        final List<DesiredNode> desiredNodesList = new ArrayList<>(updateDesiredNodesRequest.getNodes());
        if (randomBoolean()) {
            Collections.shuffle(desiredNodesList, random());
        }

        final var equivalentUpdateRequest = new UpdateDesiredNodesRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            updateDesiredNodesRequest.getHistoryID(),
            updateDesiredNodesRequest.getVersion(),
            desiredNodesList,
            false
        );

        updateDesiredNodes(equivalentUpdateRequest);

        final DesiredNodes latestDesiredNodes = getLatestDesiredNodes();
        assertStoredDesiredNodesAreCorrect(updateDesiredNodesRequest, latestDesiredNodes);
        assertStoredDesiredNodesAreCorrect(equivalentUpdateRequest, latestDesiredNodes);
    }

    public void testGoingBackwardsWithinTheSameHistoryIsForbidden() {
        final var updateDesiredNodesRequest = randomUpdateDesiredNodesRequest();
        updateDesiredNodes(updateDesiredNodesRequest);

        final var backwardsUpdateDesiredNodesRequest = new UpdateDesiredNodesRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            updateDesiredNodesRequest.getHistoryID(),
            updateDesiredNodesRequest.getVersion() - 1,
            updateDesiredNodesRequest.getNodes(),
            false
        );

        final VersionConflictException exception = expectThrows(
            VersionConflictException.class,
            () -> updateDesiredNodes(backwardsUpdateDesiredNodesRequest)
        );
        assertThat(exception.getMessage(), containsString("has been superseded by version"));
    }

    public void testSameVersionWithDifferentContentIsForbidden() {
        final var updateDesiredNodesRequest = randomUpdateDesiredNodesRequest();
        updateDesiredNodes(updateDesiredNodesRequest);

        final var updateDesiredNodesRequestWithSameHistoryIdAndVersionAndDifferentSpecs = new UpdateDesiredNodesRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            updateDesiredNodesRequest.getHistoryID(),
            updateDesiredNodesRequest.getVersion(),
            randomList(1, 10, DesiredNodesTestCase::randomDesiredNode),
            false
        );

        final IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> updateDesiredNodes(updateDesiredNodesRequestWithSameHistoryIdAndVersionAndDifferentSpecs)
        );
        assertThat(exception.getMessage(), containsString("already exists with a different definition"));
    }

    public void testCanMoveToANewHistory() {
        final var updateDesiredNodesRequest = randomUpdateDesiredNodesRequest();
        updateDesiredNodes(updateDesiredNodesRequest);

        {
            final DesiredNodes latestDesiredNodes = getLatestDesiredNodes();
            assertStoredDesiredNodesAreCorrect(updateDesiredNodesRequest, latestDesiredNodes);
        }

        final var newUpdateDesiredNodesRequest = randomUpdateDesiredNodesRequest();
        final var response = updateDesiredNodes(newUpdateDesiredNodesRequest);
        assertThat(response.hasReplacedExistingHistoryId(), is(equalTo(true)));

        {
            final DesiredNodes latestDesiredNodes = getLatestDesiredNodes();
            assertStoredDesiredNodesAreCorrect(newUpdateDesiredNodesRequest, latestDesiredNodes);
        }
    }

    public void testAtLeastOneMaterNodeIsExpected() {
        {
            final var updateDesiredNodesRequest = randomUpdateDesiredNodesRequest(
                Settings.builder().put(NODE_ROLES_SETTING.getKey(), "data_hot").build()
            );

            final IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> updateDesiredNodes(updateDesiredNodesRequest)
            );
            assertThat(exception.getMessage(), containsString("nodes must contain at least one master node"));
        }

        {
            final var updateDesiredNodesRequest = randomUpdateDesiredNodesRequest(
                Settings.builder().put(NODE_ROLES_SETTING.getKey(), "master").build()
            );
            updateDesiredNodes(updateDesiredNodesRequest);
        }
    }

    public void testUnknownSettingsAreAllowedInFutureVersions() {
        final var updateDesiredNodesRequest = randomUpdateDesiredNodesRequest(
            Settings.builder().put("desired_nodes.random_setting", Integer.MIN_VALUE).build()
        );

        updateDesiredNodes(updateDesiredNodesRequest);

        final DesiredNodes latestDesiredNodes = getLatestDesiredNodes();
        assertStoredDesiredNodesAreCorrect(updateDesiredNodesRequest, latestDesiredNodes);
    }

    public void testNodeProcessorsGetValidatedWithDesiredNodeProcessors() {
        final int numProcessors = Math.max(Runtime.getRuntime().availableProcessors() + 1, 2048);
        // This test verifies that the validation doesn't throw on desired nodes
        // with a higher number of available processors than the node running the tests.
        final var updateDesiredNodesRequest = new UpdateDesiredNodesRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            UUIDs.randomBase64UUID(),
            randomIntBetween(1, 20),
            randomList(
                1,
                20,
                () -> randomDesiredNode(Settings.builder().put(NODE_PROCESSORS_SETTING.getKey(), numProcessors).build(), numProcessors)
            ),
            false
        );

        updateDesiredNodes(updateDesiredNodesRequest);

        final DesiredNodes latestDesiredNodes = getLatestDesiredNodes();
        assertStoredDesiredNodesAreCorrect(updateDesiredNodesRequest, latestDesiredNodes);

        assertThat(latestDesiredNodes.nodes().isEmpty(), is(equalTo(false)));
        for (final var desiredNodeWithStatus : latestDesiredNodes) {
            final var desiredNode = desiredNodeWithStatus.desiredNode();
            assertThat(desiredNode.settings().get(NODE_PROCESSORS_SETTING.getKey()), is(equalTo(Integer.toString(numProcessors))));
        }
    }

    public void testUpdateDesiredNodesTasksAreBatchedCorrectly() throws Exception {
        final Runnable unblockClusterStateUpdateThread = blockClusterStateUpdateThread();

        final List<UpdateDesiredNodesRequest> proposedDesiredNodes = randomList(10, 20, this::randomUpdateDesiredNodesRequest);
        final List<ActionFuture<UpdateDesiredNodesResponse>> updateDesiredNodesFutures = new ArrayList<>();
        for (final var request : proposedDesiredNodes) {
            // Use the master client to ensure the same updates ordering as in proposedDesiredNodesList
            updateDesiredNodesFutures.add(internalCluster().masterClient().execute(UpdateDesiredNodesAction.INSTANCE, request));
        }

        for (ActionFuture<UpdateDesiredNodesResponse> future : updateDesiredNodesFutures) {
            assertThat(future.isDone(), is(equalTo(false)));
        }

        unblockClusterStateUpdateThread.run();

        for (ActionFuture<UpdateDesiredNodesResponse> future : updateDesiredNodesFutures) {
            future.actionGet();
        }

        final DesiredNodes latestDesiredNodes = getLatestDesiredNodes();
        final var latestUpdateDesiredNodesRequest = proposedDesiredNodes.get(proposedDesiredNodes.size() - 1);
        assertStoredDesiredNodesAreCorrect(latestUpdateDesiredNodesRequest, latestDesiredNodes);
    }

    public void testDeleteDesiredNodesTasksAreBatchedCorrectly() throws Exception {
        if (randomBoolean()) {
            final var updateDesiredNodesRequest = randomUpdateDesiredNodesRequest();
            updateDesiredNodes(updateDesiredNodesRequest);
        }

        final Runnable unblockClusterStateUpdateThread = blockClusterStateUpdateThread();

        final List<ActionFuture<ActionResponse.Empty>> deleteDesiredNodesFutures = new ArrayList<>(15);
        for (int i = 0; i < 15; i++) {
            deleteDesiredNodesFutures.add(
                client().execute(
                    TransportDeleteDesiredNodesAction.TYPE,
                    new AcknowledgedRequest.Plain(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                )
            );
        }

        for (ActionFuture<ActionResponse.Empty> future : deleteDesiredNodesFutures) {
            assertThat(future.isDone(), is(equalTo(false)));
        }

        unblockClusterStateUpdateThread.run();

        for (ActionFuture<ActionResponse.Empty> future : deleteDesiredNodesFutures) {
            future.actionGet();
        }

        final ClusterState state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        final DesiredNodes latestDesiredNodes = DesiredNodes.latestFromClusterState(state);
        assertThat(latestDesiredNodes, is(nullValue()));
    }

    public void testGetLatestDesiredNodes() {
        expectThrows(ResourceNotFoundException.class, this::getLatestDesiredNodes);

        final var updateDesiredNodesRequest = randomUpdateDesiredNodesRequest();
        updateDesiredNodes(updateDesiredNodesRequest);

        final var latestDesiredNodes = getLatestDesiredNodes();
        assertStoredDesiredNodesAreCorrect(updateDesiredNodesRequest, latestDesiredNodes);
    }

    public void testDeleteDesiredNodes() {
        final var updateDesiredNodesRequest = randomUpdateDesiredNodesRequest();
        updateDesiredNodes(updateDesiredNodesRequest);

        final var latestDesiredNodes = getLatestDesiredNodes();
        assertStoredDesiredNodesAreCorrect(updateDesiredNodesRequest, latestDesiredNodes);

        deleteDesiredNodes();

        expectThrows(ResourceNotFoundException.class, this::getLatestDesiredNodes);
    }

    private void assertStoredDesiredNodesAreCorrect(UpdateDesiredNodesRequest updateDesiredNodesRequest, DesiredNodes latestDesiredNodes) {
        assertThat(latestDesiredNodes.historyID(), is(equalTo(updateDesiredNodesRequest.getHistoryID())));
        assertThat(latestDesiredNodes.version(), is(equalTo(updateDesiredNodesRequest.getVersion())));
        assertThat(
            latestDesiredNodes.nodes().stream().map(DesiredNodeWithStatus::desiredNode).toList(),
            containsInAnyOrder(updateDesiredNodesRequest.getNodes().toArray())
        );
    }

    private UpdateDesiredNodesRequest randomUpdateDesiredNodesRequest() {
        return randomUpdateDesiredNodesRequest(Settings.EMPTY);
    }

    private UpdateDesiredNodesRequest randomUpdateDesiredNodesRequest(Settings settings) {
        return new UpdateDesiredNodesRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            UUIDs.randomBase64UUID(),
            randomIntBetween(2, 20),
            randomList(2, 10, () -> randomDesiredNode(settings)),
            false
        );
    }

    private UpdateDesiredNodesRequest randomDryRunUpdateDesiredNodesRequest(Settings settings) {
        return new UpdateDesiredNodesRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            UUIDs.randomBase64UUID(),
            randomIntBetween(2, 20),
            randomList(2, 10, () -> randomDesiredNode(settings)),
            true
        );
    }

    private void deleteDesiredNodes() {
        client().execute(TransportDeleteDesiredNodesAction.TYPE, new AcknowledgedRequest.Plain(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT))
            .actionGet();
    }

    private DesiredNodes getLatestDesiredNodes() {
        final GetDesiredNodesAction.Request request = new GetDesiredNodesAction.Request(TEST_REQUEST_TIMEOUT);
        final GetDesiredNodesAction.Response response = client().execute(GetDesiredNodesAction.INSTANCE, request).actionGet();
        return response.getDesiredNodes();
    }

    private UpdateDesiredNodesResponse updateDesiredNodes(UpdateDesiredNodesRequest request) {
        return client().execute(UpdateDesiredNodesAction.INSTANCE, request).actionGet();
    }

    private Runnable blockClusterStateUpdateThread() throws InterruptedException {
        final CountDownLatch unblockClusterStateUpdateTask = new CountDownLatch(1);
        final CountDownLatch blockingClusterStateUpdateTaskExecuting = new CountDownLatch(1);
        final ClusterService clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        clusterService.submitUnbatchedStateUpdateTask("blocking-task", new ClusterStateUpdateTask(Priority.IMMEDIATE) {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                blockingClusterStateUpdateTaskExecuting.countDown();
                assertTrue(unblockClusterStateUpdateTask.await(10, TimeUnit.SECONDS));
                return currentState;
            }

            @Override
            public void onFailure(Exception e) {
                blockingClusterStateUpdateTaskExecuting.countDown();
                assert false : e.getMessage();
            }
        });

        assertTrue(blockingClusterStateUpdateTaskExecuting.await(10, TimeUnit.SECONDS));
        return unblockClusterStateUpdateTask::countDown;
    }
}
