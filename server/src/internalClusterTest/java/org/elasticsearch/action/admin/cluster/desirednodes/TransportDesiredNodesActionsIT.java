/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.desirednodes;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.desirednodes.VersionConflictException;
import org.elasticsearch.cluster.metadata.DesiredNodes;
import org.elasticsearch.cluster.metadata.DesiredNodesMetadata;
import org.elasticsearch.cluster.metadata.DesiredNodesTestCase;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.elasticsearch.cluster.metadata.DesiredNodesTestCase.randomDesiredNode;
import static org.elasticsearch.cluster.metadata.DesiredNodesTestCase.randomDesiredNodes;
import static org.elasticsearch.common.util.concurrent.EsExecutors.NODE_PROCESSORS_SETTING;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_TCP_KEEP_IDLE;
import static org.elasticsearch.node.Node.NODE_EXTERNAL_ID_SETTING;
import static org.elasticsearch.node.NodeRoleSettings.NODE_ROLES_SETTING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class TransportDesiredNodesActionsIT extends ESIntegTestCase {

    @After
    public void cleanDesiredNodes() {
        deleteDesiredNodes();
    }

    public void testUpdateDesiredNodes() {
        final DesiredNodes desiredNodes = putRandomDesiredNodes();

        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final DesiredNodesMetadata metadata = state.metadata().custom(DesiredNodesMetadata.TYPE);
        assertThat(metadata, is(notNullValue()));
        final DesiredNodes latestDesiredNodes = metadata.getLatestDesiredNodes();
        assertThat(latestDesiredNodes, is(equalTo(desiredNodes)));
    }

    public void testUpdateDesiredNodesIsIdempotent() {
        final DesiredNodes desiredNodes = putRandomDesiredNodes();
        updateDesiredNodes(desiredNodes);

        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final DesiredNodesMetadata metadata = state.metadata().custom(DesiredNodesMetadata.TYPE);
        assertThat(metadata, is(notNullValue()));
        final DesiredNodes latestDesiredNodes = metadata.getLatestDesiredNodes();
        assertThat(latestDesiredNodes, is(equalTo(desiredNodes)));
    }

    public void testGoingBackwardsWithinTheSameHistoryIsForbidden() {
        final DesiredNodes desiredNodes = putRandomDesiredNodes();
        final DesiredNodes backwardsDesiredNodes = new DesiredNodes(
            desiredNodes.historyID(),
            desiredNodes.version() - 1,
            desiredNodes.nodes()
        );

        final VersionConflictException exception = expectThrows(
            VersionConflictException.class,
            () -> updateDesiredNodes(backwardsDesiredNodes)
        );
        assertThat(exception.getMessage(), containsString("has been superseded by version"));
    }

    public void testSameVersionWithDifferentContentIsForbidden() {
        final DesiredNodes desiredNodes = putRandomDesiredNodes();
        final DesiredNodes backwardsDesiredNodes = new DesiredNodes(
            desiredNodes.historyID(),
            desiredNodes.version(),
            randomList(1, 10, () -> randomDesiredNode(Version.CURRENT, (settings) -> {}))
        );

        final IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> updateDesiredNodes(backwardsDesiredNodes)
        );
        assertThat(exception.getMessage(), containsString("already exists with a different definition"));
    }

    public void testCanMoveToANewHistory() {
        final DesiredNodes desiredNodes = putRandomDesiredNodes();

        {
            final ClusterState state = client().admin().cluster().prepareState().get().getState();
            final DesiredNodesMetadata metadata = state.metadata().custom(DesiredNodesMetadata.TYPE);
            assertThat(metadata, is(notNullValue()));
            final DesiredNodes latestDesiredNodes = metadata.getLatestDesiredNodes();
            assertThat(latestDesiredNodes, is(equalTo(desiredNodes)));
        }

        final DesiredNodes newDesiredNodes = randomDesiredNodes();
        final UpdateDesiredNodesResponse response = updateDesiredNodes(newDesiredNodes);
        assertThat(response.hasReplacedExistingHistoryId(), is(equalTo(true)));

        {
            final ClusterState state = client().admin().cluster().prepareState().get().getState();
            final DesiredNodesMetadata metadata = state.metadata().custom(DesiredNodesMetadata.TYPE);
            assertThat(metadata, is(notNullValue()));
            final DesiredNodes latestDesiredNodes = metadata.getLatestDesiredNodes();
            assertThat(latestDesiredNodes, is(equalTo(newDesiredNodes)));
        }
    }

    public void testAtLeastOneMaterNodeIsExpected() {
        {
            final DesiredNodes desiredNodes = randomDesiredNodes(settings -> settings.put(NODE_ROLES_SETTING.getKey(), "data_hot"));

            final IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> updateDesiredNodes(desiredNodes));
            assertThat(exception.getMessage(), containsString("nodes must contain at least one master node"));
        }

        {
            final DesiredNodes desiredNodes = randomDesiredNodes(settings -> {
                if (randomBoolean()) {
                    settings.put(NODE_ROLES_SETTING.getKey(), "master");
                }
            });

            updateDesiredNodes(desiredNodes);
        }
    }

    public void testSettingsAreValidated() {
        final DesiredNodes desiredNodes = randomDesiredNodes(
            settings -> settings.put(SETTING_HTTP_TCP_KEEP_IDLE.getKey(), Integer.MIN_VALUE)
        );

        final IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> updateDesiredNodes(desiredNodes));
        assertThat(exception.getMessage(), containsString("Nodes with ids"));
        assertThat(exception.getMessage(), containsString("contain invalid settings"));
        assertThat(exception.getSuppressed().length > 0, is(equalTo(true)));
        assertThat(
            exception.getSuppressed()[0].getMessage(),
            containsString("Failed to parse value [-2147483648] for setting [http.tcp.keep_idle] must be >= -1")
        );
    }

    public void testNodeVersionIsValidated() {
        final DesiredNodes desiredNodes = randomDesiredNodes(Version.CURRENT.previousMajor(), settings -> {});

        final IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> updateDesiredNodes(desiredNodes));
        assertThat(exception.getMessage(), containsString("Nodes with ids"));
        assertThat(exception.getMessage(), containsString("contain invalid settings"));
        assertThat(exception.getSuppressed().length > 0, is(equalTo(true)));
        assertThat(exception.getSuppressed()[0].getMessage(), containsString("Illegal node version"));
    }

    public void testUnknownSettingsAreForbiddenInKnownVersions() {
        final DesiredNodes desiredNodes = randomDesiredNodes(
            settings -> { settings.put("desired_nodes.random_setting", Integer.MIN_VALUE); }
        );

        final IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> updateDesiredNodes(desiredNodes));
        assertThat(exception.getMessage(), containsString("Nodes with ids"));
        assertThat(exception.getMessage(), containsString("contain invalid settings"));
        assertThat(exception.getSuppressed().length > 0, is(equalTo(true)));
        assertThat(exception.getSuppressed()[0].getMessage(), containsString("unknown setting [desired_nodes.random_setting]"));
    }

    public void testUnknownSettingsAreAllowedInFutureVersions() {
        final DesiredNodes desiredNodes = randomDesiredNodes(
            Version.fromString("99.9.0"),
            settings -> { settings.put("desired_nodes.random_setting", Integer.MIN_VALUE); }
        );

        updateDesiredNodes(desiredNodes);

        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final DesiredNodesMetadata metadata = state.metadata().custom(DesiredNodesMetadata.TYPE);
        assertThat(metadata, is(notNullValue()));
        final DesiredNodes latestDesiredNodes = metadata.getLatestDesiredNodes();
        assertThat(latestDesiredNodes, is(equalTo(desiredNodes)));
    }

    public void testNodeProcessorsGetValidatedWithDesiredNodeProcessors() {
        final int numProcessors = Math.max(Runtime.getRuntime().availableProcessors() + 1, 2048);

        {
            final Consumer<Settings.Builder> settingsConsumer = (settings) -> settings.put(
                NODE_PROCESSORS_SETTING.getKey(),
                numProcessors + 1
            );
            final DesiredNodes desiredNodes = new DesiredNodes(
                UUIDs.randomBase64UUID(),
                randomIntBetween(1, 20),
                randomList(1, 20, () -> randomDesiredNode(Version.CURRENT, numProcessors, settingsConsumer))
            );

            final IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> updateDesiredNodes(desiredNodes));
            assertThat(exception.getMessage(), containsString("Nodes with ids"));
            assertThat(exception.getMessage(), containsString("contain invalid settings"));
            assertThat(exception.getSuppressed().length > 0, is(equalTo(true)));
            assertThat(
                exception.getSuppressed()[0].getMessage(),
                containsString(
                    String.format(
                        Locale.ROOT,
                        "Failed to parse value [%d] for setting [node.processors] must be <= %d",
                        numProcessors + 1,
                        numProcessors
                    )
                )
            );
        }

        {
            // This test verifies that the validation doesn't throw on desired nodes
            // with a higher number of available processors than the node running the tests.
            final Consumer<Settings.Builder> settingsConsumer = (settings) -> settings.put(NODE_PROCESSORS_SETTING.getKey(), numProcessors);
            final DesiredNodes desiredNodes = new DesiredNodes(
                UUIDs.randomBase64UUID(),
                randomIntBetween(1, 20),
                randomList(1, 20, () -> randomDesiredNode(Version.CURRENT, numProcessors, settingsConsumer))
            );

            updateDesiredNodes(desiredNodes);

            final ClusterState state = client().admin().cluster().prepareState().get().getState();
            final DesiredNodesMetadata metadata = state.metadata().custom(DesiredNodesMetadata.TYPE);
            assertThat(metadata, is(notNullValue()));
            final DesiredNodes latestDesiredNodes = metadata.getLatestDesiredNodes();
            assertThat(latestDesiredNodes, is(equalTo(desiredNodes)));
            assertThat(latestDesiredNodes.nodes().isEmpty(), is(equalTo(false)));
            assertThat(
                latestDesiredNodes.nodes().get(0).settings().get(NODE_PROCESSORS_SETTING.getKey()),
                is(equalTo(Integer.toString(numProcessors)))
            );
        }
    }

    public void testUpdateDesiredNodesTasksAreBatchedCorrectly() throws Exception {
        final Runnable unblockClusterStateUpdateThread = blockClusterStateUpdateThread();

        final List<DesiredNodes> proposedDesiredNodes = randomList(10, 20, DesiredNodesTestCase::randomDesiredNodes);
        final List<ActionFuture<UpdateDesiredNodesResponse>> updateDesiredNodesFutures = new ArrayList<>();
        for (DesiredNodes desiredNodes : proposedDesiredNodes) {
            final UpdateDesiredNodesRequest request = new UpdateDesiredNodesRequest(
                desiredNodes.historyID(),
                desiredNodes.version(),
                desiredNodes.nodes()
            );
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

        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final DesiredNodes latestDesiredNodes = DesiredNodesMetadata.latestFromClusterState(state);
        final DesiredNodes latestProposedDesiredNodes = proposedDesiredNodes.get(proposedDesiredNodes.size() - 1);
        assertThat(latestDesiredNodes, equalTo(latestProposedDesiredNodes));
    }

    public void testDeleteDesiredNodesTasksAreBatchedCorrectly() throws Exception {
        if (randomBoolean()) {
            putRandomDesiredNodes();
        }

        final Runnable unblockClusterStateUpdateThread = blockClusterStateUpdateThread();

        final List<ActionFuture<ActionResponse.Empty>> deleteDesiredNodesFutures = new ArrayList<>(15);
        for (int i = 0; i < 15; i++) {
            deleteDesiredNodesFutures.add(client().execute(DeleteDesiredNodesAction.INSTANCE, new DeleteDesiredNodesAction.Request()));
        }

        for (ActionFuture<ActionResponse.Empty> future : deleteDesiredNodesFutures) {
            assertThat(future.isDone(), is(equalTo(false)));
        }

        unblockClusterStateUpdateThread.run();

        for (ActionFuture<ActionResponse.Empty> future : deleteDesiredNodesFutures) {
            future.actionGet();
        }

        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final DesiredNodes latestDesiredNodes = DesiredNodesMetadata.latestFromClusterState(state);
        assertThat(latestDesiredNodes, is(nullValue()));
    }

    public void testGetLatestDesiredNodes() {
        expectThrows(ResourceNotFoundException.class, this::getLatestDesiredNodes);

        final DesiredNodes desiredNodes = putRandomDesiredNodes();
        assertThat(getLatestDesiredNodes(), is(equalTo(desiredNodes)));
    }

    public void testDeleteDesiredNodes() {
        final DesiredNodes desiredNodes = putRandomDesiredNodes();
        assertThat(getLatestDesiredNodes(), is(equalTo(desiredNodes)));

        deleteDesiredNodes();

        expectThrows(ResourceNotFoundException.class, this::getLatestDesiredNodes);
    }

    public void testEmptyExternalIDIsInvalid() {
        final Consumer<Settings.Builder> settingsConsumer = (settings) -> settings.put(NODE_EXTERNAL_ID_SETTING.getKey(), "    ");
        final DesiredNodes desiredNodes = new DesiredNodes(
            UUIDs.randomBase64UUID(),
            randomIntBetween(1, 20),
            randomList(1, 20, () -> randomDesiredNode(Version.CURRENT, settingsConsumer))
        );

        final IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> updateDesiredNodes(desiredNodes));
        assertThat(exception.getMessage(), containsString("Nodes with ids"));
        assertThat(exception.getMessage(), containsString("contain invalid settings"));
        assertThat(exception.getSuppressed().length > 0, is(equalTo(true)));
        assertThat(exception.getSuppressed()[0].getMessage(), containsString("[node.external_id] is missing or empty"));
    }

    private void deleteDesiredNodes() {
        final DeleteDesiredNodesAction.Request request = new DeleteDesiredNodesAction.Request();
        client().execute(DeleteDesiredNodesAction.INSTANCE, request).actionGet();
    }

    private DesiredNodes getLatestDesiredNodes() {
        final GetDesiredNodesAction.Request request = new GetDesiredNodesAction.Request();
        final GetDesiredNodesAction.Response response = client().execute(GetDesiredNodesAction.INSTANCE, request).actionGet();
        return response.getDesiredNodes();
    }

    private DesiredNodes putRandomDesiredNodes() {
        final DesiredNodes desiredNodes = randomDesiredNodes();
        updateDesiredNodes(desiredNodes);
        return desiredNodes;
    }

    private UpdateDesiredNodesResponse updateDesiredNodes(DesiredNodes desiredNodes) {
        final UpdateDesiredNodesRequest request = new UpdateDesiredNodesRequest(
            desiredNodes.historyID(),
            desiredNodes.version(),
            desiredNodes.nodes()
        );
        return client().execute(UpdateDesiredNodesAction.INSTANCE, request).actionGet();
    }

    private Runnable blockClusterStateUpdateThread() throws InterruptedException {
        final CountDownLatch unblockClusterStateUpdateTask = new CountDownLatch(1);
        final CountDownLatch blockingClusterStateUpdateTaskExecuting = new CountDownLatch(1);
        final ClusterService clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        clusterService.submitStateUpdateTask("blocking-task", new ClusterStateUpdateTask(Priority.IMMEDIATE) {
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
        }, ClusterStateTaskExecutor.unbatched());

        assertTrue(blockingClusterStateUpdateTaskExecuting.await(10, TimeUnit.SECONDS));
        return unblockClusterStateUpdateTask::countDown;
    }

}
