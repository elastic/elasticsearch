/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.desirednodes;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.desirednodes.VersionConflictException;
import org.elasticsearch.cluster.metadata.DesiredNodes;
import org.elasticsearch.cluster.metadata.DesiredNodesMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;

import java.util.function.Consumer;

import static org.elasticsearch.cluster.metadata.DesiredNodesTestCase.randomDesiredNode;
import static org.elasticsearch.cluster.metadata.DesiredNodesTestCase.randomDesiredNodes;
import static org.elasticsearch.common.util.concurrent.EsExecutors.NODE_PROCESSORS_SETTING;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_TCP_KEEP_IDLE;
import static org.elasticsearch.node.Node.NODE_EXTERNAL_ID_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
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

        final DesiredNodes newDesiredNodes = putRandomDesiredNodes();
        assertThat(newDesiredNodes.historyID(), is(not(equalTo(desiredNodes.historyID()))));

        {
            final ClusterState state = client().admin().cluster().prepareState().get().getState();
            final DesiredNodesMetadata metadata = state.metadata().custom(DesiredNodesMetadata.TYPE);
            assertThat(metadata, is(notNullValue()));
            final DesiredNodes latestDesiredNodes = metadata.getLatestDesiredNodes();
            assertThat(latestDesiredNodes, is(equalTo(newDesiredNodes)));
        }
    }

    public void testSettingsAreValidated() {
        final DesiredNodes desiredNodes = randomDesiredNodes(
            settings -> settings.put(SETTING_HTTP_TCP_KEEP_IDLE.getKey(), Integer.MIN_VALUE)
        );

        final IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> updateDesiredNodes(desiredNodes));
        assertThat(exception.getMessage(), containsString("Nodes in positions"));
        assertThat(exception.getMessage(), containsString("contain invalid settings"));
        assertThat(exception.getSuppressed().length > 0, is(equalTo(true)));
        assertThat(
            exception.getSuppressed()[0].getMessage(),
            containsString("Failed to parse value [-2147483648] for setting [http.tcp.keep_idle] must be >= -1")
        );
    }

    public void testUnknownSettingsAreForbiddenInKnownVersions() {
        final DesiredNodes desiredNodes = randomDesiredNodes(
            settings -> { settings.put("desired_nodes.random_setting", Integer.MIN_VALUE); }
        );

        final IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> updateDesiredNodes(desiredNodes));
        assertThat(exception.getMessage(), containsString("Nodes in positions"));
        assertThat(exception.getMessage(), containsString("contain invalid settings"));
        assertThat(exception.getSuppressed().length > 0, is(equalTo(true)));
        assertThat(exception.getSuppressed()[0].getMessage(), containsString("has unknown settings [desired_nodes.random_setting]"));
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

    public void testSomeSettingsCanBeOverridden() {
        final int numProcessors = Math.max(Runtime.getRuntime().availableProcessors() + 1, 2048);
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
        assertThat(latestDesiredNodes.nodes().get(0).settings().get(NODE_PROCESSORS_SETTING.getKey()), is(equalTo("2048")));
    }

    public void testGetLatestDesiredNodes() {
        assertThat(getLatestDesiredNodes(), is(nullValue()));

        final DesiredNodes desiredNodes = putRandomDesiredNodes();
        assertThat(getLatestDesiredNodes(), is(equalTo(desiredNodes)));
    }

    public void testDeleteDesiredNodes() {
        final DesiredNodes desiredNodes = putRandomDesiredNodes();
        assertThat(getLatestDesiredNodes(), is(equalTo(desiredNodes)));

        deleteDesiredNodes();

        assertThat(getLatestDesiredNodes(), is(nullValue()));
    }

    public void testEmptyExternalIDIsInvalid() {
        final Consumer<Settings.Builder> settingsConsumer = (settings) -> settings.put(NODE_EXTERNAL_ID_SETTING.getKey(), "    ");
        final DesiredNodes desiredNodes = new DesiredNodes(
            UUIDs.randomBase64UUID(),
            randomIntBetween(1, 20),
            randomList(1, 20, () -> randomDesiredNode(Version.CURRENT, settingsConsumer))
        );

        final IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> updateDesiredNodes(desiredNodes));
        assertThat(exception.getMessage(), containsString("Nodes in positions"));
        assertThat(exception.getMessage(), containsString("contain invalid settings"));
        assertThat(exception.getSuppressed().length > 0, is(equalTo(true)));
        assertThat(exception.getSuppressed()[0].getMessage(), containsString("[node.external_id] is missing or empty"));
    }

    private void deleteDesiredNodes() {
        final DeleteDesiredNodesAction.Request request = new DeleteDesiredNodesAction.Request();
        assertAcked(client().execute(DeleteDesiredNodesAction.INSTANCE, request).actionGet());
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

    private void updateDesiredNodes(DesiredNodes desiredNodes) {
        final UpdateDesiredNodesRequest request = new UpdateDesiredNodesRequest(
            desiredNodes.historyID(),
            desiredNodes.version(),
            desiredNodes.nodes()
        );
        assertAcked(client().execute(UpdateDesiredNodesAction.INSTANCE, request).actionGet());
    }

}
