/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.desirednodes;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.NoMasterBlockService;
import org.elasticsearch.cluster.desirednodes.DesiredNodesSettingsValidator;
import org.elasticsearch.cluster.metadata.DesiredNode;
import org.elasticsearch.cluster.metadata.DesiredNodeSerializationTests;
import org.elasticsearch.cluster.metadata.DesiredNodes;
import org.elasticsearch.cluster.metadata.DesiredNodesMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static org.elasticsearch.action.admin.cluster.desirednodes.UpdateDesiredNodesRequestSerializationTests.randomUpdateDesiredNodesRequest;
import static org.elasticsearch.cluster.metadata.DesiredNodeSerializationTests.randomDesiredNode;
import static org.elasticsearch.cluster.metadata.DesiredNodesMetadataSerializationTests.randomDesiredNodesMetadata;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class TransportUpdateDesiredNodesActionTests extends ESTestCase {

    public static final DesiredNodesSettingsValidator NO_OP_SETTINGS_VALIDATOR = new DesiredNodesSettingsValidator(null, null, null) {
        @Override
        public void validateSettings(DesiredNode node) {}
    };

    public void testWriteBlocks() {
        final TransportUpdateDesiredNodesAction action = new TransportUpdateDesiredNodesAction(
            mock(TransportService.class),
            mock(ClusterService.class),
            mock(ThreadPool.class),
            mock(ActionFilters.class),
            mock(IndexNameExpressionResolver.class),
            NO_OP_SETTINGS_VALIDATOR
        );

        final ClusterBlocks blocks = ClusterBlocks.builder()
            .addGlobalBlock(
                randomFrom(
                    Metadata.CLUSTER_READ_ONLY_BLOCK,
                    Metadata.CLUSTER_READ_ONLY_ALLOW_DELETE_BLOCK,
                    NoMasterBlockService.NO_MASTER_BLOCK_WRITES
                )
            )
            .build();
        final ClusterState state = ClusterState.builder(new ClusterName(randomAlphaOfLength(10))).blocks(blocks).build();
        final ClusterBlockException e = action.checkBlock(randomUpdateDesiredNodesRequest(), state);
        assertThat(e, not(nullValue()));
    }

    public void testNoBlocks() {
        final TransportUpdateDesiredNodesAction action = new TransportUpdateDesiredNodesAction(
            mock(TransportService.class),
            mock(ClusterService.class),
            mock(ThreadPool.class),
            mock(ActionFilters.class),
            mock(IndexNameExpressionResolver.class),
            NO_OP_SETTINGS_VALIDATOR
        );

        final ClusterBlocks blocks = ClusterBlocks.builder().build();
        final ClusterState state = ClusterState.builder(new ClusterName(randomAlphaOfLength(10))).blocks(blocks).build();
        final ClusterBlockException e = action.checkBlock(randomUpdateDesiredNodesRequest(), state);
        assertThat(e, is(nullValue()));
    }

    public void testUpdateDesiredNodes() {
        final Metadata.Builder metadataBuilder = Metadata.builder();
        boolean containsDesiredNodes = false;
        if (randomBoolean()) {
            containsDesiredNodes = randomBoolean();
            final DesiredNodesMetadata desiredNodesMetadata = containsDesiredNodes
                ? DesiredNodesMetadata.EMPTY
                : randomDesiredNodesMetadata();
            metadataBuilder.putCustom(DesiredNodesMetadata.TYPE, desiredNodesMetadata);
        }

        final ClusterState currentClusterState = ClusterState.builder(new ClusterName(randomAlphaOfLength(10)))
            .metadata(metadataBuilder)
            .build();

        UpdateDesiredNodesRequest request = randomUpdateDesiredNodesRequest();
        if (containsDesiredNodes && randomBoolean()) {
            // increase the version for the current history and maybe modify the nodes
            final DesiredNodesMetadata currentDesiredNodesMetadata = currentClusterState.metadata().custom(DesiredNodesMetadata.TYPE);
            final DesiredNodes desiredNodes = currentDesiredNodesMetadata.getCurrentDesiredNodes();
            final List<DesiredNode> updatedNodes = randomSubsetOf(randomIntBetween(1, desiredNodes.nodes().size()), desiredNodes.nodes());
            request = new UpdateDesiredNodesRequest(desiredNodes.historyID(), desiredNodes.version() + 1, updatedNodes);
        }

        final ClusterState updatedClusterState = TransportUpdateDesiredNodesAction.updateDesiredNodes(
            currentClusterState,
            NO_OP_SETTINGS_VALIDATOR,
            request
        );

        final DesiredNodesMetadata desiredNodesMetadata = updatedClusterState.metadata().custom(DesiredNodesMetadata.TYPE);
        assertThat(desiredNodesMetadata, is(notNullValue()));

        final DesiredNodes desiredNodes = desiredNodesMetadata.getCurrentDesiredNodes();
        assertThat(desiredNodes, is(notNullValue()));
        assertThat(desiredNodes.historyID(), is(equalTo(request.getHistoryID())));
        assertThat(desiredNodes.version(), is(equalTo(request.getVersion())));
        assertThat(desiredNodes.nodes(), is(equalTo(request.getNodes())));
    }

    public void testUpdatesAreIdempotent() {
        final DesiredNodesMetadata desiredNodesMetadata = randomDesiredNodesMetadata();
        final ClusterState currentClusterState = ClusterState.builder(new ClusterName(randomAlphaOfLength(10)))
            .metadata(Metadata.builder().putCustom(DesiredNodesMetadata.TYPE, desiredNodesMetadata).build())
            .build();

        final DesiredNodes currentDesiredNodes = desiredNodesMetadata.getCurrentDesiredNodes();
        final UpdateDesiredNodesRequest request = new UpdateDesiredNodesRequest(
            currentDesiredNodes.historyID(),
            currentDesiredNodes.version(),
            currentDesiredNodes.nodes()
        );

        final ClusterState updatedClusterState = TransportUpdateDesiredNodesAction.updateDesiredNodes(
            currentClusterState,
            NO_OP_SETTINGS_VALIDATOR,
            request
        );

        final DesiredNodesMetadata updatedDesiredNodesMetadata = updatedClusterState.metadata().custom(DesiredNodesMetadata.TYPE);
        assertThat(updatedDesiredNodesMetadata, is(notNullValue()));
        assertThat(updatedDesiredNodesMetadata.getCurrentDesiredNodes(), is(notNullValue()));
        assertThat(updatedDesiredNodesMetadata.getCurrentDesiredNodes(), is(equalTo(currentDesiredNodes)));
    }

    public void testUpdateSameHistoryAndVersionWithDifferentContentsFails() {
        final DesiredNodesMetadata desiredNodesMetadata = randomDesiredNodesMetadata();
        final ClusterState currentClusterState = ClusterState.builder(new ClusterName(randomAlphaOfLength(10)))
            .metadata(Metadata.builder().putCustom(DesiredNodesMetadata.TYPE, desiredNodesMetadata).build())
            .build();

        final DesiredNodes currentDesiredNodes = desiredNodesMetadata.getCurrentDesiredNodes();
        final UpdateDesiredNodesRequest request = new UpdateDesiredNodesRequest(
            currentDesiredNodes.historyID(),
            currentDesiredNodes.version(),
            randomList(1, 10, DesiredNodeSerializationTests::randomDesiredNode)
        );

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> TransportUpdateDesiredNodesAction.updateDesiredNodes(currentClusterState, NO_OP_SETTINGS_VALIDATOR, request)
        );
        assertThat(exception.getMessage(), containsString("todo"));
    }

    public void testBackwardUpdatesFails() {
        final DesiredNodesMetadata desiredNodesMetadata = randomDesiredNodesMetadata();
        final ClusterState currentClusterState = ClusterState.builder(new ClusterName(randomAlphaOfLength(10)))
            .metadata(Metadata.builder().putCustom(DesiredNodesMetadata.TYPE, desiredNodesMetadata).build())
            .build();

        final DesiredNodes currentDesiredNodes = desiredNodesMetadata.getCurrentDesiredNodes();
        final UpdateDesiredNodesRequest request = new UpdateDesiredNodesRequest(
            currentDesiredNodes.historyID(),
            currentDesiredNodes.version() - 1,
            currentDesiredNodes.nodes()
        );

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> TransportUpdateDesiredNodesAction.updateDesiredNodes(currentClusterState, NO_OP_SETTINGS_VALIDATOR, request)
        );
        assertThat(exception.getMessage(), containsString("todo"));
    }

    public void testUnknownSettingsInKnownVersionsFails() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, Collections.emptySet(), Collections.emptySet());
        final DesiredNodesSettingsValidator desiredNodesSettingsValidator = new DesiredNodesSettingsValidator(
            clusterSettings,
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        final ClusterState currentClusterState = ClusterState.builder(new ClusterName(randomAlphaOfLength(10))).build();
        final UpdateDesiredNodesRequest request = randomUpdateDesiredNodesRequest();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> TransportUpdateDesiredNodesAction.updateDesiredNodes(currentClusterState, desiredNodesSettingsValidator, request)
        );
        assertThat(exception.getMessage(), containsString("has unknown settings"));
    }

    public void testUnknownSettingsInUnknownVersions() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, Collections.emptySet(), Collections.emptySet());
        final DesiredNodesSettingsValidator desiredNodesSettingsValidator = new DesiredNodesSettingsValidator(
            clusterSettings,
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        final ClusterState currentClusterState = ClusterState.builder(new ClusterName(randomAlphaOfLength(10))).build();
        final UpdateDesiredNodesRequest request = randomUpdateDesiredNodesRequest(Version.fromString("99.9.0"));

        final ClusterState updatedClusterState = TransportUpdateDesiredNodesAction.updateDesiredNodes(
            currentClusterState,
            desiredNodesSettingsValidator,
            request
        );

        final DesiredNodesMetadata desiredNodesMetadata = updatedClusterState.metadata().custom(DesiredNodesMetadata.TYPE);
        assertThat(desiredNodesMetadata, is(notNullValue()));

        final DesiredNodes desiredNodes = desiredNodesMetadata.getCurrentDesiredNodes();
        assertThat(desiredNodes, is(notNullValue()));
        assertThat(desiredNodes.historyID(), is(equalTo(request.getHistoryID())));
        assertThat(desiredNodes.version(), is(equalTo(request.getVersion())));
        assertThat(desiredNodes.nodes(), is(equalTo(request.getNodes())));
    }

    public void testSettingsValidation() {
        final Set<Setting<?>> availableSettings = Set.of(
            Setting.intSetting("test.a", 1, Setting.Property.NodeScope),
            Setting.floatSetting("test.b", 1, Setting.Property.NodeScope)
        );
        final Consumer<Settings.Builder> settingsProvider = settings -> {
            if (randomBoolean()) {
                settings.put("test.a", randomAlphaOfLength(10));
            } else {
                settings.put("test.b", randomAlphaOfLength(10));
            }
        };

        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, availableSettings, Collections.emptySet());
        final DesiredNodesSettingsValidator desiredNodesSettingsValidator = new DesiredNodesSettingsValidator(
            clusterSettings,
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        final ClusterState currentClusterState = ClusterState.builder(new ClusterName(randomAlphaOfLength(10))).build();
        final UpdateDesiredNodesRequest request = new UpdateDesiredNodesRequest(
            UUIDs.randomBase64UUID(),
            randomIntBetween(1, 20),
            randomList(1, 20, () -> randomDesiredNode(Version.CURRENT, settingsProvider))
        );

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> TransportUpdateDesiredNodesAction.updateDesiredNodes(currentClusterState, desiredNodesSettingsValidator, request)
        );
        assertThat(exception.getMessage(), containsString("Failed to parse value"));
    }
}
