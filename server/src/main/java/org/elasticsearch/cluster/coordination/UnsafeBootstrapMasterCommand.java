/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import joptsimple.OptionSet;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.gateway.PersistedClusterStateService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Locale;

public class UnsafeBootstrapMasterCommand extends ElasticsearchNodeCommand {

    static final String CLUSTER_STATE_TERM_VERSION_MSG_FORMAT = "Current node cluster state (term, version) pair is (%s, %s)";
    static final String CONFIRMATION_MSG = DELIMITER
        + "\n"
        + "You should only run this tool if you have permanently lost half or more\n"
        + "of the master-eligible nodes in this cluster, and you cannot restore the\n"
        + "cluster from a snapshot. This tool can cause arbitrary data loss and its\n"
        + "use should be your last resort. If you have multiple surviving master\n"
        + "eligible nodes, you should run this tool on the node with the highest\n"
        + "cluster state (term, version) pair.\n"
        + "\n"
        + "Do you want to proceed?\n";

    static final String NOT_MASTER_NODE_MSG = "unsafe-bootstrap tool can only be run on master eligible node";

    static final String EMPTY_LAST_COMMITTED_VOTING_CONFIG_MSG =
        "last committed voting voting configuration is empty, cluster has never been bootstrapped?";

    static final String MASTER_NODE_BOOTSTRAPPED_MSG = "Master node was successfully bootstrapped";
    static final Setting<String> UNSAFE_BOOTSTRAP = ClusterService.USER_DEFINED_METADATA.getConcreteSetting(
        "cluster.metadata.unsafe-bootstrap"
    );

    UnsafeBootstrapMasterCommand() {
        super("Forces the successful election of the current node after the permanent loss of the half or more master-eligible nodes");
    }

    @Override
    protected boolean validateBeforeLock(Terminal terminal, Environment env) {
        Settings settings = env.settings();
        terminal.println(Terminal.Verbosity.VERBOSE, "Checking node.master setting");
        Boolean master = DiscoveryNode.isMasterNode(settings);
        if (master == false) {
            throw new ElasticsearchException(NOT_MASTER_NODE_MSG);
        }

        return true;
    }

    protected void processDataPaths(Terminal terminal, Path[] dataPaths, OptionSet options, Environment env) throws IOException {
        final PersistedClusterStateService persistedClusterStateService = createPersistedClusterStateService(env.settings(), dataPaths);

        final Tuple<Long, ClusterState> state = loadTermAndClusterState(persistedClusterStateService, env);
        final ClusterState oldClusterState = state.v2();

        final Metadata metadata = oldClusterState.metadata();

        final CoordinationMetadata coordinationMetadata = metadata.coordinationMetadata();
        if (coordinationMetadata == null
            || coordinationMetadata.getLastCommittedConfiguration() == null
            || coordinationMetadata.getLastCommittedConfiguration().isEmpty()) {
            throw new ElasticsearchException(EMPTY_LAST_COMMITTED_VOTING_CONFIG_MSG);
        }
        terminal.println(
            String.format(Locale.ROOT, CLUSTER_STATE_TERM_VERSION_MSG_FORMAT, coordinationMetadata.term(), metadata.version())
        );

        CoordinationMetadata newCoordinationMetadata = CoordinationMetadata.builder(coordinationMetadata)
            .clearVotingConfigExclusions()
            .lastAcceptedConfiguration(
                new CoordinationMetadata.VotingConfiguration(Collections.singleton(persistedClusterStateService.getNodeId()))
            )
            .lastCommittedConfiguration(
                new CoordinationMetadata.VotingConfiguration(Collections.singleton(persistedClusterStateService.getNodeId()))
            )
            .build();

        Settings persistentSettings = Settings.builder().put(metadata.persistentSettings()).put(UNSAFE_BOOTSTRAP.getKey(), true).build();
        Metadata.Builder newMetadata = Metadata.builder(metadata)
            .clusterUUID(Metadata.UNKNOWN_CLUSTER_UUID)
            .generateClusterUuidIfNeeded()
            .clusterUUIDCommitted(true)
            .persistentSettings(persistentSettings)
            .coordinationMetadata(newCoordinationMetadata);
        for (IndexMetadata indexMetadata : metadata.indices().values()) {
            newMetadata.put(
                IndexMetadata.builder(indexMetadata)
                    .settings(
                        Settings.builder()
                            .put(indexMetadata.getSettings())
                            .put(IndexMetadata.SETTING_HISTORY_UUID, UUIDs.randomBase64UUID())
                    )
            );
        }

        final ClusterState newClusterState = ClusterState.builder(oldClusterState).metadata(newMetadata).build();

        if (terminal.isPrintable(Terminal.Verbosity.VERBOSE)) {
            terminal.println(
                Terminal.Verbosity.VERBOSE,
                "[old cluster state = " + oldClusterState + ", new cluster state = " + newClusterState + "]"
            );
        }

        confirm(terminal, CONFIRMATION_MSG);

        try (PersistedClusterStateService.Writer writer = persistedClusterStateService.createWriter()) {
            writer.writeFullStateAndCommit(state.v1(), newClusterState);
        }

        terminal.println(MASTER_NODE_BOOTSTRAPPED_MSG);
    }
}
