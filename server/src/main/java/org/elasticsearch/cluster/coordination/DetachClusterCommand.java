/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import joptsimple.OptionSet;

import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.env.Environment;
import org.elasticsearch.gateway.PersistedClusterStateService;

import java.io.IOException;
import java.nio.file.Path;

public class DetachClusterCommand extends ElasticsearchNodeCommand {

    static final String NODE_DETACHED_MSG = "Node was successfully detached from the cluster";
    static final String CONFIRMATION_MSG = DELIMITER
        + "\n"
        + "You should only run this tool if you have permanently lost all of the\n"
        + "master-eligible nodes in this cluster and you cannot restore the cluster\n"
        + "from a snapshot, or you have already unsafely bootstrapped a new cluster\n"
        + "by running `elasticsearch-node unsafe-bootstrap` on a master-eligible\n"
        + "node that belonged to the same cluster as this node. This tool can cause\n"
        + "arbitrary data loss and its use should be your last resort.\n"
        + "\n"
        + "Do you want to proceed?\n";

    public DetachClusterCommand() {
        super("Detaches this node from its cluster, allowing it to unsafely join a new cluster");
    }

    @Override
    protected void processDataPaths(Terminal terminal, Path[] dataPaths, int nodeLockId, OptionSet options, Environment env)
        throws IOException {
        final PersistedClusterStateService persistedClusterStateService = createPersistedClusterStateService(env.settings(), dataPaths);

        terminal.println(Terminal.Verbosity.VERBOSE, "Loading cluster state");
        final ClusterState oldClusterState = loadTermAndClusterState(persistedClusterStateService, env).v2();
        final ClusterState newClusterState = ClusterState.builder(oldClusterState)
            .metadata(updateMetadata(oldClusterState.metadata()))
            .build();
        terminal.println(
            Terminal.Verbosity.VERBOSE,
            "[old cluster state = " + oldClusterState + ", new cluster state = " + newClusterState + "]"
        );

        confirm(terminal, CONFIRMATION_MSG);

        try (PersistedClusterStateService.Writer writer = persistedClusterStateService.createWriter()) {
            writer.writeFullStateAndCommit(updateCurrentTerm(), newClusterState);
        }

        terminal.println(NODE_DETACHED_MSG);
    }

    // package-private for tests
    static Metadata updateMetadata(Metadata oldMetadata) {
        final CoordinationMetadata coordinationMetadata = CoordinationMetadata.builder()
            .lastAcceptedConfiguration(CoordinationMetadata.VotingConfiguration.MUST_JOIN_ELECTED_MASTER)
            .lastCommittedConfiguration(CoordinationMetadata.VotingConfiguration.MUST_JOIN_ELECTED_MASTER)
            .term(0)
            .build();
        return Metadata.builder(oldMetadata).coordinationMetadata(coordinationMetadata).clusterUUIDCommitted(false).build();
    }

    // package-private for tests
    static long updateCurrentTerm() {
        return 0;
    }
}
