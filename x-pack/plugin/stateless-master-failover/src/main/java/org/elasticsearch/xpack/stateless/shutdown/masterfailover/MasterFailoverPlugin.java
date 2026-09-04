/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.shutdown.masterfailover;

import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ShutdownAwarePlugin;

import java.util.Collection;
import java.util.List;

public class MasterFailoverPlugin extends Plugin implements ShutdownAwarePlugin {

    private static final Logger logger = LogManager.getLogger(MasterFailoverPlugin.class);

    ClusterService clusterService;

    @Override
    public Collection<?> createComponents(PluginServices services) {
        this.clusterService = services.clusterService();
        return List.of();
    }

    @Override
    public boolean safeToShutdown(String nodeId, SingleNodeShutdownMetadata.Type shutdownType) {
        // called on the elected master (in a cluster state listener) so we only need to check our own node ID
        return nodeId.equals(clusterService.state().nodes().getLocalNodeId()) == false;
    }

    @Override
    public void signalShutdown(Collection<String> shutdownNodeIds) {
        // master handles shutdown signal via StatelessElectionStrategy#nodeMayWinElection
    }
}
