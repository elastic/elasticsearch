/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination.stateless;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.coordination.Reconfigurator;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;

import java.util.Set;

public class SingleNodeReconfigurator extends Reconfigurator {
    public SingleNodeReconfigurator(Settings settings, ClusterSettings clusterSettings) {
        super(settings, clusterSettings);
    }

    @Override
    public CoordinationMetadata.VotingConfiguration reconfigure(
        Set<DiscoveryNode> liveNodes,
        Set<String> retiredNodeIds,
        DiscoveryNode currentMaster,
        CoordinationMetadata.VotingConfiguration currentConfig
    ) {
        return currentConfig;
    }

    @Override
    public ClusterState maybeReconfigureAfterNewMasterIsElected(ClusterState clusterState) {
        return ClusterState.builder(clusterState)
            .metadata(
                Metadata.builder(clusterState.metadata())
                    .coordinationMetadata(
                        CoordinationMetadata.builder(clusterState.coordinationMetadata())
                            .lastAcceptedConfiguration(
                                new CoordinationMetadata.VotingConfiguration(Set.of(clusterState.nodes().getMasterNodeId()))
                            )
                            .build()
                    )
                    .build()
            )
            .build();
    }
}
