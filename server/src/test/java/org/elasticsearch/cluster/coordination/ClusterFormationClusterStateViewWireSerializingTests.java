/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;

public class ClusterFormationClusterStateViewWireSerializingTests extends AbstractWireSerializingTestCase<
    ClusterFormationFailureHelper.ClusterFormationClusterStateView> {
    @Override
    protected Writeable.Reader<ClusterFormationFailureHelper.ClusterFormationClusterStateView> instanceReader() {
        return ClusterFormationFailureHelper.ClusterFormationClusterStateView::new;
    }

    @Override
    protected ClusterFormationFailureHelper.ClusterFormationClusterStateView createTestInstance() {
        String localNodeId = UUID.randomUUID().toString();
        return new ClusterFormationFailureHelper.ClusterFormationClusterStateView(
            ClusterState.builder(ClusterName.DEFAULT)
                .stateUUID("stateUUID")
                .nodes(
                    DiscoveryNodes.builder()
                        .masterNodeId(localNodeId)
                        .localNodeId(localNodeId)
                        .add(DiscoveryNodeUtils.create(localNodeId))
                        .build()
                )
                .version(randomLong())
                .metadata(
                    Metadata.builder()
                        .clusterUUID("clusterUUID")
                        .coordinationMetadata(
                            CoordinationMetadata.builder()
                                .term(1)
                                .lastCommittedConfiguration(
                                    new CoordinationMetadata.VotingConfiguration(Set.of("commitedConfigurationNodeId"))
                                )
                                .lastAcceptedConfiguration(
                                    new CoordinationMetadata.VotingConfiguration(Set.of("acceptedConfigurationNodeId"))
                                )
                                .addVotingConfigExclusion(
                                    new CoordinationMetadata.VotingConfigExclusion("excludedNodeId", "excludedNodeName")
                                )
                                .build()
                        )
                        .build()
                )
                .build(),
            randomLong()
        );
    }

    @Override
    protected ClusterFormationFailureHelper.ClusterFormationClusterStateView mutateInstance(
        ClusterFormationFailureHelper.ClusterFormationClusterStateView instance
    ) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
