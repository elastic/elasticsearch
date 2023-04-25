/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Type;

public class ShutdownTests extends ESTestCase {
    public static final DiscoveryNode localNode = new DiscoveryNode("localNode", buildNewFakeTransportAddress(), Version.CURRENT);
    public static final DiscoveryNode otherNode = new DiscoveryNode("otherNode", buildNewFakeTransportAddress(), Version.CURRENT);
    public static final ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
        .version(1L)
        .nodes(DiscoveryNodes.builder().add(localNode).add(otherNode).localNodeId(localNode.getId()).build())
        .metadata(
            Metadata.builder()
                .clusterUUID("clusteruuid")
                .coordinationMetadata(
                    CoordinationMetadata.builder()
                        .term(2)
                        .lastCommittedConfiguration(CoordinationMetadata.VotingConfiguration.EMPTY_CONFIG)
                        .lastAcceptedConfiguration(CoordinationMetadata.VotingConfiguration.EMPTY_CONFIG)
                        .build()
                )
                .build()
        )
        .stateUUID("stateuuid")
        .build();

    public static final long now = 123_000;
    public static final long start = 120_000;

    public void testKeepAllNonSigTerms() {

        // Keep all non-sigterms
        Arrays.stream(SingleNodeShutdownMetadata.Type.values())
            .filter(t -> t != Type.SIGTERM)
            .forEach(
                t -> assertTrue(
                    Strings.format("type [%s] should be kept", t),
                    NodeSeenService.keepShutdownMetadata(
                        SingleNodeShutdownMetadata.builder()
                            .setNodeId(localNode.getId())
                            .setTargetNodeName(t == Type.REPLACE ? localNode.getId() : null)
                            .setType(t)
                            .setStartedAtMillis(start)
                            .setReason("my reason")
                            .build(),
                        now,
                        state
                    )
                )
            );
        // assertThat(true, NodeSeenService.keepShutdownMetadata())

        TimeValue grace = new TimeValue(1000);
        SingleNodeShutdownMetadata shutdown = SingleNodeShutdownMetadata.builder()
            .setNodeId(localNode.getId())
            .setType(Type.SIGTERM)
            .setStartedAtMillis(start)
            .setGracefulShutdown(grace)
            .setReason("my reason")
            .build();
    }

    public void testKeepIfNodeExists() {
        Stream.of(localNode, otherNode)
            .forEach(
                node -> assertTrue(
                    Strings.format("existing node [%s] should be kept", node),
                    NodeSeenService.keepShutdownMetadata(
                        SingleNodeShutdownMetadata.builder()
                            .setNodeId(node.getId())
                            .setTargetNodeName(node.getId())
                            .setType(Type.SIGTERM)
                            .setStartedAtMillis(start)
                            .setReason("my reason")
                            .build(),
                        now,
                        state
                    )
                )
            );
    }
}
