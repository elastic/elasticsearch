/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.node.TestDiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Type;

public class ShutdownTests extends ESTestCase {
    public static final DiscoveryNode localNode = TestDiscoveryNode.create("localNode");
    public static final DiscoveryNode otherNode = TestDiscoveryNode.create("otherNode");
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
                        state.nodes()
                    )
                )
            );

        TimeValue grace = new TimeValue(1000);
        SingleNodeShutdownMetadata shutdown = SingleNodeShutdownMetadata.builder()
            .setNodeId(localNode.getId())
            .setType(Type.SIGTERM)
            .setStartedAtMillis(start)
            .setGracePeriod(grace)
            .setReason("my reason")
            .build();
    }

    public void testKeepIfNodeExists() {
        TimeValue grace = new TimeValue(1000);
        Stream.of(localNode, otherNode)
            .forEach(
                node -> assertTrue(
                    Strings.format("existing node [%s] should be kept", node),
                    NodeSeenService.keepShutdownMetadata(
                        SingleNodeShutdownMetadata.builder()
                            .setNodeId(node.getId())
                            .setType(Type.SIGTERM)
                            .setStartedAtMillis(start)
                            .setGracePeriod(grace)
                            .setReason("my reason")
                            .build(),
                        start + (2 * grace.millis()), // would otherwise be cleaned up
                        state.nodes()
                    )
                )
            );
    }

    public void testDropIfLargeClockSkew() {
        TimeValue grace = new TimeValue(1000);
        assertFalse(
            NodeSeenService.keepShutdownMetadata(
                SingleNodeShutdownMetadata.builder()
                    .setNodeId("anotherNode")
                    .setType(Type.SIGTERM)
                    .setStartedAtMillis(start)
                    .setGracePeriod(grace)
                    .setReason("my reason")
                    .build(),
                start - 120_000,
                state.nodes()
            )
        );
    }

    public void testSomeNegativeClockSkewOK() {
        TimeValue grace = new TimeValue(1);
        assertTrue(
            NodeSeenService.keepShutdownMetadata(
                SingleNodeShutdownMetadata.builder()
                    .setNodeId("anotherNode")
                    .setType(Type.SIGTERM)
                    .setStartedAtMillis(start)
                    .setGracePeriod(grace)
                    .setReason("my reason")
                    .build(),
                start - 1_000,
                state.nodes()
            )
        );
    }

    public void testGraceSafety() {
        long graceMs = 60 * 60 * 1_000;
        TimeValue grace = new TimeValue(graceMs);
        SingleNodeShutdownMetadata shutdown = SingleNodeShutdownMetadata.builder()
            .setNodeId("anotherNode")
            .setType(Type.SIGTERM)
            .setStartedAtMillis(start)
            .setGracePeriod(grace)
            .setReason("my reason")
            .build();
        assertTrue(NodeSeenService.keepShutdownMetadata(shutdown, start + graceMs - 1, state.nodes()));
        assertTrue(NodeSeenService.keepShutdownMetadata(shutdown, start + graceMs, state.nodes()));
        assertTrue(NodeSeenService.keepShutdownMetadata(shutdown, start + graceMs + (graceMs / 10), state.nodes()));
        assertFalse(NodeSeenService.keepShutdownMetadata(shutdown, start + graceMs + (graceMs / 10) + 1, state.nodes()));
    }
}
