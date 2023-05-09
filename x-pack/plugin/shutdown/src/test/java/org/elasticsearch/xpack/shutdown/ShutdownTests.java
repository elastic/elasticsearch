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
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Type;
import static org.elasticsearch.xpack.shutdown.NodeSeenService.RemoveSigtermShutdownTaskExecutor;

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
                t -> assertFalse(
                    Strings.format("type [%s] is not a stale sigterm", t),
                    RemoveSigtermShutdownTaskExecutor.isSigtermShutdownStale(
                        SingleNodeShutdownMetadata.builder()
                            .setNodeId(localNode.getId())
                            .setTargetNodeName(t == Type.REPLACE ? localNode.getId() : null)
                            .setType(t)
                            .setStartedAtMillis(start)
                            .setReason("my reason")
                            .build(),
                        now,
                        state.nodes()::nodeExists
                    )
                )
            );
    }

    public void testKeepIfNodeExists() {
        Stream.of(localNode, otherNode).forEach(node -> {
            var shutdown = SingleNodeShutdownMetadata.builder()
                .setNodeId(node.getId())
                .setType(Type.SIGTERM)
                .setStartedAtMillis(start)
                .setReason("my reason")
                .build();
            assertFalse(
                Strings.format("existing node [%s] is not stale", node),
                RemoveSigtermShutdownTaskExecutor.isSigtermShutdownStale(
                    shutdown,
                    start + (2 * shutdown.getGracePeriod().millis()), // would otherwise be cleaned up
                    state.nodes()::nodeExists
                )
            );
        });
    }

    public void testDropIfLargeClockSkew() {
        assertTrue(
            RemoveSigtermShutdownTaskExecutor.isSigtermShutdownStale(
                SingleNodeShutdownMetadata.builder()
                    .setNodeId("anotherNode")
                    .setType(Type.SIGTERM)
                    .setStartedAtMillis(start)
                    .setReason("my reason")
                    .build(),
                start - 120_000,
                state.nodes()::nodeExists
            )
        );
    }

    public void testSomeNegativeClockSkewOK() {
        assertFalse(
            RemoveSigtermShutdownTaskExecutor.isSigtermShutdownStale(
                SingleNodeShutdownMetadata.builder()
                    .setNodeId("anotherNode")
                    .setType(Type.SIGTERM)
                    .setStartedAtMillis(start)
                    .setReason("my reason")
                    .build(),
                start - 1_000,
                state.nodes()::nodeExists
            )
        );
    }

    public void testGraceSafety() {
        SingleNodeShutdownMetadata shutdown = SingleNodeShutdownMetadata.builder()
            .setNodeId("anotherNode")
            .setType(Type.SIGTERM)
            .setStartedAtMillis(start)
            .setReason("my reason")
            .build();
        long graceMs = shutdown.getGracePeriod().millis();
        Predicate<String> nodeExists = state.nodes()::nodeExists;
        assertFalse(RemoveSigtermShutdownTaskExecutor.isSigtermShutdownStale(shutdown, start + graceMs - 1, nodeExists));
        assertFalse(RemoveSigtermShutdownTaskExecutor.isSigtermShutdownStale(shutdown, start + graceMs, nodeExists));
        assertFalse(RemoveSigtermShutdownTaskExecutor.isSigtermShutdownStale(shutdown, start + graceMs + (graceMs / 10), nodeExists));
        assertTrue(RemoveSigtermShutdownTaskExecutor.isSigtermShutdownStale(shutdown, start + graceMs + (graceMs / 10) + 1, nodeExists));
    }
}
