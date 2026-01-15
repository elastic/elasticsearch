/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.BuildVersion;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.monitor.StatusInfo;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.elasticsearch.monitor.StatusInfo.Status.HEALTHY;
import static org.elasticsearch.monitor.StatusInfo.Status.UNHEALTHY;

public class ClusterFormationStateWireSerializingTests extends AbstractWireSerializingTestCase<
    ClusterFormationFailureHelper.ClusterFormationState> {
    @Override
    protected Writeable.Reader<ClusterFormationFailureHelper.ClusterFormationState> instanceReader() {
        return ClusterFormationFailureHelper.ClusterFormationState::readFrom;
    }

    @Override
    protected ClusterFormationFailureHelper.ClusterFormationState createTestInstance() {
        Map<String, DiscoveryNode> masterEligibleNodes = new HashMap<>();
        for (int i = 0; i < randomIntBetween(0, 5); i++) {
            String nodeId = UUID.randomUUID().toString();
            masterEligibleNodes.put(nodeId, DiscoveryNodeUtils.create(nodeId));
        }
        return new ClusterFormationFailureHelper.ClusterFormationState(
            randomList(5, () -> randomAlphaOfLengthBetween(3, 6)),
            new ClusterFormationFailureHelper.ClusterFormationClusterStateView(
                DiscoveryNodeUtils.create(UUID.randomUUID().toString()),
                masterEligibleNodes,
                randomLong(),
                randomLong(),
                new CoordinationMetadata.VotingConfiguration(randomSet(0, 5, () -> UUID.randomUUID().toString())),
                new CoordinationMetadata.VotingConfiguration(randomSet(0, 5, () -> UUID.randomUUID().toString())),
                randomLong()
            ),
            randomList(0, 5, ESTestCase::buildNewFakeTransportAddress),
            randomList(1, 5, () -> DiscoveryNodeUtils.create(UUID.randomUUID().toString())),
            randomSet(1, 5, () -> DiscoveryNodeUtils.create(UUID.randomUUID().toString())),
            randomBoolean(),
            new StatusInfo(randomBoolean() ? HEALTHY : UNHEALTHY, randomAlphanumericOfLength(5)),
            randomList(
                0,
                5,
                () -> new JoinStatus(
                    DiscoveryNodeUtils.create(UUID.randomUUID().toString()),
                    randomLongBetween(0, 1000),
                    randomAlphaOfLengthBetween(0, 100),
                    randomTimeValue()
                )
            )
        );
    }

    public void testGoldenImageSerialization() throws IOException {
        // ClusterFormationState is basically unique in that it is only sent over the wire in a cluster that cannot form. Today we have no
        // BwC tests of this case, which puts us at risk of making a wire protocol change to this object and never noticing. It would be
        // pretty difficult to make such a test with real nodes, but we can at least catch inadvertent wire protocol changes by comparing
        // the serialized version of an object to a "golden image" captured at the time this test was written.

        final var nodes = new DiscoveryNode[8];
        for (int i = 0; i < nodes.length; i++) {
            // DiscoveryNode is pretty complex so there is some risk of its serialization changing from the golden format captured here.
            // We know its serialization is well-covered by BwC tests so that shouldn't be a problem.
            nodes[i] = DiscoveryNodeUtils.builder("node-" + i + "-id")
                .name("node-" + i + "-id")
                .ephemeralId("node-" + i + "-ephemeral-id")
                .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE))
                .address(new TransportAddress(TransportAddress.META_ADDRESS, i + 1))
                .version(
                    new VersionInformation(
                        BuildVersion.fromVersionId(9_12_34_56),
                        Version.fromId(9_23_45_67),
                        IndexVersion.fromId(9_45_67_89),
                        IndexVersion.fromId(9_34_56_78),
                        IndexVersion.fromId(9_56_78_90)
                    )
                )
                .build();
        }

        final var clusterFormationState = new ClusterFormationFailureHelper.ClusterFormationState(
            List.of("master-node-1", "master-node-2"),
            new ClusterFormationFailureHelper.ClusterFormationClusterStateView(
                nodes[0],
                Map.of(nodes[1].getId(), nodes[1]), // singleton for deterministic ordering
                3,
                4,
                CoordinationMetadata.VotingConfiguration.of(nodes[2]), // singleton for deterministic ordering
                CoordinationMetadata.VotingConfiguration.of(nodes[3]), // singleton for deterministic ordering
                5
            ),
            List.of(new TransportAddress(TransportAddress.META_ADDRESS, 5)),
            List.of(nodes[4], nodes[5]),
            Set.of(nodes[6]), // singleton for deterministic ordering
            false,
            new StatusInfo(HEALTHY, "healthy-status"),
            List.of(new JoinStatus(nodes[7], 6, "join-status-message", TimeValue.ONE_HOUR))
        );

        final byte[] bytes;
        try (var baos = new ByteArrayOutputStream()) {
            clusterFormationState.writeTo(new OutputStreamStreamOutput(baos));
            bytes = baos.toByteArray();
        }
        assertEquals("""
            Ag1tYXN0ZXItbm9kZS0xDW1hc3Rlci1ub2RlLTIJbm9kZS0wLWlkCW5vZGUtMC1pZBNub2RlLTAtZXBoZW1lcmFsLWlkBzAuMC4wLjAHMC4wLjAuMAQAAAAA\
            BzAuMC4wLjAAAAABAAEGbWFzdGVyAW0Ah9GzBJWZwQSOtboEkv3HBAlub2RlLTAtaWQBCW5vZGUtMS1pZAlub2RlLTEtaWQJbm9kZS0xLWlkE25vZGUtMS1l\
            cGhlbWVyYWwtaWQHMC4wLjAuMAcwLjAuMC4wBAAAAAAHMC4wLjAuMAAAAAIAAQZtYXN0ZXIBbQCH0bMElZnBBI61ugSS/ccECW5vZGUtMS1pZAAAAAAAAAAD\
            AAAAAAAAAAQBCW5vZGUtMi1pZAEJbm9kZS0zLWlkAQQAAAAABzAuMC4wLjAAAAAFAglub2RlLTQtaWQJbm9kZS00LWlkE25vZGUtNC1lcGhlbWVyYWwtaWQH\
            MC4wLjAuMAcwLjAuMC4wBAAAAAAHMC4wLjAuMAAAAAUAAQZtYXN0ZXIBbQCH0bMElZnBBI61ugSS/ccECW5vZGUtNC1pZAlub2RlLTUtaWQJbm9kZS01LWlk\
            E25vZGUtNS1lcGhlbWVyYWwtaWQHMC4wLjAuMAcwLjAuMC4wBAAAAAAHMC4wLjAuMAAAAAYAAQZtYXN0ZXIBbQCH0bMElZnBBI61ugSS/ccECW5vZGUtNS1p\
            ZAEJbm9kZS02LWlkCW5vZGUtNi1pZBNub2RlLTYtZXBoZW1lcmFsLWlkBzAuMC4wLjAHMC4wLjAuMAQAAAAABzAuMC4wLjAAAAAHAAEGbWFzdGVyAW0Ah9Gz\
            BJWZwQSOtboEkv3HBAlub2RlLTYtaWQAAAAAAAAABQAHSEVBTFRIWQ5oZWFsdGh5LXN0YXR1cwEJbm9kZS03LWlkCW5vZGUtNy1pZBNub2RlLTctZXBoZW1l\
            cmFsLWlkBzAuMC4wLjAHMC4wLjAuMAQAAAAABzAuMC4wLjAAAAAIAAEGbWFzdGVyAW0Ah9GzBJWZwQSOtboEkv3HBAlub2RlLTctaWQAAAAAAAAABhNqb2lu\
            LXN0YXR1cy1tZXNzYWdlAgU=""", Base64.getEncoder().encodeToString(bytes));

        assertEquals(clusterFormationState, ClusterFormationFailureHelper.ClusterFormationState.readFrom(new ByteArrayStreamInput(bytes)));
    }

    @Override
    protected ClusterFormationFailureHelper.ClusterFormationState mutateInstance(
        ClusterFormationFailureHelper.ClusterFormationState instance
    ) {
        // Since ClusterFormationState is a record, we don't need to check for equality
        return null;
    }
}
