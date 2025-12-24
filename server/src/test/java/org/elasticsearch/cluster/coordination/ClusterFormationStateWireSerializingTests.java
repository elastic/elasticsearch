/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.monitor.StatusInfo;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.elasticsearch.monitor.StatusInfo.Status.HEALTHY;
import static org.elasticsearch.monitor.StatusInfo.Status.UNHEALTHY;

public class ClusterFormationStateWireSerializingTests extends AbstractWireSerializingTestCase<
    ClusterFormationFailureHelper.ClusterFormationState> {
    @Override
    protected Writeable.Reader<ClusterFormationFailureHelper.ClusterFormationState> instanceReader() {
        return ClusterFormationFailureHelper.ClusterFormationState::new;
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
            DiscoveryNodeUtils.create(UUID.randomUUID().toString()),
            masterEligibleNodes,
            randomLong(),
            randomLong(),
            new CoordinationMetadata.VotingConfiguration(randomSet(0, 5, () -> UUID.randomUUID().toString())),
            new CoordinationMetadata.VotingConfiguration(randomSet(0, 5, () -> UUID.randomUUID().toString())),
            randomList(0, 5, ESTestCase::buildNewFakeTransportAddress),
            randomList(1, 5, () -> DiscoveryNodeUtils.create(UUID.randomUUID().toString())),
            randomSet(1, 5, () -> DiscoveryNodeUtils.create(UUID.randomUUID().toString())),
            randomLong(),
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

    @Override
    protected ClusterFormationFailureHelper.ClusterFormationState mutateInstance(
        ClusterFormationFailureHelper.ClusterFormationState instance
    ) throws IOException {
        // Since ClusterFormationState is a record, we don't need to check for equality
        return null;
    }
}
