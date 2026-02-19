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
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.elasticsearch.cluster.coordination.CoordinationDiagnosticsService.CoordinationDiagnosticsDetails;

/**
 * Wire serialization tests for {@link CoordinationDiagnosticsDetails}.
 */
public class CoordinationDiagnosticsDetailsWireSerializingTests
    extends AbstractWireSerializingTestCase<CoordinationDiagnosticsDetails> {

    @Override
    protected Writeable.Reader<CoordinationDiagnosticsDetails> instanceReader() {
        return CoordinationDiagnosticsDetails::new;
    }

    @Override
    protected CoordinationDiagnosticsDetails createTestInstance() {
        return randomCoordinationDiagnosticsDetails();
    }

    @Override
    protected CoordinationDiagnosticsDetails mutateInstance(CoordinationDiagnosticsDetails instance) throws IOException {
        DiscoveryNode currentMaster = instance.currentMaster();
        List<DiscoveryNode> recentMasters = instance.recentMasters();
        String remoteExceptionMessage = instance.remoteExceptionMessage();
        String remoteExceptionStackTrace = instance.remoteExceptionStackTrace();
        Map<String, String> nodeToClusterFormationDescriptionMap = instance.nodeToClusterFormationDescriptionMap();

        int field = between(0, 4);
        switch (field) {
            case 0 -> currentMaster = randomValueOtherThan(
                currentMaster,
                () -> randomBoolean() ? randomDiscoveryNode() : null
            );
            case 1 -> {
                List<DiscoveryNode> mutableRecentMasters = recentMasters == null
                    ? new ArrayList<>()
                    : new ArrayList<>(recentMasters);
                mutableRecentMasters.add(randomDiscoveryNode());
                recentMasters = mutableRecentMasters;
            }
            case 2 -> remoteExceptionMessage = randomValueOtherThan(
                remoteExceptionMessage,
                () -> randomBoolean() ? randomAlphaOfLengthBetween(0, 100) : null
            );
            case 3 -> remoteExceptionStackTrace = randomValueOtherThan(
                remoteExceptionStackTrace,
                () -> randomBoolean() ? randomAlphaOfLengthBetween(0, 200) : null
            );
            default -> nodeToClusterFormationDescriptionMap = randomValueOtherThan(
                nodeToClusterFormationDescriptionMap,
                this::randomNodeToClusterFormationDescriptionMap
            );
        }

        Map<String, String> mapForConstructor = nodeToClusterFormationDescriptionMap != null
            ? nodeToClusterFormationDescriptionMap
            : Map.of();
        return new CoordinationDiagnosticsDetails(
            currentMaster,
            recentMasters,
            remoteExceptionMessage,
            remoteExceptionStackTrace,
            mapForConstructor
        );
    }

    private DiscoveryNode randomDiscoveryNode() {
        return DiscoveryNodeUtils.builder(UUID.randomUUID().toString())
            .address(new TransportAddress(TransportAddress.META_ADDRESS, randomIntBetween(1, 65535)))
            .build();
    }

    private CoordinationDiagnosticsDetails randomCoordinationDiagnosticsDetails() {
        DiscoveryNode currentMaster = randomBoolean() ? randomDiscoveryNode() : null;
        List<DiscoveryNode> recentMasters = randomBoolean() ? randomList(0, 5, this::randomDiscoveryNode) : null;
        String remoteExceptionMessage = randomBoolean() ? randomAlphaOfLengthBetween(0, 100) : null;
        String remoteExceptionStackTrace = randomBoolean() ? randomAlphaOfLengthBetween(0, 200) : null;
        Map<String, String> nodeToClusterFormationDescriptionMap = randomNodeToClusterFormationDescriptionMap();
        return new CoordinationDiagnosticsDetails(
            currentMaster,
            recentMasters,
            remoteExceptionMessage,
            remoteExceptionStackTrace,
            nodeToClusterFormationDescriptionMap
        );
    }

    private Map<String, String> randomNodeToClusterFormationDescriptionMap() {
        Map<String, String> nodeToClusterFormationDescriptionMap = new HashMap<>();
        int size = between(0, 5);
        for (int i = 0; i < size; i++) {
            nodeToClusterFormationDescriptionMap.put(
                randomAlphaOfLengthBetween(1, 20),
                randomAlphaOfLengthBetween(0, 50)
            );
        }
        return nodeToClusterFormationDescriptionMap.isEmpty() ? Map.of() : nodeToClusterFormationDescriptionMap;
    }
}
