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
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.cluster.coordination.CoordinationDiagnosticsService.CoordinationDiagnosticsDetails;

/**
 * Wire serialization tests for {@link CoordinationDiagnosticsDetails}.
 */
public class CoordinationDiagnosticsDetailsWireSerializingTests extends AbstractWireSerializingTestCase<CoordinationDiagnosticsDetails> {

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
        // Since CoordinationDiagnosticsDetails is a record, we don't need to check for equality
        return null;
    }

    private DiscoveryNode randomDiscoveryNode() {
        return DiscoveryNodeUtils.builder(UUIDs.randomBase64UUID(random()))
            .name(randomAlphaOfLength(10))
            .ephemeralId(UUIDs.randomBase64UUID(random()))
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
            nodeToClusterFormationDescriptionMap.put(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(0, 50));
        }
        return nodeToClusterFormationDescriptionMap;
    }
}
