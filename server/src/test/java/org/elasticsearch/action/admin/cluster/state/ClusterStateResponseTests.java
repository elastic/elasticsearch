/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.state;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashSet;

public class ClusterStateResponseTests extends AbstractWireSerializingTestCase<ClusterStateResponse> {

    @Override
    protected ClusterStateResponse createTestInstance() {
        ClusterName clusterName = new ClusterName(randomAlphaOfLength(4));
        ClusterState clusterState = null;
        if (randomBoolean()) {
            ClusterState.Builder clusterStateBuilder = ClusterState.builder(clusterName).version(randomNonNegativeLong());
            if (randomBoolean()) {
                final DiscoveryNode masterNode = new DiscoveryNode(
                    randomAlphaOfLength(4),
                    ESTestCase.buildNewFakeTransportAddress(),
                    Collections.emptyMap(),
                    new HashSet<>(DiscoveryNodeRole.roles()),
                    Version.CURRENT
                );
                clusterStateBuilder.nodes(DiscoveryNodes.builder().add(masterNode).masterNodeId(masterNode.getId()).build());
            }
            clusterState = clusterStateBuilder.build();
        }
        return new ClusterStateResponse(clusterName, clusterState, randomBoolean());
    }

    @Override
    protected Writeable.Reader<ClusterStateResponse> instanceReader() {
        return ClusterStateResponse::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
    }
}
