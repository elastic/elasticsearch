/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;

import java.util.Set;

import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class InternalExecutePolicyActionTests extends ESTestCase {

    private InternalExecutePolicyAction.Transport transportAction;

    @Before
    public void instantiateTransportAction() {
        transportAction = new InternalExecutePolicyAction.Transport(mock(TransportService.class), mock(ActionFilters.class), null, null);
    }

    public void testSelectNodeForPolicyExecution() {
        var node1 = newNode(randomAlphaOfLength(4));
        var node2 = newNode(randomAlphaOfLength(4));
        var node3 = newNode(randomAlphaOfLength(4));
        var discoNodes = DiscoveryNodes.builder()
            .add(node1)
            .add(node2)
            .add(node3)
            .masterNodeId(node1.getId())
            .localNodeId(node1.getId())
            .build();
        var result = transportAction.selectNodeForPolicyExecution(discoNodes);
        assertThat(result, either(equalTo(node2)).or(equalTo(node3)));
    }

    public void testSelectNodeForPolicyExecutionSingleNode() {
        var node1 = newNode(randomAlphaOfLength(4));
        var discoNodes = DiscoveryNodes.builder().add(node1).masterNodeId(node1.getId()).localNodeId(node1.getId()).build();
        var result = transportAction.selectNodeForPolicyExecution(discoNodes);
        assertThat(result, equalTo(node1));
    }

    public void testSelectNodeForPolicyExecutionDedicatedMasters() {
        var roles = Set.of(DiscoveryNodeRole.MASTER_ROLE);
        var node1 = newNode(randomAlphaOfLength(4), roles);
        var node2 = newNode(randomAlphaOfLength(4), roles);
        var node3 = newNode(randomAlphaOfLength(4), roles);
        var node4 = newNode(randomAlphaOfLength(4));
        var node5 = newNode(randomAlphaOfLength(4));
        var node6 = newNode(randomAlphaOfLength(4));
        var discoNodes = DiscoveryNodes.builder()
            .add(node1)
            .add(node2)
            .add(node3)
            .add(node4)
            .add(node5)
            .add(node6)
            .masterNodeId(node2.getId())
            .localNodeId(node2.getId())
            .build();
        var result = transportAction.selectNodeForPolicyExecution(discoNodes);
        assertThat(result, either(equalTo(node4)).or(equalTo(node5)).or(equalTo(node6)));
    }

    public void testSelectNodeForPolicyExecutionNoNodeWithIngestRole() {
        var roles = Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE);
        var node1 = newNode(randomAlphaOfLength(4), roles);
        var node2 = newNode(randomAlphaOfLength(4), roles);
        var node3 = newNode(randomAlphaOfLength(4), roles);
        var discoNodes = DiscoveryNodes.builder()
            .add(node1)
            .add(node2)
            .add(node3)
            .masterNodeId(node1.getId())
            .localNodeId(node1.getId())
            .build();
        var e = expectThrows(IllegalStateException.class, () -> transportAction.selectNodeForPolicyExecution(discoNodes));
        assertThat(e.getMessage(), equalTo("no ingest nodes in this cluster"));
    }

    public void testSelectNodeForPolicyExecutionPickLocalNodeIfNotElectedMaster() {
        var node1 = newNode(randomAlphaOfLength(4));
        var node2 = newNode(randomAlphaOfLength(4));
        var node3 = newNode(randomAlphaOfLength(4));
        var discoNodes = DiscoveryNodes.builder()
            .add(node1)
            .add(node2)
            .add(node3)
            .masterNodeId(node1.getId())
            .localNodeId(node2.getId())
            .build();
        var result = transportAction.selectNodeForPolicyExecution(discoNodes);
        assertThat(result, equalTo(node2));
    }

    private static DiscoveryNode newNode(String nodeId) {
        return newNode(nodeId, Version.V_7_15_0);
    }

    private static DiscoveryNode newNode(String nodeId, Version version) {
        var roles = Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.INGEST_ROLE);
        return newNode(nodeId, roles, version);
    }

    private static DiscoveryNode newNode(String nodeId, Set<DiscoveryNodeRole> roles) {
        return newNode(nodeId, roles, Version.V_7_15_0);
    }

    private static DiscoveryNode newNode(String nodeId, Set<DiscoveryNodeRole> roles, Version version) {
        return DiscoveryNodeUtils.builder(nodeId).roles(roles).version(version).build();
    }
}
