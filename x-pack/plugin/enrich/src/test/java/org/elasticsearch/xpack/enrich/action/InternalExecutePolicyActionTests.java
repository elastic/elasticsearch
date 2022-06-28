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
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.emptyMap;
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
        DiscoveryNode node1 = newNode(randomAlphaOfLength(4));
        DiscoveryNode node2 = newNode(randomAlphaOfLength(4));
        DiscoveryNode node3 = newNode(randomAlphaOfLength(4));
        DiscoveryNodes discoNodes = DiscoveryNodes.builder()
            .add(node1)
            .add(node2)
            .add(node3)
            .masterNodeId(node1.getId())
            .localNodeId(node1.getId())
            .build();
        DiscoveryNode result = transportAction.selectNodeForPolicyExecution(discoNodes);
        assertThat(result, either(equalTo(node2)).or(equalTo(node3)));
    }

    public void testSelectNodeForPolicyExecutionSingleNode() {
        DiscoveryNode node1 = newNode(randomAlphaOfLength(4));
        DiscoveryNodes discoNodes = DiscoveryNodes.builder().add(node1).masterNodeId(node1.getId()).localNodeId(node1.getId()).build();
        DiscoveryNode result = transportAction.selectNodeForPolicyExecution(discoNodes);
        assertThat(result, equalTo(node1));
    }

    public void testSelectNodeForPolicyExecutionDedicatedMasters() {
        Set<DiscoveryNodeRole> roles = Collections.singleton(DiscoveryNodeRole.MASTER_ROLE);
        DiscoveryNode node1 = newNode(randomAlphaOfLength(4), roles);
        DiscoveryNode node2 = newNode(randomAlphaOfLength(4), roles);
        DiscoveryNode node3 = newNode(randomAlphaOfLength(4), roles);
        DiscoveryNode node4 = newNode(randomAlphaOfLength(4));
        DiscoveryNode node5 = newNode(randomAlphaOfLength(4));
        DiscoveryNode node6 = newNode(randomAlphaOfLength(4));
        DiscoveryNodes discoNodes = DiscoveryNodes.builder()
            .add(node1)
            .add(node2)
            .add(node3)
            .add(node4)
            .add(node5)
            .add(node6)
            .masterNodeId(node2.getId())
            .localNodeId(node2.getId())
            .build();
        DiscoveryNode result = transportAction.selectNodeForPolicyExecution(discoNodes);
        assertThat(result, either(equalTo(node4)).or(equalTo(node5)).or(equalTo(node6)));
    }

    public void testSelectNodeForPolicyExecutionNoNodeWithIngestRole() {
        Set<DiscoveryNodeRole> roles = new HashSet<>(Arrays.asList(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE));
        DiscoveryNode node1 = newNode(randomAlphaOfLength(4), roles);
        DiscoveryNode node2 = newNode(randomAlphaOfLength(4), roles);
        DiscoveryNode node3 = newNode(randomAlphaOfLength(4), roles);
        DiscoveryNodes discoNodes = DiscoveryNodes.builder()
            .add(node1)
            .add(node2)
            .add(node3)
            .masterNodeId(node1.getId())
            .localNodeId(node1.getId())
            .build();
        Exception e = expectThrows(IllegalStateException.class, () -> transportAction.selectNodeForPolicyExecution(discoNodes));
        assertThat(e.getMessage(), equalTo("no ingest nodes in this cluster"));
    }

    public void testSelectNodeForPolicyExecutionMixedVersions() {
        DiscoveryNode node1 = newNode(randomAlphaOfLength(4), Version.V_7_14_0);
        DiscoveryNode node2 = newNode(randomAlphaOfLength(4), Version.V_7_14_0);
        DiscoveryNode node3 = newNode(randomAlphaOfLength(4));
        DiscoveryNodes discoNodes = DiscoveryNodes.builder()
            .add(node1)
            .add(node2)
            .add(node3)
            .masterNodeId(node3.getId())
            .localNodeId(node3.getId())
            .build();
        Exception e = expectThrows(IllegalStateException.class, () -> transportAction.selectNodeForPolicyExecution(discoNodes));
        assertThat(e.getMessage(), equalTo("no suitable node was found to perform enrich policy execution"));
    }

    public void testSelectNodeForPolicyExecutionPickLocalNodeIfNotElectedMaster() {
        DiscoveryNode node1 = newNode(randomAlphaOfLength(4));
        DiscoveryNode node2 = newNode(randomAlphaOfLength(4));
        DiscoveryNode node3 = newNode(randomAlphaOfLength(4));
        DiscoveryNodes discoNodes = DiscoveryNodes.builder()
            .add(node1)
            .add(node2)
            .add(node3)
            .masterNodeId(node1.getId())
            .localNodeId(node2.getId())
            .build();
        DiscoveryNode result = transportAction.selectNodeForPolicyExecution(discoNodes);
        assertThat(result, equalTo(node2));
    }

    private static DiscoveryNode newNode(String nodeId) {
        return newNode(nodeId, Version.V_7_15_0);
    }

    private static DiscoveryNode newNode(String nodeId, Version version) {
        Set<DiscoveryNodeRole> roles = new HashSet<>(
            Arrays.asList(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.INGEST_ROLE)
        );
        return newNode(nodeId, roles, version);
    }

    private static DiscoveryNode newNode(String nodeId, Set<DiscoveryNodeRole> roles) {
        return newNode(nodeId, roles, Version.V_7_15_0);
    }

    private static DiscoveryNode newNode(String nodeId, Set<DiscoveryNodeRole> roles, Version version) {
        return new DiscoveryNode(nodeId, buildNewFakeTransportAddress(), emptyMap(), roles, version);
    }
}
