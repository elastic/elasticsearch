/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.env.Environment;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportNodeEnrollmentActionTests extends ESTestCase {

    public void testDoExecute() throws Exception {
        final Environment env = mock(Environment.class);
        Path tempDir = createTempDir();
        Path httpCaPath = tempDir.resolve("httpCa.p12");
        Path transportPath = tempDir.resolve("transport.p12");
        Files.copy(getDataPath("/org/elasticsearch/xpack/core/action/httpCa.p12"), httpCaPath);
        Files.copy(getDataPath("/org/elasticsearch/xpack/core/action/transport.p12"), transportPath);
        when(env.configFile()).thenReturn(tempDir);
        final ClusterService clusterService = mock(ClusterService.class);
        final String clusterName = randomAlphaOfLengthBetween(6, 10);
        when(clusterService.getClusterName()).thenReturn(new ClusterName(clusterName));
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        final int nodeSize = randomIntBetween(1, 10);
        for (int i = 0; i < nodeSize; i++) {
            builder.add(newNode("node" + i));
        }
        ClusterState clusterState = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.getNodes()).thenReturn(builder.build());

        final TransportNodeEnrollmentAction
            action =
            new TransportNodeEnrollmentAction(mock(TransportService.class), clusterService, mock(ActionFilters.class), env);
        final NodeEnrollmentRequest request = new NodeEnrollmentRequest();
        final PlainActionFuture<NodeEnrollmentResponse> future = new PlainActionFuture<>();
        action.doExecute(mock(Task.class), request, future);
        final NodeEnrollmentResponse response = future.get();
        assertThat(response.getClusterName(), equalTo(clusterName));
        assertThat(response.getHttpCaKeystore(),
            equalTo(Base64.getUrlEncoder().encodeToString(Files.readAllBytes(getDataPath("httpCa.p12")))));
        assertThat(response.getTransportKeystore(),
            equalTo(Base64.getUrlEncoder().encodeToString(Files.readAllBytes(getDataPath("transport.p12")))));
        assertThat(response.getNodesAddresses().size(), equalTo(nodeSize));
    }

    private DiscoveryNode newNode(String nodeId) {
        return new DiscoveryNode(nodeId,
            buildNewFakeTransportAddress(),
            emptyMap(),
            Set.copyOf(randomSubsetOf(DiscoveryNodeRole.roles())),
            Version.CURRENT);
    }
}
