/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoAction;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordAction;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordRequest;
import org.junit.Before;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportClientEnrollmentActionTests extends ESTestCase {
    private List<ChangePasswordRequest> changePasswordRequests;
    private List<NodesInfoRequest> nodesInfoRequests;
    private TransportClientEnrollmentAction action;
    private int numberOfNodes;
    private Path httpCaPath;

    @Before @SuppressWarnings("unchecked") public void setup() throws Exception {
        changePasswordRequests = new ArrayList<>();
        nodesInfoRequests = new ArrayList<>();
        final Environment env = mock(Environment.class);
        final Path tempDir = createTempDir();
        httpCaPath = tempDir.resolve("httpCa.crt");
        Files.copy(getDataPath("/org/elasticsearch/xpack/core/action/httpCa.crt"), httpCaPath);
        when(env.configFile()).thenReturn(tempDir);
        final Settings settings = Settings.EMPTY;
        final ThreadContext threadContext = new ThreadContext(settings);
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        final Client client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        final List<NodeInfo> nodeInfos = new ArrayList<>();
        numberOfNodes = randomIntBetween(1, 6);
        for (int i = 0; i < numberOfNodes; i++) {
            DiscoveryNode n = node(i);
            nodeInfos.add(new NodeInfo(Version.CURRENT,
                null,
                n,
                null,
                null,
                null,
                null,
                null,
                null,
                new HttpInfo(new BoundTransportAddress(new TransportAddress[] { n.getAddress() }, n.getAddress()), randomLong()),
                null,
                null,
                null,
                null));
        }
        doAnswer(invocation -> {
            NodesInfoRequest nodesInfoRequest = (NodesInfoRequest) invocation.getArguments()[1];
            nodesInfoRequests.add(nodesInfoRequest);
            ActionListener<NodesInfoResponse> listener = (ActionListener) invocation.getArguments()[2];
            listener.onResponse(new NodesInfoResponse(new ClusterName("cluster"), nodeInfos, List.of()));
            return null;
        }).when(client).execute(eq(NodesInfoAction.INSTANCE), any(), any());

        doAnswer(invocation -> {
            ChangePasswordRequest changePasswordRequest = (ChangePasswordRequest) invocation.getArguments()[1];
            changePasswordRequests.add(changePasswordRequest);
            ActionListener<ActionResponse.Empty> listener = (ActionListener) invocation.getArguments()[2];
            listener.onResponse(ActionResponse.Empty.INSTANCE);
            return null;
        }).when(client).execute(eq(ChangePasswordAction.INSTANCE), any(), any());

        final TransportService transportService = new TransportService(Settings.EMPTY,
            mock(Transport.class),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet());
        action = new TransportClientEnrollmentAction(transportService, client, settings, env, mock(ActionFilters.class));
    }

    public void testKibanaEnrollment() throws Exception {
        final boolean shouldChangePassword = randomBoolean();
        final SecureString password = new SecureString(randomAlphaOfLengthBetween(8, 16).toCharArray());
        final ClientEnrollmentRequest request = new ClientEnrollmentRequest("kibana", shouldChangePassword ? password : null);
        final PlainActionFuture<ClientEnrollmentResponse> future = new PlainActionFuture<>();
        action.doExecute(mock(Task.class), request, future);
        final ClientEnrollmentResponse response = future.actionGet();
        assertThat(response.getHttpCa(), equalTo(Base64.getUrlEncoder().encodeToString(Files.readAllBytes(httpCaPath))));
        assertThat(response.getNodesAddresses().size(), equalTo(numberOfNodes));
        if (shouldChangePassword) {
            assertThat(changePasswordRequests.size(), equalTo(1));
        } else {
            assertThat(changePasswordRequests.size(), equalTo(0));
        }
        assertThat(nodesInfoRequests.size(), equalTo(1));
    }

    public void testGenericClientEnrollment() throws Exception {
        final ClientEnrollmentRequest request = new ClientEnrollmentRequest(randomAlphaOfLengthBetween(5,8), null);
        final PlainActionFuture<ClientEnrollmentResponse> future = new PlainActionFuture<>();
        action.doExecute(mock(Task.class), request, future);
        final ClientEnrollmentResponse response = future.actionGet();
        assertThat(response.getHttpCa(), equalTo(Base64.getUrlEncoder().encodeToString(Files.readAllBytes(httpCaPath))));
        assertThat(response.getNodesAddresses().size(), equalTo(numberOfNodes));
        assertThat(changePasswordRequests.size(), equalTo(0));
        assertThat(nodesInfoRequests.size(), equalTo(1));
    }

    private DiscoveryNode node(final int id) {
        return new DiscoveryNode("node-" + id, Integer.toString(id), buildNewFakeTransportAddress(), Map.of(), Set.of(), Version.CURRENT);
    }
}
