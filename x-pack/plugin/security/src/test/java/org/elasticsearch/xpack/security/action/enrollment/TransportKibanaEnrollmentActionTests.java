/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.enrollment;

import org.elasticsearch.ElasticsearchException;
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
import org.elasticsearch.client.ValidationException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.MockSecureSettings;
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
import org.elasticsearch.xpack.core.security.action.enrollment.KibanaEnrollmentRequest;
import org.elasticsearch.xpack.core.security.action.enrollment.KibanaEnrollmentResponse;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordAction;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordRequest;
import org.elasticsearch.xpack.core.ssl.SSLConfiguration;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.junit.Before;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportKibanaEnrollmentActionTests extends ESTestCase {
    private List<ChangePasswordRequest> changePasswordRequests;
    private List<NodesInfoRequest> nodesInfoRequests;
    private TransportKibanaEnrollmentAction action;
    private Client client;
    private int numberOfNodes;
    private Path httpCaPath;

    @Before @SuppressWarnings("unchecked") public void setup() throws Exception {
        changePasswordRequests = new ArrayList<>();
        nodesInfoRequests = new ArrayList<>();
        final Environment env = mock(Environment.class);
        final Path tempDir = createTempDir();
        httpCaPath = tempDir.resolve("httpCa.p12");
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/action/enrollment/httpCa.p12"), httpCaPath);
        when(env.configFile()).thenReturn(tempDir);
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("keystore.secure_password", "password");
        final Settings settings = Settings.builder()
            .put("keystore.path", "httpCa.p12")
            .setSecureSettings(secureSettings)
            .build();
        when(env.settings()).thenReturn(settings);
        final SSLService sslService = mock(SSLService.class);
        final SSLConfiguration sslConfiguration = new SSLConfiguration(settings);
        when(sslService.getHttpTransportSSLConfiguration()).thenReturn(sslConfiguration);
        final ThreadContext threadContext = new ThreadContext(settings);
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        client = mock(Client.class);
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
        action = new TransportKibanaEnrollmentAction(transportService, client, sslService, env, mock(ActionFilters.class));
    }

    public void testKibanaEnrollment() {
        final KibanaEnrollmentRequest request = new KibanaEnrollmentRequest();
        final PlainActionFuture<KibanaEnrollmentResponse> future = new PlainActionFuture<>();
        action.doExecute(mock(Task.class), request, future);
        final KibanaEnrollmentResponse response = future.actionGet();
        assertThat(response.getHttpCa(), startsWith("MIIDSjCCAjKgAwIBAgIVALCgZXvbceUrjJaQMheDCX0kXnRJMA0GCSqGSIb3DQEBCwUAMDQxMjAw" +
            "BgNVBAMTKUVsYXN0aWMgQ2VydGlmaWNhdGUgVG9vbCBBdXRvZ2VuZXJhdGVkIENBMB4XDTIxMDQyODEyNTY0MVoXDTI0MDQyNzEyNTY0MVowNDEyMDAGA1UEA" +
            "xMpRWxhc3RpYyBDZXJ0aWZpY2F0ZSBUb29sIEF1dG9nZW5lcmF0ZWQgQ0EwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCCJbOU4JvxDD_F"));
        assertThat(response.getNodesAddresses().size(), equalTo(numberOfNodes));
        assertThat(changePasswordRequests.size(), equalTo(1));
        assertThat(nodesInfoRequests.size(), equalTo(1));
    }

    public void testKibanaEnrollmentFailedPasswordChange() {
        // Override change password mock
        doAnswer(invocation -> {
            ActionListener<ActionResponse.Empty> listener = (ActionListener) invocation.getArguments()[2];
            listener.onFailure(new ValidationException());
            return null;
        }).when(client).execute(eq(ChangePasswordAction.INSTANCE), any(), any());
        final KibanaEnrollmentRequest request = new KibanaEnrollmentRequest();
        final PlainActionFuture<KibanaEnrollmentResponse> future = new PlainActionFuture<>();
        action.doExecute(mock(Task.class), request, future);
        ElasticsearchException e = expectThrows(ElasticsearchException.class, future::actionGet);
        assertThat(e.getDetailedMessage(), containsString("Failed to set the password for user [kibana_system]"));
    }

    private DiscoveryNode node(final int id) {
        return new DiscoveryNode("node-" + id, Integer.toString(id), buildNewFakeTransportAddress(), Map.of(), Set.of(), Version.CURRENT);
    }
}
