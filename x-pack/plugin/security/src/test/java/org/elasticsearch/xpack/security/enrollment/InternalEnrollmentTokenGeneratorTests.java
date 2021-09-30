/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.enrollment;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoAction;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.EnrollmentToken;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.ssl.TestsSSLService;
import org.junit.Before;
import org.junit.BeforeClass;

import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InternalEnrollmentTokenGeneratorTests extends ESTestCase {

    private Environment environment;
    private Client client;

    @BeforeClass
    public static void muteInFips() {
        assumeFalse("Enrollment is not supported in FIPS 140-2 as we are using PKCS#12 keystores", inFipsJvm());
    }

    @Before
    public void setup() throws Exception {
        final Path tempDir = createTempDir();
        final Path httpCaPath = tempDir.resolve("httpCa.p12");
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/action/enrollment/httpCa.p12"), httpCaPath);
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.security.http.ssl.keystore.secure_password", "password");
        secureSettings.setString("bootstrap.password", "password");
        final Settings settings = Settings.builder()
            .put("xpack.security.http.ssl.enabled", true)
            .put("xpack.security.http.ssl.keystore.path", httpCaPath)
            .put("xpack.security.enrollment.enabled", "true")
            .setSecureSettings(secureSettings)
            .put("path.home", tempDir)
            .build();
        environment = new Environment(settings, tempDir);
        client = mock(Client.class);
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(settings));
        doAnswer(invocationOnMock -> {
            CreateApiKeyRequest request = (CreateApiKeyRequest) invocationOnMock.getArguments()[1];
            @SuppressWarnings("unchecked")
            ActionListener<CreateApiKeyResponse> responseActionListener = (ActionListener<CreateApiKeyResponse>) invocationOnMock
                .getArguments()[2];
            responseActionListener.onResponse(
                new CreateApiKeyResponse(
                    request.getName(),
                    "api-key-id",
                    new SecureString("api-key-secret".toCharArray()),
                    Instant.now().plus(Duration.ofMillis(request.getExpiration().getMillis()))
                )
            );
            return null;
        }).when(client).execute(eq(CreateApiKeyAction.INSTANCE), any(CreateApiKeyRequest.class), anyActionListener());
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<NodesInfoResponse> responseActionListener = (ActionListener<NodesInfoResponse>) invocationOnMock
                .getArguments()[2];
            responseActionListener.onResponse(
                new NodesInfoResponse(
                    new ClusterName("cluster_name"),
                    List.of(
                        new NodeInfo(
                            Version.CURRENT,
                            null,
                            new DiscoveryNode("node-name", "1", buildNewFakeTransportAddress(), Map.of(), Set.of(), Version.CURRENT),
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            new HttpInfo(
                                new BoundTransportAddress(
                                    new TransportAddress[] { new TransportAddress(InetAddress.getByName("192.168.1.2"), 9200) },
                                    new TransportAddress(InetAddress.getByName("192.168.1.2"), 9200)
                                ),
                                0L
                            ),
                            null,
                            null,
                            null,
                            null
                        )
                    ),
                    List.of()
                )
            );
            return null;
        }).when(client).execute(eq(NodesInfoAction.INSTANCE), any(), any());
    }

    public void testCreationSuccess() {
        final SSLService sslService = new TestsSSLService(environment);
        final InternalEnrollmentTokenGenerator generator = new InternalEnrollmentTokenGenerator(environment, sslService, client);
        PlainActionFuture<EnrollmentToken> future = new PlainActionFuture<>();
        generator.createKibanaEnrollmentToken(future);
        EnrollmentToken token = future.actionGet();
        assertThat(token.getApiKey(), equalTo("api-key-id:api-key-secret"));
        assertThat(token.getBoundAddress().size(), equalTo(1));
        assertThat(token.getBoundAddress().get(0), equalTo("192.168.1.2:9200"));
        assertThat(token.getVersion(), equalTo(Version.CURRENT.toString()));
        assertThat(token.getFingerprint(), equalTo("ce480d53728605674fcfd8ffb51000d8a33bf32de7c7f1e26b4d428f8a91362d"));
    }

    public void testFailureToGenerateKey() {
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<CreateApiKeyResponse> responseActionListener = (ActionListener<CreateApiKeyResponse>) invocationOnMock
                .getArguments()[2];
            responseActionListener.onFailure(new Exception("an error"));
            return null;
        }).when(client).execute(eq(CreateApiKeyAction.INSTANCE), any(CreateApiKeyRequest.class), anyActionListener());
        final SSLService sslService = new TestsSSLService(environment);
        final InternalEnrollmentTokenGenerator generator = new InternalEnrollmentTokenGenerator(environment, sslService, client);
        PlainActionFuture<EnrollmentToken> future = new PlainActionFuture<>();
        generator.createKibanaEnrollmentToken(future);
        EnrollmentToken token = future.actionGet();
        assertThat(token, nullValue());
    }

    public void testFailureToGetNodesInfo() {
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<NodesInfoResponse> responseActionListener = (ActionListener<NodesInfoResponse>) invocationOnMock
                .getArguments()[2];
            responseActionListener.onFailure(new Exception("error"));
            return null;
        }).when(client).execute(eq(NodesInfoAction.INSTANCE), any(), any());
        final SSLService sslService = new TestsSSLService(environment);
        final InternalEnrollmentTokenGenerator generator = new InternalEnrollmentTokenGenerator(environment, sslService, client);
        PlainActionFuture<EnrollmentToken> future = new PlainActionFuture<>();
        generator.createKibanaEnrollmentToken(future);
        EnrollmentToken token = future.actionGet();
        assertThat(token, nullValue());
    }
}
