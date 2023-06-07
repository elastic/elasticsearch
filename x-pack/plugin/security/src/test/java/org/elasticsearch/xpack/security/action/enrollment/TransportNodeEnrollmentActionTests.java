/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.enrollment;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoAction;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportInfo;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.enrollment.NodeEnrollmentRequest;
import org.elasticsearch.xpack.core.security.action.enrollment.NodeEnrollmentResponse;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.ssl.SslSettingsLoader;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Key;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportNodeEnrollmentActionTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    public void testDoExecute() throws Exception {
        assumeFalse("NodeEnrollment is not supported on FIPS because it requires a KeyStore", inFipsJvm());

        final Environment env = mock(Environment.class);
        Path tempDir = createTempDir();
        Path httpCaPath = tempDir.resolve("httpCa.p12");
        Path transportPath = tempDir.resolve("transport.p12");
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/action/enrollment/httpCa.p12"), httpCaPath);
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/action/enrollment/transport.p12"), transportPath);
        when(env.configFile()).thenReturn(tempDir);
        final SSLService sslService = mock(SSLService.class);
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("keystore.secure_password", "password");
        final Settings httpSettings = Settings.builder().put("keystore.path", httpCaPath).setSecureSettings(secureSettings).build();
        final SslConfiguration httpSslConfiguration = SslSettingsLoader.load(httpSettings, null, env);
        when(sslService.getHttpTransportSSLConfiguration()).thenReturn(httpSslConfiguration);
        final Settings transportSettings = Settings.builder()
            .put("keystore.path", transportPath)
            .put("keystore.password", "password")
            .build();
        final SslConfiguration transportSslConfiguration = SslSettingsLoader.load(transportSettings, null, env);
        when(sslService.getTransportSSLConfiguration()).thenReturn(transportSslConfiguration);
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        final Client client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        final List<NodeInfo> nodeInfos = new ArrayList<>();
        final int numberOfNodes = randomIntBetween(1, 6);
        final List<NodesInfoRequest> nodesInfoRequests = new ArrayList<>();
        for (int i = 0; i < numberOfNodes; i++) {
            DiscoveryNode n = node(i);
            nodeInfos.add(
                new NodeInfo(
                    Version.CURRENT,
                    TransportVersion.current(),
                    null,
                    n,
                    null,
                    null,
                    null,
                    null,
                    null,
                    new TransportInfo(new BoundTransportAddress(new TransportAddress[] { n.getAddress() }, n.getAddress()), null, false),
                    null,
                    null,
                    null,
                    null,
                    null,
                    null
                )
            );
        }
        doAnswer(invocation -> {
            NodesInfoRequest nodesInfoRequest = (NodesInfoRequest) invocation.getArguments()[1];
            nodesInfoRequests.add(nodesInfoRequest);
            ActionListener<NodesInfoResponse> listener = (ActionListener) invocation.getArguments()[2];
            listener.onResponse(new NodesInfoResponse(new ClusterName("cluster"), nodeInfos, List.of()));
            return null;
        }).when(client).execute(same(NodesInfoAction.INSTANCE), any(), any());

        final TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );

        final TransportNodeEnrollmentAction action = new TransportNodeEnrollmentAction(
            transportService,
            sslService,
            client,
            mock(ActionFilters.class)
        );
        final NodeEnrollmentRequest request = new NodeEnrollmentRequest();
        final PlainActionFuture<NodeEnrollmentResponse> future = new PlainActionFuture<>();
        action.doExecute(mock(Task.class), request, future);
        final NodeEnrollmentResponse response = future.get();
        assertSameCertificate(response.getHttpCaCert(), httpCaPath, "password".toCharArray(), true);
        assertSameCertificate(response.getTransportCert(), transportPath, "password".toCharArray(), false);
        assertThat(response.getNodesAddresses(), hasSize(numberOfNodes));
        assertThat(nodesInfoRequests, hasSize(1));

        assertWarnings("[keystore.password] setting was deprecated in Elasticsearch and will be removed in a future release.");
    }

    private void assertSameCertificate(String cert, Path original, char[] originalPassword, boolean isCa) throws Exception {
        Map<Certificate, Key> originalKeysAndCerts = CertParsingUtils.readPkcs12KeyPairs(original, originalPassword, p -> originalPassword);
        Certificate deserializedCert = CertParsingUtils.readCertificates(
            new ByteArrayInputStream(Base64.getDecoder().decode(cert.getBytes(StandardCharsets.UTF_8)))
        ).get(0);
        assertThat(originalKeysAndCerts, hasKey(deserializedCert));
        assertThat(deserializedCert, instanceOf(X509Certificate.class));
        if (isCa) {
            assertThat(((X509Certificate) deserializedCert).getBasicConstraints(), not(-1));
        } else {
            assertThat(((X509Certificate) deserializedCert).getBasicConstraints(), is(-1));
        }
    }

    private DiscoveryNode node(final int id) {
        return DiscoveryNodeUtils.builder(Integer.toString(id)).name("node-" + id).roles(Set.of()).build();
    }
}
