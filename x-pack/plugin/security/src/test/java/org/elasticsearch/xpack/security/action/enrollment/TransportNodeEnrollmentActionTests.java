/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.enrollment;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.enrollment.NodeEnrollmentRequest;
import org.elasticsearch.xpack.core.security.action.enrollment.NodeEnrollmentResponse;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;
import org.elasticsearch.xpack.core.ssl.SSLConfiguration;
import org.elasticsearch.xpack.core.ssl.SSLService;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Key;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportNodeEnrollmentActionTests extends ESTestCase {

    public void testDoExecute() throws Exception {
        final Environment env = mock(Environment.class);
        Path tempDir = createTempDir();
        Path httpCaPath = tempDir.resolve("httpCa.p12");
        Path transportPath = tempDir.resolve("transport.p12");
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/action/enrollment/httpCa.p12"), httpCaPath);
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/action/enrollment/transport.p12"), transportPath);
        when(env.configFile()).thenReturn(tempDir);
        final SSLService sslService = mock(SSLService.class);
        final Settings httpSettings = Settings.builder()
            .put("keystore.path", "httpCa.p12")
            .put("keystore.password", "password")
            .build();
        final SSLConfiguration httpSslConfiguration = new SSLConfiguration(httpSettings);
        when(sslService.getHttpTransportSSLConfiguration()).thenReturn(httpSslConfiguration);
        final Settings transportSettings = Settings.builder()
            .put("keystore.path", "transport.p12")
            .put("keystore.password", "password")
            .build();
        final SSLConfiguration transportSslConfiguration = new SSLConfiguration(transportSettings);
        when(sslService.getTransportSSLConfiguration()).thenReturn(transportSslConfiguration);
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

        final TransportNodeEnrollmentAction action =
            new TransportNodeEnrollmentAction(mock(TransportService.class), clusterService, sslService, mock(ActionFilters.class), env);
        final NodeEnrollmentRequest request = new NodeEnrollmentRequest();
        final PlainActionFuture<NodeEnrollmentResponse> future = new PlainActionFuture<>();
        action.doExecute(mock(Task.class), request, future);
        final NodeEnrollmentResponse response = future.get();
        assertThat(response.getClusterName(), equalTo(clusterName));
        assertSameCertificate(response.getHttpCaCert(), httpCaPath, "password".toCharArray(), true);
        assertSameCertificate(response.getTransportCert(), transportPath, "password".toCharArray(), false);
        assertThat(response.getNodesAddresses().size(), equalTo(nodeSize));
    }

    private void assertSameCertificate(String cert, Path original, char[] originalPassword, boolean isCa) throws Exception{
        Map<Certificate, Key> originalKeysAndCerts = CertParsingUtils.readPkcs12KeyPairs(original, originalPassword, p -> originalPassword);
        Certificate deserializedCert = CertParsingUtils.readCertificates(
            new ByteArrayInputStream(Base64.getUrlDecoder().decode(cert.getBytes(StandardCharsets.UTF_8)))).get(0);
        assertThat(originalKeysAndCerts, hasKey(deserializedCert));
        assertThat(deserializedCert, instanceOf(X509Certificate.class));
        if (isCa) {
            assertThat(((X509Certificate) deserializedCert).getBasicConstraints(), not(-1));
        } else {
            assertThat(((X509Certificate) deserializedCert).getBasicConstraints(), is(-1));
        }
    }

    private DiscoveryNode newNode(String nodeId) {
        return new DiscoveryNode(nodeId,
            buildNewFakeTransportAddress(),
            emptyMap(),
            Set.copyOf(randomSubsetOf(DiscoveryNodeRole.roles())),
            Version.CURRENT);
    }
}
