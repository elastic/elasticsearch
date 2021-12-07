/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.pki;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslClientAuthenticationMode;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.test.SecuritySingleNodeTestCase;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

import static org.elasticsearch.test.SecuritySettingsSource.addSSLSettingsForNodePEMFiles;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

/**
 * Test authentication via PKI on both REST and Transport layers
 */
public class PkiAuthenticationTests extends SecuritySingleNodeTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Settings nodeSettings() {
        SslClientAuthenticationMode clientAuth = randomBoolean()
            ? SslClientAuthenticationMode.REQUIRED
            : SslClientAuthenticationMode.OPTIONAL;

        Settings.Builder builder = Settings.builder().put(super.nodeSettings());
        addSSLSettingsForNodePEMFiles(builder, "xpack.security.http.", true);
        builder.put("xpack.security.http.ssl.enabled", true)
            .put("xpack.security.http.ssl.client_authentication", clientAuth)
            .put("xpack.security.authc.realms.file.file.order", "0")
            .put("xpack.security.authc.realms.pki.pki1.order", "2")
            .putList(
                "xpack.security.authc.realms.pki.pki1.certificate_authorities",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt").toString()
            )
            .put("xpack.security.authc.realms.pki.pki1.files.role_mapping", getDataPath("role_mapping.yml"))
            .put("xpack.security.authc.realms.pki.pki1.files.role_mapping", getDataPath("role_mapping.yml"))
            // pki1 never authenticates because of the principal pattern
            .put("xpack.security.authc.realms.pki.pki1.username_pattern", "CN=(MISMATCH.*?)(?:,|$)")
            .put("xpack.security.authc.realms.pki.pki2.order", "3")
            .putList(
                "xpack.security.authc.realms.pki.pki2.certificate_authorities",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt").toString(),
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode_ec.crt").toString()
            )
            .put("xpack.security.authc.realms.pki.pki2.files.role_mapping", getDataPath("role_mapping.yml"));
        return builder.build();
    }

    @Override
    protected boolean transportSSLEnabled() {
        return true;
    }

    @Override
    protected boolean enableWarningsCheck() {
        // TODO: consider setting this back to true now that the transport client is gone
        return false;
    }

    public void testRestAuthenticationViaPki() throws Exception {
        SSLContext context = getRestSSLContext(
            "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem",
            "testnode",
            "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt",
            Arrays.asList(
                "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt",
                "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt",
                "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode_ec.crt"
            )
        );
        try (CloseableHttpClient client = HttpClients.custom().setSSLContext(context).build()) {
            HttpPut put = new HttpPut(getNodeUrl() + "foo");
            try (CloseableHttpResponse response = SocketAccess.doPrivileged(() -> client.execute(put))) {
                String body = EntityUtils.toString(response.getEntity());
                assertThat(body, containsString("\"acknowledged\":true"));
            }
        }
    }

    public void testRestAuthenticationFailure() throws Exception {
        SSLContext context = getRestSSLContext(
            "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.pem",
            "testclient",
            "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt",
            Arrays.asList(
                "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt",
                "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt",
                "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode_ec.crt"
            )
        );
        try (CloseableHttpClient client = HttpClients.custom().setSSLContext(context).build()) {
            HttpPut put = new HttpPut(getNodeUrl() + "foo");
            try (CloseableHttpResponse response = SocketAccess.doPrivileged(() -> client.execute(put))) {
                assertThat(response.getStatusLine().getStatusCode(), is(401));
                String body = EntityUtils.toString(response.getEntity());
                assertThat(body, containsString("unable to authenticate user [Elasticsearch Test Client]"));
            }
        }
    }

    private SSLContext getRestSSLContext(String keyPath, String password, String certPath, List<String> trustedCertPaths) throws Exception {
        SSLContext context = SSLContext.getInstance("TLS");
        final List<Path> resolvedPaths = trustedCertPaths.stream().map(p -> getDataPath(p)).collect(Collectors.toList());
        TrustManager tm = CertParsingUtils.getTrustManagerFromPEM(resolvedPaths);
        KeyManager km = CertParsingUtils.getKeyManagerFromPEM(getDataPath(certPath), getDataPath(keyPath), password.toCharArray());
        context.init(new KeyManager[] { km }, new TrustManager[] { tm }, new SecureRandom());
        return context;
    }

    private String getNodeUrl() {
        TransportAddress transportAddress = randomFrom(
            node().injector().getInstance(HttpServerTransport.class).boundAddress().boundAddresses()
        );
        final InetSocketAddress inetSocketAddress = transportAddress.address();
        return String.format(Locale.ROOT, "https://%s/", NetworkAddress.format(inetSocketAddress));
    }
}
