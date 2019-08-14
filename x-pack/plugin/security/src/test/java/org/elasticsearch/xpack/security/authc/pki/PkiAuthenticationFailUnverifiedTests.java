/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.pki;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.test.SecuritySingleNodeTestCase;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;
import org.elasticsearch.xpack.core.ssl.PemUtils;
import org.elasticsearch.xpack.core.ssl.SSLClientAuth;
import org.elasticsearch.xpack.core.ssl.VerificationMode;

import java.net.InetSocketAddress;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

import static org.elasticsearch.test.SecuritySettingsSource.addSSLSettingsForNodePEMFiles;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class PkiAuthenticationFailUnverifiedTests extends SecuritySingleNodeTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Settings nodeSettings() {
        SSLClientAuth sslClientAuth = randomBoolean() ? SSLClientAuth.REQUIRED : SSLClientAuth.OPTIONAL;
        Settings.Builder builder = Settings.builder()
            .put(super.nodeSettings());
        addSSLSettingsForNodePEMFiles(builder, "xpack.security.http.", true);
        builder.put("xpack.security.http.ssl.enabled", true)
            .put("xpack.security.http.ssl.client_authentication", sslClientAuth)
            // verification mode does not validate certificates, hence those should not be used by the PKI realm
            .put("xpack.security.http.ssl.verification_mode", VerificationMode.NONE)
            .put("xpack.security.authc.realms.file.file.order", "0")
            .put("xpack.security.authc.realms.pki.pki1.order", "1")
            .putList("xpack.security.authc.realms.pki.pki1.certificate_authorities",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt").toString())
            .put("xpack.security.authc.realms.pki.pki1.files.role_mapping", getDataPath("role_mapping.yml"));
        return builder.build();
    }

    public void testAuthenticationFailsWithMissingCredentials() throws Exception {
        SSLContext context = getRestSSLContext("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem",
            "testnode",
            "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt",
            Arrays.asList("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt",
                "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt",
                "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode_ec.crt"));
        try (CloseableHttpClient client = HttpClients.custom().setSSLContext(context).build()) {
            HttpPut put = new HttpPut(getNodeUrl() + "foo");
            try (CloseableHttpResponse response = SocketAccess.doPrivileged(() -> client.execute(put))) {
                assertThat(response.getStatusLine().getStatusCode(), is(401));
                String body = EntityUtils.toString(response.getEntity());
                assertThat(body, containsString("missing authentication credentials for REST request"));
            }
        }
    }

    private SSLContext getRestSSLContext(String keyPath, String password, String certPath, List<String> trustedCertPaths) throws Exception {
        SSLContext context = SSLContext.getInstance("TLS");
        TrustManager tm = CertParsingUtils.trustManager(CertParsingUtils.readCertificates(trustedCertPaths.stream().map(p -> getDataPath
            (p)).collect(Collectors.toList())));
        KeyManager km = CertParsingUtils.keyManager(CertParsingUtils.readCertificates(Collections.singletonList(getDataPath
            (certPath))), PemUtils.readPrivateKey(getDataPath(keyPath), password::toCharArray), password.toCharArray());
        context.init(new KeyManager[]{km}, new TrustManager[]{tm}, new SecureRandom());
        return context;
    }

    private String getNodeUrl() {
        TransportAddress transportAddress = randomFrom(node().injector().getInstance(HttpServerTransport.class)
                .boundAddress().boundAddresses());
        final InetSocketAddress inetSocketAddress = transportAddress.address();
        return String.format(Locale.ROOT, "https://%s/", NetworkAddress.format(inetSocketAddress));
    }
}
