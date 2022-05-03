/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.pki;

import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslClientAuthenticationMode;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.SecuritySingleNodeTestCase;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.junit.BeforeClass;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.SecureRandom;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import static org.hamcrest.Matchers.is;

public class PkiOptionalClientAuthTests extends SecuritySingleNodeTestCase {

    private static int randomClientPort;

    @BeforeClass
    public static void initPort() {
        randomClientPort = randomIntBetween(49000, 65500);
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    protected Settings nodeSettings() {
        String randomClientPortRange = randomClientPort + "-" + (randomClientPort + 100);

        Settings.Builder builder = Settings.builder()
            .put(super.nodeSettings())
            .put("xpack.security.http.ssl.enabled", true)
            .put("xpack.security.http.ssl.client_authentication", SslClientAuthenticationMode.OPTIONAL)
            .put("xpack.security.http.ssl.key", getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem"))
            .put(
                "xpack.security.http.ssl.certificate",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt")
            )
            .put("xpack.security.authc.realms.file.file.order", "0")
            .put("xpack.security.authc.realms.pki.pki1.order", "2")
            .put(
                "xpack.security.authc.realms.pki.pki1.truststore.path",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/truststore-testnode-only.jks")
            )
            .put("xpack.security.authc.realms.pki.pki1.files.role_mapping", getDataPath("role_mapping.yml"))
            .put("transport.profiles.want_client_auth.port", randomClientPortRange)
            .put("transport.profiles.want_client_auth.bind_host", "localhost")
            .put(
                "transport.profiles.want_client_auth.xpack.security.ssl.key",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem")
            )
            .put(
                "transport.profiles.want_client_auth.xpack.security.ssl.certificate",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt")
            )
            .put("transport.profiles.want_client_auth.xpack.security.ssl.client_authentication", SslClientAuthenticationMode.OPTIONAL);

        SecuritySettingsSource.addSecureSettings(builder, secureSettings -> {
            secureSettings.setString("xpack.security.authc.realms.pki.pki1.truststore.secure_password", "truststore-testnode-only");
            secureSettings.setString("xpack.security.http.ssl.secure_key_passphrase", "testnode");
            secureSettings.setString("transport.profiles.want_client_auth.xpack.security.ssl.secure_key_passphrase", "testnode");
        });
        return builder.build();

    }

    @Override
    protected boolean transportSSLEnabled() {
        return true;
    }

    public void testRestClientWithoutClientCertificate() throws Exception {
        SSLIOSessionStrategy sessionStrategy = new SSLIOSessionStrategy(getSSLContext());
        try (RestClient restClient = createRestClient(httpClientBuilder -> httpClientBuilder.setSSLStrategy(sessionStrategy), "https")) {
            ResponseException e = expectThrows(ResponseException.class, () -> restClient.performRequest(new Request("GET", "_nodes")));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(401));

            Request request = new Request("GET", "_nodes");
            RequestOptions.Builder options = request.getOptions().toBuilder();
            options.addHeader(
                "Authorization",
                UsernamePasswordToken.basicAuthHeaderValue(
                    SecuritySettingsSource.TEST_USER_NAME,
                    new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray())
                )
            );
            request.setOptions(options);
            Response response = restClient.performRequest(request);
            assertThat(response.getStatusLine().getStatusCode(), is(200));
        }
    }

    private SSLContext getSSLContext() throws Exception {
        SSLContext sc = SSLContext.getInstance("TLSv1.2");
        Path truststore = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/truststore-testnode-only.jks");
        KeyStore keyStore = KeyStore.getInstance("JKS");
        try (InputStream stream = Files.newInputStream(truststore)) {
            keyStore.load(stream, "truststore-testnode-only".toCharArray());
        }
        TrustManagerFactory factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        factory.init(keyStore);
        sc.init(null, factory.getTrustManagers(), new SecureRandom());
        return sc;
    }
}
