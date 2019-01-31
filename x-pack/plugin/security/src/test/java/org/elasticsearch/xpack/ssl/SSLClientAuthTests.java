/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ssl;

import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.xpack.core.TestXPackTransportClient;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;
import org.elasticsearch.xpack.core.ssl.PemUtils;
import org.elasticsearch.xpack.core.ssl.SSLClientAuth;
import org.elasticsearch.xpack.security.LocalStateSecurity;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.security.cert.CertPathBuilderException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class SSLClientAuthTests extends SecurityIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings baseSettings = super.nodeSettings(nodeOrdinal);

        Settings.Builder builder = Settings.builder().put(baseSettings);
        baseSettings.getByPrefix("xpack.security.transport.ssl.")
                .keySet()
                .forEach(k -> {
                    String httpKey = "xpack.security.http.ssl." + k;
                    String value = baseSettings.get("xpack.security.transport.ssl." + k);
                    if (value != null) {
                        builder.put(httpKey, baseSettings.get("xpack.security.transport.ssl." + k));
                    }
                });

        MockSecureSettings secureSettings = (MockSecureSettings) builder.getSecureSettings();
        for (String key : new HashSet<>(secureSettings.getSettingNames())) {
            SecureString value = secureSettings.getString(key);
            if (value == null) {
                try {
                    if (key.startsWith("xpack.security.transport.ssl.")) {
                        byte[] file = toByteArray(secureSettings.getFile(key));
                        secureSettings.setFile(key.replace("xpack.security.transport.ssl.", "xpack.security.http.ssl."), file);
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            } else if (key.startsWith("xpack.security.transport.ssl.")) {
                secureSettings.setString(key.replace("xpack.security.transport.ssl.", "xpack.security.http.ssl."), value.toString());
            }
        }

        return builder
                // invert the require auth settings
                .put("xpack.security.transport.ssl.client_authentication", SSLClientAuth.NONE)
                .put("xpack.security.http.ssl.enabled", true)
                .put("xpack.security.http.ssl.client_authentication", SSLClientAuth.REQUIRED)
                .build();
    }

    @Override
    protected boolean transportSSLEnabled() {
        return true;
    }

    public void testThatHttpFailsWithoutSslClientAuth() throws IOException {
        SSLIOSessionStrategy sessionStrategy = new SSLIOSessionStrategy(SSLContexts.createDefault(), NoopHostnameVerifier.INSTANCE);
        try (RestClient restClient = createRestClient(httpClientBuilder -> httpClientBuilder.setSSLStrategy(sessionStrategy), "https")) {
            restClient.performRequest(new Request("GET", "/"));
            fail("Expected SSLHandshakeException");
        } catch (IOException e) {
            Throwable t = ExceptionsHelper.unwrap(e, CertPathBuilderException.class);
            assertThat(t, instanceOf(CertPathBuilderException.class));
            if (inFipsJvm()) {
                assertThat(t.getMessage(), containsString("Unable to find certificate chain"));
            } else {
                assertThat(t.getMessage(), containsString("unable to find valid certification path to requested target"));
            }
        }
    }

    public void testThatHttpWorksWithSslClientAuth() throws IOException {
        SSLIOSessionStrategy sessionStrategy = new SSLIOSessionStrategy(getSSLContext(), NoopHostnameVerifier.INSTANCE);
        try (RestClient restClient = createRestClient(httpClientBuilder -> httpClientBuilder.setSSLStrategy(sessionStrategy), "https")) {
            Request request = new Request("GET", "/");
            RequestOptions.Builder options = request.getOptions().toBuilder();
            options.addHeader("Authorization", basicAuthHeaderValue(transportClientUsername(), transportClientPassword()));
            request.setOptions(options);
            Response response = restClient.performRequest(request);
            assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
            assertThat(EntityUtils.toString(response.getEntity()), containsString("You Know, for Search"));
        }
    }

    public void testThatTransportWorksWithoutSslClientAuth() throws IOException {
        // specify an arbitrary key and certificate - not the certs needed to connect to the transport protocol
        Path keyPath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient-client-profile.pem");
        Path certPath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient-client-profile.crt");
        Path nodeCertPath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt");

        if (Files.notExists(keyPath) || Files.notExists(certPath)) {
            throw new ElasticsearchException("key or certificate path doesn't exist");
        }

        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.security.transport.ssl.secure_key_passphrase", "testclient-client-profile");
        Settings settings = Settings.builder()
            .put("xpack.security.transport.ssl.enabled", true)
            .put("xpack.security.transport.ssl.client_authentication", SSLClientAuth.NONE)
            .put("xpack.security.transport.ssl.key", keyPath)
            .put("xpack.security.transport.ssl.certificate", certPath)
            .put("xpack.security.transport.ssl.certificate_authorities", nodeCertPath)
            .setSecureSettings(secureSettings)
            .put("cluster.name", internalCluster().getClusterName())
            .put(SecurityField.USER_SETTING.getKey(), transportClientUsername() + ":" + new String(transportClientPassword().getChars()))
            .build();
        try (TransportClient client = new TestXPackTransportClient(settings, LocalStateSecurity.class)) {
            Transport transport = internalCluster().getDataNodeInstance(Transport.class);
            TransportAddress transportAddress = transport.boundAddress().publishAddress();
            client.addTransportAddress(transportAddress);

            assertGreenClusterState(client);
        }
    }

    private SSLContext getSSLContext() {
        try {
            String certPath = "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt";
            String nodeCertPath = "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt";
            String keyPath = "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.pem";
            TrustManager tm = CertParsingUtils.trustManager(CertParsingUtils.readCertificates(Arrays.asList(getDataPath
                (certPath), getDataPath(nodeCertPath))));
            KeyManager km = CertParsingUtils.keyManager(CertParsingUtils.readCertificates(Collections.singletonList(getDataPath
                (certPath))), PemUtils.readPrivateKey(getDataPath(keyPath), "testclient"::toCharArray), "testclient".toCharArray());
            SSLContext context = SSLContext.getInstance("TLSv1.2");
            context.init(new KeyManager[] { km }, new TrustManager[] { tm }, new SecureRandom());
            return context;
        } catch (Exception e) {
            throw new ElasticsearchException("failed to initialize SSLContext", e);
        }
    }

    private byte[] toByteArray(InputStream is) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] internalBuffer = new byte[1024];
        int read = is.read(internalBuffer);
        while (read != -1) {
            baos.write(internalBuffer, 0, read);
            read = is.read(internalBuffer);
        }
        return baos.toByteArray();
    }
}
