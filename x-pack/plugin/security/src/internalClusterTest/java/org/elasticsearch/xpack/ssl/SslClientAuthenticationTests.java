/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslClientAuthenticationMode;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.security.cert.CertPathBuilderException;
import java.security.cert.CertificateException;
import java.util.HashSet;
import java.util.List;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class SslClientAuthenticationTests extends SecurityIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings baseSettings = super.nodeSettings(nodeOrdinal, otherSettings);

        Settings.Builder builder = Settings.builder().put(baseSettings);
        baseSettings.getByPrefix("xpack.security.transport.ssl.").keySet().forEach(k -> {
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
            .put("xpack.security.transport.ssl.client_authentication", SslClientAuthenticationMode.NONE)
            // Due to the TLSv1.3 bug with session resumption when client authentication is not
            // used, we need to set the protocols since we disabled client auth for transport
            // to avoid failures on pre 11.0.3 JDKs. See #getProtocols
            .putList("xpack.security.transport.ssl.supported_protocols", XPackSettings.DEFAULT_SUPPORTED_PROTOCOLS)
            .put("xpack.security.http.ssl.enabled", true)
            .put("xpack.security.http.ssl.client_authentication", SslClientAuthenticationMode.REQUIRED)
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
            if (inFipsJvm()) {
                Throwable t = ExceptionsHelper.unwrap(e, CertificateException.class);
                assertThat(t, instanceOf(CertificateException.class));
                assertThat(t.getMessage(), containsString("Unable to construct a valid chain"));
            } else {
                Throwable t = ExceptionsHelper.unwrap(e, CertPathBuilderException.class);
                assertThat(t, instanceOf(CertPathBuilderException.class));
                assertThat(t.getMessage(), containsString("unable to find valid certification path to requested target"));
            }
        }
    }

    public void testThatHttpWorksWithSslClientAuth() throws IOException {
        SSLIOSessionStrategy sessionStrategy = new SSLIOSessionStrategy(getSSLContext(), NoopHostnameVerifier.INSTANCE);
        try (RestClient restClient = createRestClient(httpClientBuilder -> httpClientBuilder.setSSLStrategy(sessionStrategy), "https")) {
            Request request = new Request("GET", "/");
            RequestOptions.Builder options = request.getOptions().toBuilder();
            options.addHeader("Authorization", basicAuthHeaderValue(nodeClientUsername(), nodeClientPassword()));
            request.setOptions(options);
            Response response = restClient.performRequest(request);
            assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
            assertThat(EntityUtils.toString(response.getEntity()), containsString("You Know, for Search"));
        }
    }

    private SSLContext getSSLContext() {
        try {
            String certPathName = "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt";
            String nodeCertPath = "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt";
            String nodeEcCertPath = "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode_ec.crt";
            String keyPathName = "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.pem";

            final Path certPath = getDataPath(certPathName);
            final Path keyPath = getDataPath(keyPathName);

            final List<Path> caCerts = List.of(certPath, getDataPath(nodeCertPath), getDataPath(nodeEcCertPath));
            final TrustManager tm = CertParsingUtils.getTrustManagerFromPEM(caCerts);

            KeyManager km = CertParsingUtils.getKeyManagerFromPEM(certPath, keyPath, "testclient".toCharArray());
            SSLContext context = SSLContext.getInstance(inFipsJvm() ? "TLSv1.2" : randomFrom("TLSv1.3", "TLSv1.2"));
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
