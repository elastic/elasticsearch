/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.ssl;

import com.google.common.base.Charsets;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.net.InetAddresses;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.test.ShieldIntegrationTest;
import org.elasticsearch.transport.Transport;
import org.junit.Test;

import javax.net.ssl.*;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Locale;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.hamcrest.Matchers.*;

@ClusterScope(scope = Scope.SUITE)
public class SslIntegrationTests extends ShieldIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder().put(super.nodeSettings(nodeOrdinal))
                .put(InternalNode.HTTP_ENABLED, true)
                .put("shield.http.ssl", true).build();
    }

    @Override
    protected boolean sslTransportEnabled() {
        return true;
    }

    // no SSL exception as this is the exception is returned when connecting
    @Test(expected = NoNodeAvailableException.class)
    public void testThatUnconfiguredCiphersAreRejected() {
        try(TransportClient transportClient = new TransportClient(settingsBuilder()
                .put(transportClientSettings())
                .put("name", "programmatic_transport_client")
                .put("cluster.name", internalCluster().getClusterName())
                .putArray("shield.ssl.ciphers", new String[]{"TLS_ECDH_anon_WITH_RC4_128_SHA", "SSL_RSA_WITH_3DES_EDE_CBC_SHA"})
                .build())) {

            TransportAddress transportAddress = internalCluster().getInstance(Transport.class).boundAddress().boundAddress();
            transportClient.addTransportAddress(transportAddress);

            transportClient.admin().cluster().prepareHealth().get();
        }
    }

    @Test
    public void testThatConnectionToHTTPWorks() throws Exception {
        TrustManager[] trustAllCerts = new TrustManager[]{
                new X509TrustManager() {
                    public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                        return null;
                    }

                    public void checkClientTrusted(
                            java.security.cert.X509Certificate[] certs, String authType) {
                    }

                    public void checkServerTrusted(
                            java.security.cert.X509Certificate[] certs, String authType) {
                    }
                }
        };

        // Install the all-trusting trust manager
        SSLContext sc = SSLContext.getInstance("SSL");
        sc.init(null, trustAllCerts, new java.security.SecureRandom());
        HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
        // totally secure
        HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {
            @Override
            public boolean verify(String s, SSLSession sslSession) {
                return true;
            }
        });

        TransportAddress transportAddress = internalCluster().getInstance(HttpServerTransport.class).boundAddress().boundAddress();
        assertThat(transportAddress, is(instanceOf(InetSocketTransportAddress.class)));
        InetSocketTransportAddress inetSocketTransportAddress = (InetSocketTransportAddress) transportAddress;
        String url = String.format(Locale.ROOT, "https://%s:%s/", InetAddresses.toUriString(inetSocketTransportAddress.address().getAddress()), inetSocketTransportAddress.address().getPort());

        HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        connection.setRequestProperty(UsernamePasswordToken.BASIC_AUTH_HEADER, UsernamePasswordToken.basicAuthHeaderValue(nodeClientUsername(), nodeClientPassword()));
        connection.connect();

        assertThat(connection.getResponseCode(), is(200));
        String data = Streams.copyToString(new InputStreamReader(connection.getInputStream(), Charsets.UTF_8));
        assertThat(data, containsString("You Know, for Search"));
    }
}
