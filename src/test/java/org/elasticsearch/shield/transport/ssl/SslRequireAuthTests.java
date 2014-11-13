/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.ssl;

import com.carrotsearch.ant.tasks.junit4.dependencies.com.google.common.base.Charsets;
import com.google.common.net.InetAddresses;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.shield.test.ShieldIntegrationTest;
import org.junit.Test;

import javax.net.ssl.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.Locale;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.hamcrest.Matchers.*;

/**
 *
 */
@ClusterScope(scope = Scope.SUITE, numDataNodes = 1, numClientNodes = 0)
public class SslRequireAuthTests extends ShieldIntegrationTest {

    public static final HostnameVerifier HOSTNAME_VERIFIER = new HostnameVerifier() {
        @Override
        public boolean verify(String s, SSLSession sslSession) {
            return true;
        }
    };

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(InternalNode.HTTP_ENABLED, true)
                .put(getSSLSettingsForStore("certs/simple/testnode.jks", "testnode"))
                .put("shield.transport.ssl.require.client.auth", true)
                .put("shield.http.ssl.require.client.auth", true)
                .build();
    }

    @Override
    protected Settings transportClientSettings() {
        return ImmutableSettings.builder()
                .put(super.transportClientSettings())
                .put(getSSLSettingsForStore("certs/simple/testclient.jks", "testclient"))
                .build();
    }

    @Test
    public void testThatConnectionToHTTPWorks() throws Exception {
        File store = new File(getClass().getResource("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.jks").toURI());

        KeyStore ks;
        KeyManagerFactory kmf;
        try (FileInputStream in = new FileInputStream(store)){
            // Load KeyStore
            ks = KeyStore.getInstance("jks");
            ks.load(in, "testnode".toCharArray());

            // Initialize KeyManagerFactory
            kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ks, "testnode".toCharArray());
        }

        TrustManagerFactory trustFactory;
        try (FileInputStream in = new FileInputStream(store)) {
            // Load TrustStore
            ks.load(in, "testnode".toCharArray());

            // Initialize a trust manager factory with the trusted store
            trustFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustFactory.init(ks);

            // Retrieve the trust managers from the factory
        }
        setupTrustManagers(kmf.getKeyManagers(), trustFactory.getTrustManagers());

        TransportAddress transportAddress = internalCluster().getInstance(HttpServerTransport.class).boundAddress().boundAddress();
        assertThat(transportAddress, is(instanceOf(InetSocketTransportAddress.class)));
        InetSocketTransportAddress inetSocketTransportAddress = (InetSocketTransportAddress) transportAddress;
        String url = String.format(Locale.ROOT, "https://%s:%s/", InetAddresses.toUriString(inetSocketTransportAddress.address().getAddress()), inetSocketTransportAddress.address().getPort());

        HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        connection.setRequestProperty("Authorization", UsernamePasswordToken.basicAuthHeaderValue(getClientUsername(), getClientPassword()));
        connection.connect();

        assertThat(connection.getResponseCode(), is(200));
        String data = Streams.copyToString(new InputStreamReader(connection.getInputStream(), Charsets.UTF_8));
        assertThat(data, containsString("You Know, for Search"));
    }

    private void setupTrustManagers(KeyManager[] keyManagers, TrustManager[] trustManagers) throws Exception {
        SSLContext sc = SSLContext.getInstance("TLS");
        sc.init(keyManagers, trustManagers, new SecureRandom());
        HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
        // totally secure
        HttpsURLConnection.setDefaultHostnameVerifier(HOSTNAME_VERIFIER);
    }

    private void setupTrustManagers(TrustManager[] trustManagers) throws Exception {
        setupTrustManagers(null, trustManagers);
    }
}
