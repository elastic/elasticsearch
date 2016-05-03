/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.pki;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.shield.Security;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.shield.transport.SSLClientAuth;
import org.elasticsearch.shield.transport.netty.ShieldNettyHttpServerTransport;
import org.elasticsearch.shield.transport.netty.ShieldNettyTransport;
import org.elasticsearch.test.ShieldIntegTestCase;
import org.elasticsearch.test.ShieldSettingsSource;
import org.elasticsearch.test.rest.client.http.HttpRequestBuilder;
import org.elasticsearch.test.rest.client.http.HttpResponse;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.xpack.XPackPlugin;
import org.junit.BeforeClass;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.SecureRandom;

import static org.elasticsearch.test.ShieldSettingsSource.DEFAULT_PASSWORD;
import static org.elasticsearch.test.ShieldSettingsSource.DEFAULT_USER_NAME;
import static org.elasticsearch.test.ShieldSettingsSource.getSSLSettingsForStore;
import static org.hamcrest.Matchers.is;

public class PkiOptionalClientAuthTests extends ShieldIntegTestCase {

    private static int randomClientPort;

    @BeforeClass
    public static void initPort() {
        randomClientPort = randomIntBetween(49000, 65500);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        String randomClientPortRange = randomClientPort + "-" + (randomClientPort+100);

        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(NetworkModule.HTTP_ENABLED.getKey(), true)
                .put(ShieldNettyHttpServerTransport.SSL_SETTING.getKey(), true)
                .put(ShieldNettyHttpServerTransport.CLIENT_AUTH_SETTING.getKey(), SSLClientAuth.OPTIONAL)
                .put("xpack.security.authc.realms.file.type", "file")
                .put("xpack.security.authc.realms.file.order", "0")
                .put("xpack.security.authc.realms.pki1.type", "pki")
                .put("xpack.security.authc.realms.pki1.order", "1")
                .put("xpack.security.authc.realms.pki1.truststore.path",
                        getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/truststore-testnode-only.jks"))
                .put("xpack.security.authc.realms.pki1.truststore.password", "truststore-testnode-only")
                .put("xpack.security.authc.realms.pki1.files.role_mapping", getDataPath("role_mapping.yml"))
                .put("transport.profiles.want_client_auth.port", randomClientPortRange)
                .put("transport.profiles.want_client_auth.bind_host", "localhost")
                .put("transport.profiles.want_client_auth.xpack.security.ssl.client.auth", SSLClientAuth.OPTIONAL)
                .build();
    }

    @Override
    protected boolean sslTransportEnabled() {
        return true;
    }

    @Override
    protected boolean autoSSLEnabled() {
        return false;
    }

    public void testRestClientWithoutClientCertificate() throws Exception {
        HttpServerTransport httpServerTransport = internalCluster().getDataNodeInstance(HttpServerTransport.class);

        try (CloseableHttpClient httpClient = HttpClients.custom().setSslcontext(getSSLContext()).build()) {
            HttpRequestBuilder requestBuilder = new HttpRequestBuilder(httpClient)
                    .host("localhost")
                    .port(((InetSocketTransportAddress)httpServerTransport.boundAddress().publishAddress()).address().getPort())
                    .protocol("https")
                    .method("GET")
                    .path("/_nodes");
            HttpResponse response = requestBuilder.execute();
            assertThat(response.getStatusCode(), is(401));

            requestBuilder.addHeader(UsernamePasswordToken.BASIC_AUTH_HEADER,
                    UsernamePasswordToken.basicAuthHeaderValue(ShieldSettingsSource.DEFAULT_USER_NAME,
                            new SecuredString(ShieldSettingsSource.DEFAULT_PASSWORD.toCharArray())));
            response = requestBuilder.execute();
            assertThat(response.getStatusCode(), is(200));
        }
    }

    public void testTransportClientWithoutClientCertificate() {
        Transport transport = internalCluster().getDataNodeInstance(Transport.class);
        int port = ((InetSocketTransportAddress)
                randomFrom(transport.profileBoundAddresses().get("want_client_auth").boundAddresses())).address().getPort();

        Settings sslSettingsForStore = getSSLSettingsForStore
                ("/org/elasticsearch/shield/transport/ssl/certs/simple/truststore-testnode-only.jks", "truststore-testnode-only");
        Settings settings = Settings.builder()
                .put(sslSettingsForStore)
                .put(Security.USER_SETTING.getKey(), DEFAULT_USER_NAME + ":" + DEFAULT_PASSWORD)
                .put("cluster.name", internalCluster().getClusterName())
                .put(ShieldNettyTransport.SSL_SETTING.getKey(), true)
                .build();


        try (TransportClient client = TransportClient.builder().settings(settings).addPlugin(XPackPlugin.class).build()) {
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getLoopbackAddress(), port));
            assertGreenClusterState(client);
        }
    }

    private SSLContext getSSLContext() throws Exception {
        SSLContext sc = SSLContext.getInstance("TLSv1.2");
        Path truststore = getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/truststore-testnode-only.jks");
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
