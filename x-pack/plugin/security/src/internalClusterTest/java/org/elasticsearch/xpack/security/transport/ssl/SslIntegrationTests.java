/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.ssl;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
import org.elasticsearch.xpack.core.ssl.SSLConfiguration;
import org.elasticsearch.xpack.core.ssl.SSLService;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.TrustManagerFactory;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Locale;

import static org.elasticsearch.test.SecuritySettingsSource.addSSLSettingsForNodePEMFiles;
import static org.elasticsearch.test.SecuritySettingsSource.addSSLSettingsForPEMFiles;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;

public class SslIntegrationTests extends SecurityIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        final Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal));
        addSSLSettingsForNodePEMFiles(builder, "xpack.security.http.", randomBoolean());
        return builder.put("xpack.security.http.ssl.enabled", true).build();
    }

    @Override
    protected boolean transportSSLEnabled() {
        return true;
    }

    public void testThatConnectionToHTTPWorks() throws Exception {
        Settings.Builder builder = Settings.builder().put("xpack.security.http.ssl.enabled", true);
        addSSLSettingsForPEMFiles(
            builder, "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.pem",
            "testclient",
            "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt",
            "xpack.security.http.",
            Arrays.asList("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"));
        SSLService service = new SSLService(TestEnvironment.newEnvironment(buildEnvSettings(builder.build())));

        CredentialsProvider provider = new BasicCredentialsProvider();
        provider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(nodeClientUsername(),
                new String(nodeClientPassword().getChars())));
        SSLConfiguration sslConfiguration = service.getSSLConfiguration("xpack.security.http.ssl");
        try (CloseableHttpClient client = HttpClients.custom()
                .setSSLSocketFactory(new SSLConnectionSocketFactory(service.sslSocketFactory(sslConfiguration),
                        SSLConnectionSocketFactory.getDefaultHostnameVerifier()))
                .setDefaultCredentialsProvider(provider).build();
             CloseableHttpResponse response = SocketAccess.doPrivileged(() -> client.execute(new HttpGet(getNodeUrl())))) {
            assertThat(response.getStatusLine().getStatusCode(), is(200));
            String data = Streams.copyToString(new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8));
            assertThat(data, containsString("You Know, for Search"));
        }
    }

    public void testThatHttpUsingSSLv3IsRejected() throws Exception {
        assumeFalse("Can't run in a FIPS JVM as we can't even get an instance of SSL SSL Context", inFipsJvm());
        SSLContext sslContext = SSLContext.getInstance("SSL");
        TrustManagerFactory factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        factory.init((KeyStore) null);

        sslContext.init(null, factory.getTrustManagers(), new SecureRandom());
        SSLConnectionSocketFactory sf = new SSLConnectionSocketFactory(sslContext, new String[]{ "SSLv3" }, null,
                NoopHostnameVerifier.INSTANCE);
        try (CloseableHttpClient client = HttpClients.custom().setSSLSocketFactory(sf).build()) {
            expectThrows(SSLHandshakeException.class, () -> SocketAccess.doPrivileged(() -> client.execute(new HttpGet(getNodeUrl()))));
        }
    }

    private String getNodeUrl() {
        TransportAddress transportAddress =
                randomFrom(internalCluster().getInstance(HttpServerTransport.class).boundAddress().boundAddresses());
        final InetSocketAddress inetSocketAddress = transportAddress.address();
        return String.format(Locale.ROOT, "https://%s/", NetworkAddress.format(inetSocketAddress));
    }
}
