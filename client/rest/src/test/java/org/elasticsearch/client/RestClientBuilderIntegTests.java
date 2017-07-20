/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import org.apache.http.HttpHost;
import org.codehaus.mojo.animal_sniffer.IgnoreJRERequirement;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.KeyStore;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Integration test to validate the builder builds a client with the correct configuration
 */
//animal-sniffer doesn't like our usage of com.sun.net.httpserver.* classes
@IgnoreJRERequirement
public class RestClientBuilderIntegTests extends RestClientTestCase {

    private static HttpsServer httpsServer;

    @BeforeClass
    public static void startHttpServer() throws Exception {
        httpsServer = MockHttpServer.createHttps(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpsServer.setHttpsConfigurator(new HttpsConfigurator(getSslContext()));
        httpsServer.createContext("/", new ResponseHandler());
        httpsServer.start();
    }

    //animal-sniffer doesn't like our usage of com.sun.net.httpserver.* classes
    @IgnoreJRERequirement
    private static class ResponseHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange httpExchange) throws IOException {
            httpExchange.sendResponseHeaders(200, -1);
            httpExchange.close();
        }
    }

    @AfterClass
    public static void stopHttpServers() throws IOException {
        httpsServer.stop(0);
        httpsServer = null;
    }

    public void testBuilderUsesDefaultSSLContext() throws Exception {
        final SSLContext defaultSSLContext = SSLContext.getDefault();
        try {
            try (RestClient client = buildRestClient()) {
                try {
                    client.performRequest("GET", "/");
                    fail("connection should have been rejected due to SSL handshake");
                } catch (Exception e) {
                    assertThat(e.getMessage(), containsString("General SSLEngine problem"));
                }
            }

            SSLContext.setDefault(getSslContext());
            try (RestClient client = buildRestClient()) {
                Response response = client.performRequest("GET", "/");
                assertEquals(200, response.getStatusLine().getStatusCode());
            }
        } finally {
            SSLContext.setDefault(defaultSSLContext);
        }
    }

    private RestClient buildRestClient() {
        InetSocketAddress address = httpsServer.getAddress();
        return RestClient.builder(new HttpHost(address.getHostString(), address.getPort(), "https")).build();
    }

    private static SSLContext getSslContext() throws Exception {
        SSLContext sslContext = SSLContext.getInstance("TLS");
        try (InputStream in = RestClientBuilderIntegTests.class.getResourceAsStream("/testks.jks")) {
            KeyStore keyStore = KeyStore.getInstance("JKS");
            keyStore.load(in, "password".toCharArray());
            KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
            kmf.init(keyStore, "password".toCharArray());
            TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
            tmf.init(keyStore);
            sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        }
        return sslContext;
    }
}
