/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.net.client;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;

import org.elasticsearch.common.io.Streams;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

@SuppressWarnings("restriction")
public class BasicSSLServer {

    @SuppressForbidden(reason = "it's a test, not production code")
    private static class EchoHandler implements HttpHandler {
        public void handle(HttpExchange e) throws IOException {
            e.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0);
            Streams.copy(e.getRequestBody(), e.getResponseBody());
        }
    }

    private HttpsServer server;
    private ExecutorService executor;

    public void start(int port) throws Exception {

        // similar to Executors.newCached but with a smaller bound and much smaller keep-alive
        executor = Executors.newCachedThreadPool();

        server = HttpsServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), port);

        server.setHttpsConfigurator(httpConfigurator());
        server.createContext("/ssl", new EchoHandler());
        server.setExecutor(executor);
        server.start();
    }

    private static HttpsConfigurator httpConfigurator() throws Exception {
        char[] pass = "password".toCharArray();
        // so this works on JDK 7 as well
        SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
        KeyStore ks = KeyStore.getInstance("JKS");
        KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");

        ks.load(BasicSSLServer.class.getResourceAsStream("/ssl/server.keystore"), pass);
        kmf.init(ks, pass);

        // NOCOMMIT I think we could share a key rather than trust all, right?
        TrustManager[] trustAll = new TrustManager[] {
                new X509TrustManager() {
                    @Override
                    public X509Certificate[] getAcceptedIssuers() {
                        return new X509Certificate[0];
                    }
                    
                    @Override
                    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {}
                    
                    @Override
                    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {}
                }
        };
        
        // chain
        sslContext.init(kmf.getKeyManagers(), trustAll, null);

        HttpsConfigurator configurator = new HttpsConfigurator(sslContext) {
            public void configure(HttpsParameters params) {
                try {
                    SSLContext c = getSSLContext();
                    SSLParameters defaults = c.getDefaultSSLParameters();
                    params.setSSLParameters(defaults);
                    // client can send a cert if they want to (use Need to force the client to present one)
                    //params.setWantClientAuth(true);

                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
        };

        return configurator;
    }

    public void stop() {
        server.stop(1);
        server = null;
        executor.shutdownNow();
        executor = null;
    }

    public InetSocketAddress address() {
        return server != null ? server.getAddress() : null;
    }

    public String url() {
        return server != null ? "https://localhost:" + address().getPort() + "/ssl" : null;
    }
}