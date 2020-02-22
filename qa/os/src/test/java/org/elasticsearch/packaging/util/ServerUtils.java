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

package org.elasticsearch.packaging.util;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.LayeredConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

public class ServerUtils {

    private static final Logger logger =  LogManager.getLogger(ServerUtils.class);

    private static String SECURITY_ENABLED = "xpack.security.enabled: true";
    private static String SSL_ENABLED = "xpack.security.http.ssl.enabled: true";

    // generous timeout  as nested virtualization can be quite slow ...
    private static final long waitTime = TimeUnit.MINUTES.toMillis(3);
    private static final long timeoutLength = TimeUnit.SECONDS.toMillis(30);
    private static final long requestInterval = TimeUnit.SECONDS.toMillis(5);

    public static void waitForElasticsearch(Installation installation) throws Exception {
        boolean xpackEnabled = false;

        // TODO: need a way to check if docker has security enabled, the yml config is not bind mounted so can't look from here
        if (installation.distribution.packaging != Distribution.Packaging.DOCKER) {
            Path configFilePath = installation.config("elasticsearch.yml");
            // this is fragile, but currently doesn't deviate from a single line enablement and not worth the parsing effort
            String configFile = Files.readString(configFilePath, StandardCharsets.UTF_8);
            xpackEnabled = configFile.contains(SECURITY_ENABLED) || configFile.contains(SSL_ENABLED);
        }

        if (xpackEnabled) {
            // with security enabled, we may or may not have setup a user/pass, so we use a more generic port being available check.
            // this isn't as good as a health check, but long term all this waiting should go away when node startup does not
            // make the http port available until the system is really ready to serve requests
            waitForXpack();
        } else {
            waitForElasticsearch("green", null, installation, null, null);
        }
    }

    /**
     * Executes the supplied request, optionally applying HTTP basic auth if the
     * username and pasword field are supplied.
     * @param request the request to execute
     * @param username the username to supply, or null
     * @param password the password to supply, or null
     * @param caCert path to the ca certificate the server side ssl cert was generated from, or no if not using ssl
     * @return the response from the server
     * @throws IOException if an error occurs
     */
    private static HttpResponse execute(Request request, String username, String password, Path caCert) throws Exception {
        final Executor executor;
        if (caCert != null) {
            try (InputStream inStream = Files.newInputStream(caCert)) {
                CertificateFactory cf = CertificateFactory.getInstance("X.509");
                X509Certificate cert = (X509Certificate) cf.generateCertificate(inStream);
                KeyStore truststore = KeyStore.getInstance(KeyStore.getDefaultType());
                truststore.load(null, null);
                truststore.setCertificateEntry("myClusterCA", cert);
                KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(truststore, null);
                TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(truststore);
                SSLContext context = SSLContext.getInstance("TLSv1.2");
                context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
                final LayeredConnectionSocketFactory ssl = new SSLConnectionSocketFactory(context);
                final Registry<ConnectionSocketFactory> sfr = RegistryBuilder.<ConnectionSocketFactory>create()
                    .register("http", PlainConnectionSocketFactory.getSocketFactory())
                    .register("https", ssl)
                    .build();
                PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager(sfr);
                connectionManager.setDefaultMaxPerRoute(100);
                connectionManager.setMaxTotal(200);
                connectionManager.setValidateAfterInactivity(1000);
                executor = Executor.newInstance(HttpClientBuilder.create()
                    .setConnectionManager(connectionManager)
                    .build());
            }
        } else {
            executor = Executor.newInstance();
        }

        if (username != null && password != null) {
            executor.auth(username, password);
            executor.authPreemptive(new HttpHost("localhost", 9200));
        }
        return executor.execute(request).returnResponse();
    }

    // polls every second for Elasticsearch to be running on 9200
    private static void waitForXpack() {
        int retries = 60;
        while (retries > 0) {
            retries -= 1;
            try (Socket s = new Socket(InetAddress.getLoopbackAddress(), 9200)) {
                return;
            } catch (IOException e) {
                // ignore, only want to establish a connection
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        throw new RuntimeException("Elasticsearch (with x-pack) did not start");
    }

    public static void waitForElasticsearch(
        String status,
        String index,
        Installation installation,
        String username,
        String password
    ) throws Exception {

        Objects.requireNonNull(status);

        // we loop here rather than letting httpclient handle retries so we can measure the entire waiting time
        final long startTime = System.currentTimeMillis();
        long lastRequest = 0;
        long timeElapsed = 0;
        boolean started = false;
        Throwable thrownException = null;

        Path caCert = installation.config("certs/ca/ca.crt");
        if (Files.exists(caCert) == false) {
            caCert = null; // no cert, so don't use ssl
        }

        while (started == false && timeElapsed < waitTime) {
            if (System.currentTimeMillis() - lastRequest > requestInterval) {
                try {

                    final HttpResponse response = execute(
                        Request
                            .Get("http://localhost:9200/_cluster/health")
                            .connectTimeout((int) timeoutLength)
                            .socketTimeout((int) timeoutLength),
                        username,
                        password,
                        caCert
                    );

                    if (response.getStatusLine().getStatusCode() >= 300) {
                        final String statusLine = response.getStatusLine().toString();
                        final String body = EntityUtils.toString(response.getEntity());
                        throw new RuntimeException("Connecting to elasticsearch cluster health API failed:\n" + statusLine + "\n" + body);
                    }

                    started = true;

                } catch (IOException e) {
                    if (thrownException == null) {
                        thrownException = e;
                    } else {
                        thrownException.addSuppressed(e);
                    }
                }

                lastRequest = System.currentTimeMillis();
            }

            timeElapsed = System.currentTimeMillis() - startTime;
        }

        if (started == false) {
            if (installation != null) {
                FileUtils.logAllLogs(installation.logs, logger);
            }

            throw new RuntimeException("Elasticsearch did not start", thrownException);
        }

        final String url;
        if (index == null) {
            url = "http://localhost:9200/_cluster/health?wait_for_status=" + status + "&timeout=60s&pretty";
        } else {
            url = "http://localhost:9200/_cluster/health/" + index + "?wait_for_status=" + status + "&timeout=60s&pretty";
        }

        final String body = makeRequest(Request.Get(url), username, password, caCert);
        assertThat("cluster health response must contain desired status", body, containsString(status));
    }

    public static void runElasticsearchTests() throws Exception {
        makeRequest(
            Request.Post("http://localhost:9200/library/_doc/1?refresh=true&pretty")
                .bodyString("{ \"title\": \"Book #1\", \"pages\": 123 }", ContentType.APPLICATION_JSON));

        makeRequest(
            Request.Post("http://localhost:9200/library/_doc/2?refresh=true&pretty")
                .bodyString("{ \"title\": \"Book #2\", \"pages\": 456 }", ContentType.APPLICATION_JSON));

        String count = makeRequest(Request.Get("http://localhost:9200/_count?pretty"));
        assertThat(count, containsString("\"count\" : 2"));

        makeRequest(Request.Delete("http://localhost:9200/_all"));
    }

    public static String makeRequest(Request request) throws Exception {
        return makeRequest(request, null, null, null);
    }

    public static String makeRequest(Request request, String username, String password, Path caCert) throws Exception {
        final HttpResponse response = execute(request, username, password, caCert);
        final String body = EntityUtils.toString(response.getEntity());

        if (response.getStatusLine().getStatusCode() >= 300) {
            throw new RuntimeException("Request failed:\n" + response.getStatusLine().toString() + "\n" + body);
        }

        return body;
    }
}
