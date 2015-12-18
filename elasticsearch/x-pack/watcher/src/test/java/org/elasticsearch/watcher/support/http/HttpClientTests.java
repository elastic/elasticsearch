/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.http;

import com.squareup.okhttp.mockwebserver.MockResponse;
import com.squareup.okhttp.mockwebserver.MockWebServer;
import com.squareup.okhttp.mockwebserver.RecordedRequest;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.Network;
import org.elasticsearch.watcher.support.http.auth.HttpAuthRegistry;
import org.elasticsearch.watcher.support.http.auth.basic.BasicAuth;
import org.elasticsearch.watcher.support.http.auth.basic.BasicAuthFactory;
import org.elasticsearch.watcher.support.secret.SecretService;
import org.junit.After;
import org.junit.Before;

import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.UnrecoverableKeyException;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

/**
 */
public class HttpClientTests extends ESTestCase {

    private MockWebServer webServer;
    private HttpClient httpClient;
    private HttpAuthRegistry authRegistry;
    private SecretService secretService;
    private Environment environment = new Environment(Settings.builder().put("path.home", createTempDir()).build());

    private int webPort;

    @Before
    public void init() throws Exception {
        secretService = new SecretService.PlainText();
        authRegistry = new HttpAuthRegistry(singletonMap(BasicAuth.TYPE, new BasicAuthFactory(secretService)));
        webServer = startWebServer(9200, 9300);
        webPort = webServer.getPort();
        httpClient = new HttpClient(Settings.EMPTY, authRegistry, environment).start();
    }

    @After
    public void after() throws Exception {
        webServer.shutdown();
    }

    public void testBasics() throws Exception {
        int responseCode = randomIntBetween(200, 203);
        String body = randomAsciiOfLengthBetween(2, 8096);
        webServer.enqueue(new MockResponse().setResponseCode(responseCode).setBody(body));


        HttpRequest.Builder requestBuilder = HttpRequest.builder("localhost", webPort)
                .method(HttpMethod.POST)
                .path("/" + randomAsciiOfLength(5));

        String paramKey = randomAsciiOfLength(3);
        String paramValue = randomAsciiOfLength(3);
        requestBuilder.setParam(paramKey, paramValue);

        // Certain headers keys like via and host are illegal and the jdk http client ignores those, so lets
        // prepend all keys with `_`, so we don't run into a failure because randomly a restricted header was used:
        String headerKey = "_" + randomAsciiOfLength(3);
        String headerValue = randomAsciiOfLength(3);
        requestBuilder.setHeader(headerKey, headerValue);

        requestBuilder.body(randomAsciiOfLength(5));
        HttpRequest request = requestBuilder.build();

        HttpResponse response = httpClient.execute(request);
        RecordedRequest recordedRequest = webServer.takeRequest();


        assertThat(response.status(), equalTo(responseCode));
        assertThat(response.body().toUtf8(), equalTo(body));
        assertThat(webServer.getRequestCount(), equalTo(1));
        assertThat(recordedRequest.getBody().readString(StandardCharsets.UTF_8), equalTo(request.body()));
        assertThat(recordedRequest.getPath().split("\\?")[0], equalTo(request.path()));
        assertThat(recordedRequest.getPath().split("\\?")[1], equalTo(paramKey + "=" + paramValue));
        assertThat(recordedRequest.getHeader(headerKey), equalTo(headerValue));
    }

    public void testNoQueryString() throws Exception {
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("body"));
        HttpRequest.Builder requestBuilder = HttpRequest.builder("localhost", webPort)
                .method(HttpMethod.GET)
                .path("/test");

        HttpResponse response = httpClient.execute(requestBuilder.build());
        assertThat(response.status(), equalTo(200));
        assertThat(response.body().toUtf8(), equalTo("body"));

        RecordedRequest recordedRequest = webServer.takeRequest();
        assertThat(recordedRequest.getPath(), equalTo("/test"));
        assertThat(recordedRequest.getBody().readUtf8Line(), nullValue());
    }

    public void testUrlEncoding() throws Exception{
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("body"));
        HttpRequest.Builder requestBuilder = HttpRequest.builder("localhost", webPort)
                .method(HttpMethod.GET)
                .path("/test")
                .setParam("key", "value 123:123");

        HttpResponse response = httpClient.execute(requestBuilder.build());
        assertThat(response.status(), equalTo(200));
        assertThat(response.body().toUtf8(), equalTo("body"));

        RecordedRequest recordedRequest = webServer.takeRequest();
        assertThat(recordedRequest.getPath(), equalTo("/test?key=value%20123:123"));
        assertThat(recordedRequest.getBody().readUtf8Line(), nullValue());
    }

    public void testBasicAuth() throws Exception {
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("body"));
        HttpRequest.Builder request = HttpRequest.builder("localhost", webPort)
                .method(HttpMethod.POST)
                .path("/test")
                .auth(new BasicAuth("user", "pass".toCharArray()))
                .body("body");
        HttpResponse response = httpClient.execute(request.build());
        assertThat(response.status(), equalTo(200));
        assertThat(response.body().toUtf8(), equalTo("body"));
        RecordedRequest recordedRequest = webServer.takeRequest();
        assertThat(recordedRequest.getPath(), equalTo("/test"));
        assertThat(recordedRequest.getHeader("Authorization"), equalTo("Basic dXNlcjpwYXNz"));
    }

    public void testHttps() throws Exception {
        Path resource = getDataPath("/org/elasticsearch/shield/keystore/truststore-testnode-only.jks");

        Settings settings;
        if (randomBoolean()) {
            settings = Settings.builder()
                    .put(HttpClient.SETTINGS_SSL_TRUSTSTORE, resource.toString())
                    .put(HttpClient.SETTINGS_SSL_TRUSTSTORE_PASSWORD, "truststore-testnode-only")
                    .build();
        } else {
            settings = Settings.builder()
                    .put(HttpClient.SETTINGS_SSL_SHIELD_TRUSTSTORE, resource.toString())
                    .put(HttpClient.SETTINGS_SSL_SHIELD_TRUSTSTORE_PASSWORD, "truststore-testnode-only")
                    .build();
        }
        HttpClient httpClient = new HttpClient(settings, authRegistry, environment).start();

        // We can't use the client created above for the server since it is only a truststore
        webServer.useHttps(new HttpClient(Settings.builder()
                .put(HttpClient.SETTINGS_SSL_KEYSTORE, getDataPath("/org/elasticsearch/shield/keystore/testnode.jks"))
                .put(HttpClient.SETTINGS_SSL_KEYSTORE_PASSWORD, "testnode")
                .build(), authRegistry, environment)
                .start()
                .getSslSocketFactory(), false);

        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("body"));
        HttpRequest.Builder request = HttpRequest.builder("localhost", webPort)
                .scheme(Scheme.HTTPS)
                .path("/test")
                .body("body");
        HttpResponse response = httpClient.execute(request.build());
        assertThat(response.status(), equalTo(200));
        assertThat(response.body().toUtf8(), equalTo("body"));
        RecordedRequest recordedRequest = webServer.takeRequest();
        assertThat(recordedRequest.getPath(), equalTo("/test"));
        assertThat(recordedRequest.getBody().readUtf8Line(), equalTo("body"));
    }

    public void testHttpsClientAuth() throws Exception {
        Path resource = getDataPath("/org/elasticsearch/shield/keystore/testnode.jks");
        Settings settings;
        if (randomBoolean()) {
            settings = Settings.builder()
                    .put(HttpClient.SETTINGS_SSL_KEYSTORE, resource.toString())
                    .put(HttpClient.SETTINGS_SSL_KEYSTORE_PASSWORD, "testnode")
                    .build();
        } else {
            settings = Settings.builder()
                    .put(HttpClient.SETTINGS_SSL_SHIELD_KEYSTORE, resource.toString())
                    .put(HttpClient.SETTINGS_SSL_SHIELD_KEYSTORE_PASSWORD, "testnode")
                    .build();
        }

        HttpClient httpClient = new HttpClient(settings, authRegistry, environment).start();
        webServer.useHttps(new ClientAuthRequiringSSLSocketFactory(httpClient.getSslSocketFactory()), false);

        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("body"));
        HttpRequest.Builder request = HttpRequest.builder("localhost", webPort)
                .scheme(Scheme.HTTPS)
                .path("/test")
                .body("body");
        HttpResponse response = httpClient.execute(request.build());
        assertThat(response.status(), equalTo(200));
        assertThat(response.body().toUtf8(), equalTo("body"));
        RecordedRequest recordedRequest = webServer.takeRequest();
        assertThat(recordedRequest.getPath(), equalTo("/test"));
        assertThat(recordedRequest.getBody().readUtf8Line(), equalTo("body"));
    }

    public void testHttpClientReadKeyWithDifferentPassword() throws Exception {
        // This truststore doesn't have a cert with a valid SAN so hostname verification will fail if used
        Path resource = getDataPath("/org/elasticsearch/shield/keystore/testnode-different-passwords.jks");

        Settings settings;
        final boolean watcherSettings = randomBoolean();
        if (watcherSettings) {
            settings = Settings.builder()
                    .put(HttpClient.SETTINGS_SSL_KEYSTORE, resource.toString())
                    .put(HttpClient.SETTINGS_SSL_KEYSTORE_PASSWORD, "testnode")
                    .put(HttpClient.SETTINGS_SSL_KEYSTORE_KEY_PASSWORD, "testnode1")
                    .build();
        } else {
            settings = Settings.builder()
                    .put(HttpClient.SETTINGS_SSL_SHIELD_KEYSTORE, resource.toString())
                    .put(HttpClient.SETTINGS_SSL_SHIELD_KEYSTORE_PASSWORD, "testnode")
                    .put(HttpClient.SETTINGS_SSL_SHIELD_KEYSTORE_KEY_PASSWORD, "testnode1")
                    .build();
        }

        HttpClient httpClient = new HttpClient(settings, authRegistry, environment).start();
        assertThat(httpClient.getSslSocketFactory(), notNullValue());

        Settings.Builder badSettings = Settings.builder().put(settings);
        if (watcherSettings) {
            badSettings.remove(HttpClient.SETTINGS_SSL_KEYSTORE_KEY_PASSWORD);
        } else {
            badSettings.remove(HttpClient.SETTINGS_SSL_SHIELD_KEYSTORE_KEY_PASSWORD);
        }

        try {
            new HttpClient(badSettings.build(), authRegistry, environment).start();
            fail("an exception should have been thrown since the key is not recoverable without the password");
        } catch (Exception e) {
            UnrecoverableKeyException rootCause = (UnrecoverableKeyException) ExceptionsHelper.unwrap(e, UnrecoverableKeyException.class);
            assertThat(rootCause, notNullValue());
            assertThat(rootCause.getMessage(), containsString("Cannot recover key"));
        }
    }

    public void test400Code() throws Exception {
        webServer.enqueue(new MockResponse().setResponseCode(400));
        HttpRequest.Builder request = HttpRequest.builder("localhost", webPort)
                .method(HttpMethod.POST)
                .path("/test")
                .auth(new BasicAuth("user", "pass".toCharArray()))
                .body("body");
        HttpResponse response = httpClient.execute(request.build());
        assertThat(response.status(), equalTo(400));
        assertThat(response.hasContent(), is(false));
        assertThat(response.body(), nullValue());
    }

    @Network
    public void testHttpsWithoutTruststore() throws Exception {
        HttpClient httpClient = new HttpClient(Settings.EMPTY, authRegistry, environment).start();
        assertThat(httpClient.getSslSocketFactory(), nullValue());

        // Known server with a valid cert from a commercial CA
        HttpRequest.Builder request = HttpRequest.builder("www.elastic.co", 443).scheme(Scheme.HTTPS);
        HttpResponse response = httpClient.execute(request.build());
        assertThat(response.status(), equalTo(200));
        assertThat(response.hasContent(), is(true));
        assertThat(response.body(), notNullValue());
    }

    @Network
    public void testHttpsWithoutTruststoreAndSSLIntegrationActive() throws Exception {
        // Add some settings with  SSL prefix to force socket factory creation
        String setting = (randomBoolean() ? HttpClient.SETTINGS_SSL_PREFIX : HttpClient.SETTINGS_SSL_SHIELD_PREFIX) +
                "foo.bar";
        Settings settings = Settings.builder()
                .put(setting, randomBoolean())
                .build();
        HttpClient httpClient = new HttpClient(settings, authRegistry, environment).start();
        assertThat(httpClient.getSslSocketFactory(), notNullValue());

        // Known server with a valid cert from a commercial CA
        HttpRequest.Builder request = HttpRequest.builder("www.elastic.co", 443).scheme(Scheme.HTTPS);
        HttpResponse response = httpClient.execute(request.build());
        assertThat(response.status(), equalTo(200));
        assertThat(response.hasContent(), is(true));
        assertThat(response.body(), notNullValue());
    }

    public void testThatProxyCanBeConfigured() throws Exception {
        // this test fakes a proxy server that sends a response instead of forwarding it to the mock web server
        MockWebServer proxyServer = startWebServer(62000, 63000);
        proxyServer.enqueue(new MockResponse().setResponseCode(200).setBody("fullProxiedContent"));

        try {
            Settings settings = Settings.builder()
                    .put(HttpClient.SETTINGS_PROXY_HOST, "localhost")
                    .put(HttpClient.SETTINGS_PROXY_PORT, proxyServer.getPort())
                    .build();
            HttpClient httpClient = new HttpClient(settings, authRegistry, environment).start();

            HttpRequest.Builder requestBuilder = HttpRequest.builder("localhost", webPort)
                    .method(HttpMethod.GET)
                    .path("/");

            HttpResponse response = httpClient.execute(requestBuilder.build());
            assertThat(response.status(), equalTo(200));
            assertThat(response.body().toUtf8(), equalTo("fullProxiedContent"));

            // ensure we hit the proxyServer and not the webserver
            assertThat(webServer.getRequestCount(), equalTo(0));
            assertThat(proxyServer.getRequestCount(), equalTo(1));
        } finally {
            proxyServer.shutdown();
        }
    }

    public void testThatProxyCanBeOverriddenByRequest() throws Exception {
        // this test fakes a proxy server that sends a response instead of forwarding it to the mock web server
        MockWebServer proxyServer = startWebServer(62000, 63000);
        proxyServer.enqueue(new MockResponse().setResponseCode(200).setBody("fullProxiedContent"));

        try {
            Settings settings = Settings.builder()
                    .put(HttpClient.SETTINGS_PROXY_HOST, "localhost")
                    .put(HttpClient.SETTINGS_PROXY_PORT, proxyServer.getPort() + 1)
                    .build();
            HttpClient httpClient = new HttpClient(settings, authRegistry, environment).start();

            HttpRequest.Builder requestBuilder = HttpRequest.builder("localhost", webPort)
                    .method(HttpMethod.GET)
                    .proxy(new HttpProxy("localhost", proxyServer.getPort()))
                    .path("/");

            HttpResponse response = httpClient.execute(requestBuilder.build());
            assertThat(response.status(), equalTo(200));
            assertThat(response.body().toUtf8(), equalTo("fullProxiedContent"));

            // ensure we hit the proxyServer and not the webserver
            assertThat(webServer.getRequestCount(), equalTo(0));
            assertThat(proxyServer.getRequestCount(), equalTo(1));
        } finally {
            proxyServer.shutdown();
        }
    }

    private MockWebServer startWebServer(int startPort, int endPort) throws IOException {
        for (int port = startPort; port < endPort; port++) {
            try {
                MockWebServer mockWebServer = new MockWebServer();
                mockWebServer.start(port);
                return mockWebServer;
            } catch (BindException be) {
                logger.warn("port [{}] was already in use trying next port", webPort);
            }
        }
        throw new ElasticsearchException("unable to find open port between 9200 and 9300");
    }

    static class ClientAuthRequiringSSLSocketFactory extends SSLSocketFactory {
        final SSLSocketFactory delegate;

        ClientAuthRequiringSSLSocketFactory(SSLSocketFactory delegate) {
            this.delegate = delegate;
        }

        @Override
        public String[] getDefaultCipherSuites() {
            return delegate.getDefaultCipherSuites();
        }

        @Override
        public String[] getSupportedCipherSuites() {
            return delegate.getSupportedCipherSuites();
        }

        @Override
        public Socket createSocket(Socket socket, String s, int i, boolean b) throws IOException {
            SSLSocket sslSocket = (SSLSocket) delegate.createSocket(socket, s, i, b);
            sslSocket.setNeedClientAuth(true);
            return sslSocket;
        }

        @Override
        public Socket createSocket(String s, int i) throws IOException, UnknownHostException {
            SSLSocket sslSocket = (SSLSocket) delegate.createSocket(s, i);
            sslSocket.setNeedClientAuth(true);
            return sslSocket;
        }

        @Override
        public Socket createSocket(String s, int i, InetAddress inetAddress, int i1) throws IOException, UnknownHostException {
            SSLSocket sslSocket = (SSLSocket) delegate.createSocket(s, i, inetAddress, i1);
            sslSocket.setNeedClientAuth(true);
            return sslSocket;
        }

        @Override
        public Socket createSocket(InetAddress inetAddress, int i) throws IOException {
            SSLSocket sslSocket = (SSLSocket) delegate.createSocket(inetAddress, i);
            sslSocket.setNeedClientAuth(true);
            return sslSocket;
        }

        @Override
        public Socket createSocket(InetAddress inetAddress, int i, InetAddress inetAddress1, int i1) throws IOException {
            SSLSocket sslSocket = (SSLSocket) delegate.createSocket(inetAddress, i, inetAddress1, i1);
            sslSocket.setNeedClientAuth(true);
            return sslSocket;
        }
    }
}
