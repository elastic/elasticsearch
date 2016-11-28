/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.common.http;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import com.squareup.okhttp.mockwebserver.MockResponse;
import com.squareup.okhttp.mockwebserver.MockWebServer;
import com.squareup.okhttp.mockwebserver.RecordedRequest;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.Network;
import org.elasticsearch.xpack.common.http.auth.HttpAuthRegistry;
import org.elasticsearch.xpack.common.http.auth.basic.BasicAuth;
import org.elasticsearch.xpack.common.http.auth.basic.BasicAuthFactory;
import org.elasticsearch.xpack.ssl.SSLService;
import org.elasticsearch.xpack.ssl.VerificationMode;
import org.junit.After;
import org.junit.Before;

import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class HttpClientTests extends ESTestCase {

    private MockWebServer webServer;
    private HttpClient httpClient;
    private HttpAuthRegistry authRegistry;
    private Environment environment = new Environment(Settings.builder().put("path.home", createTempDir()).build());

    @Before
    public void init() throws Exception {
        authRegistry = new HttpAuthRegistry(singletonMap(BasicAuth.TYPE, new BasicAuthFactory(null)));
        webServer = startWebServer();
        httpClient = new HttpClient(Settings.EMPTY, authRegistry, new SSLService(environment.settings(), environment));
    }

    @After
    public void after() throws Exception {
        webServer.shutdown();
    }

    public void testBasics() throws Exception {
        int responseCode = randomIntBetween(200, 203);
        String body = randomAsciiOfLengthBetween(2, 8096);
        webServer.enqueue(new MockResponse().setResponseCode(responseCode).setBody(body));


        HttpRequest.Builder requestBuilder = HttpRequest.builder("localhost", webServer.getPort())
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
        assertThat(response.body().utf8ToString(), equalTo(body));
        assertThat(webServer.getRequestCount(), equalTo(1));
        assertThat(recordedRequest.getBody().readString(StandardCharsets.UTF_8), equalTo(request.body()));
        assertThat(recordedRequest.getPath().split("\\?")[0], equalTo(request.path()));
        assertThat(recordedRequest.getPath().split("\\?")[1], equalTo(paramKey + "=" + paramValue));
        assertThat(recordedRequest.getHeader(headerKey), equalTo(headerValue));
    }

    public void testNoQueryString() throws Exception {
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("body"));
        HttpRequest.Builder requestBuilder = HttpRequest.builder("localhost", webServer.getPort())
                .method(HttpMethod.GET)
                .path("/test");

        HttpResponse response = httpClient.execute(requestBuilder.build());
        assertThat(response.status(), equalTo(200));
        assertThat(response.body().utf8ToString(), equalTo("body"));

        RecordedRequest recordedRequest = webServer.takeRequest();
        assertThat(recordedRequest.getPath(), equalTo("/test"));
        assertThat(recordedRequest.getBody().readUtf8Line(), nullValue());
    }

    public void testUrlEncodingWithQueryStrings() throws Exception{
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("body"));
        HttpRequest.Builder requestBuilder = HttpRequest.builder("localhost", webServer.getPort())
                .method(HttpMethod.GET)
                .path("/test")
                .setParam("key", "value 123:123");

        HttpResponse response = httpClient.execute(requestBuilder.build());
        assertThat(response.status(), equalTo(200));
        assertThat(response.body().utf8ToString(), equalTo("body"));

        RecordedRequest recordedRequest = webServer.takeRequest();
        assertThat(recordedRequest.getPath(), equalTo("/test?key=value+123%3A123"));
        assertThat(recordedRequest.getBody().readUtf8Line(), nullValue());
    }

    public void testBasicAuth() throws Exception {
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("body"));
        HttpRequest.Builder request = HttpRequest.builder("localhost", webServer.getPort())
                .method(HttpMethod.POST)
                .path("/test")
                .auth(new BasicAuth("user", "pass".toCharArray()))
                .body("body");
        HttpResponse response = httpClient.execute(request.build());
        assertThat(response.status(), equalTo(200));
        assertThat(response.body().utf8ToString(), equalTo("body"));
        RecordedRequest recordedRequest = webServer.takeRequest();
        assertThat(recordedRequest.getPath(), equalTo("/test"));
        assertThat(recordedRequest.getHeader("Authorization"), equalTo("Basic dXNlcjpwYXNz"));
    }

    public void testNoPathSpecified() throws Exception {
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("doesntmatter"));
        HttpRequest.Builder request = HttpRequest.builder("localhost", webServer.getPort()).method(HttpMethod.GET);
        httpClient.execute(request.build());
        RecordedRequest recordedRequest = webServer.takeRequest();
        assertThat(recordedRequest.getPath(), equalTo("/"));
    }

    public void testHttps() throws Exception {
        Path resource = getDataPath("/org/elasticsearch/xpack/security/keystore/truststore-testnode-only.jks");

        Settings settings;
        if (randomBoolean()) {
            settings = Settings.builder()
                    .put("xpack.http.ssl.truststore.path", resource.toString())
                    .put("xpack.http.ssl.truststore.password", "truststore-testnode-only")
                    .build();
        } else {
            settings = Settings.builder()
                    .put("xpack.ssl.truststore.path", resource.toString())
                    .put("xpack.ssl.truststore.password", "truststore-testnode-only")
                    .build();
        }
        HttpClient httpClient = new HttpClient(settings, authRegistry, new SSLService(settings, environment));

        // We can't use the client created above for the server since it is only a truststore
        Settings settings2 = Settings.builder()
                .put("xpack.ssl.keystore.path", getDataPath("/org/elasticsearch/xpack/security/keystore/testnode.jks"))
                .put("xpack.ssl.keystore.password", "testnode")
                .build();
        webServer.useHttps(new SSLService(settings2, environment).sslSocketFactory(Settings.EMPTY), false);

        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("body"));
        HttpRequest.Builder request = HttpRequest.builder("localhost", webServer.getPort())
                .scheme(Scheme.HTTPS)
                .path("/test")
                .body("body");
        HttpResponse response = httpClient.execute(request.build());
        assertThat(response.status(), equalTo(200));
        assertThat(response.body().utf8ToString(), equalTo("body"));
        RecordedRequest recordedRequest = webServer.takeRequest();
        assertThat(recordedRequest.getPath(), equalTo("/test"));
        assertThat(recordedRequest.getBody().readUtf8Line(), equalTo("body"));
    }

    public void testHttpsDisableHostnameVerification() throws Exception {
        Path resource = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode-no-subjaltname.jks");

        Settings settings;
        if (randomBoolean()) {
            settings = Settings.builder()
                    .put("xpack.http.ssl.truststore.path", resource.toString())
                    .put("xpack.http.ssl.truststore.password", "testnode-no-subjaltname")
                    .put("xpack.http.ssl.verification_mode", randomFrom(VerificationMode.NONE, VerificationMode.CERTIFICATE))
                    .build();
        } else {
            settings = Settings.builder()
                    .put("xpack.ssl.truststore.path", resource.toString())
                    .put("xpack.ssl.truststore.password", "testnode-no-subjaltname")
                    .put("xpack.ssl.verification_mode", randomFrom(VerificationMode.NONE, VerificationMode.CERTIFICATE))
                    .build();
        }
        HttpClient httpClient = new HttpClient(settings, authRegistry, new SSLService(settings, environment));

        // We can't use the client created above for the server since it only defines a truststore
        Settings settings2 = Settings.builder()
                .put("xpack.ssl.keystore.path",
                        getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode-no-subjaltname.jks"))
                .put("xpack.ssl.keystore.password", "testnode-no-subjaltname")
                .build();
        webServer.useHttps(new SSLService(settings2, environment).sslSocketFactory(Settings.EMPTY), false);

        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("body"));
        HttpRequest.Builder request = HttpRequest.builder("localhost", webServer.getPort())
                .scheme(Scheme.HTTPS)
                .path("/test")
                .body("body");
        HttpResponse response = httpClient.execute(request.build());
        assertThat(response.status(), equalTo(200));
        assertThat(response.body().utf8ToString(), equalTo("body"));
        RecordedRequest recordedRequest = webServer.takeRequest();
        assertThat(recordedRequest.getPath(), equalTo("/test"));
        assertThat(recordedRequest.getBody().readUtf8Line(), equalTo("body"));
    }

    public void testHttpsClientAuth() throws Exception {
        Path resource = getDataPath("/org/elasticsearch/xpack/security/keystore/testnode.jks");
        Settings settings;
        if (randomBoolean()) {
            settings = Settings.builder()
                    .put("xpack.http.ssl.keystore.path", resource.toString())
                    .put("xpack.http.ssl.keystore.password", "testnode")
                    .build();
        } else {
            settings = Settings.builder()
                    .put("xpack.ssl.keystore.path", resource.toString())
                    .put("xpack.ssl.keystore.password", "testnode")
                    .build();
        }

        final SSLService sslService = new SSLService(settings, environment);
        HttpClient httpClient = new HttpClient(settings, authRegistry, sslService);
        webServer.useHttps(
                new ClientAuthRequiringSSLSocketFactory(sslService.sslSocketFactory(settings.getByPrefix("xpack.http.ssl."))), false);

        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("body"));
        HttpRequest.Builder request = HttpRequest.builder("localhost", webServer.getPort())
                .scheme(Scheme.HTTPS)
                .path("/test")
                .body("body");
        HttpResponse response = httpClient.execute(request.build());
        assertThat(response.status(), equalTo(200));
        assertThat(response.body().utf8ToString(), equalTo("body"));
        RecordedRequest recordedRequest = webServer.takeRequest();
        assertThat(recordedRequest.getPath(), equalTo("/test"));
        assertThat(recordedRequest.getBody().readUtf8Line(), equalTo("body"));
    }

    public void testHttpResponseWithAnyStatusCodeCanReturnBody() throws Exception {
        int statusCode = randomFrom(200, 201, 400, 401, 403, 404, 405, 409, 413, 429, 500, 503);
        String body = RandomStrings.randomAsciiOfLength(random(), 100);
        boolean hasBody = usually();
        MockResponse mockResponse = new MockResponse().setResponseCode(statusCode);
        if (hasBody) {
            mockResponse.setBody(body);
        }
        webServer.enqueue(mockResponse);
        HttpRequest.Builder request = HttpRequest.builder("localhost", webServer.getPort())
                .method(HttpMethod.POST)
                .path("/test")
                .auth(new BasicAuth("user", "pass".toCharArray()))
                .body("body")
                .connectionTimeout(TimeValue.timeValueMillis(500))
                .readTimeout(TimeValue.timeValueMillis(500));
        HttpResponse response = httpClient.execute(request.build());
        assertThat(response.status(), equalTo(statusCode));
        assertThat(response.hasContent(), is(hasBody));
        if (hasBody) {
            assertThat(response.body().utf8ToString(), is(body));
        }
    }

    @Network
    public void testHttpsWithoutTruststore() throws Exception {
        HttpClient httpClient = new HttpClient(Settings.EMPTY, authRegistry, new SSLService(Settings.EMPTY, environment));

        // Known server with a valid cert from a commercial CA
        HttpRequest.Builder request = HttpRequest.builder("www.elastic.co", 443).scheme(Scheme.HTTPS);
        HttpResponse response = httpClient.execute(request.build());
        assertThat(response.status(), equalTo(200));
        assertThat(response.hasContent(), is(true));
        assertThat(response.body(), notNullValue());
    }

    public void testThatProxyCanBeConfigured() throws Exception {
        // this test fakes a proxy server that sends a response instead of forwarding it to the mock web server
        MockWebServer proxyServer = startWebServer();
        proxyServer.enqueue(new MockResponse().setResponseCode(200).setBody("fullProxiedContent"));

        try {
            Settings settings = Settings.builder()
                    .put(HttpClient.SETTINGS_PROXY_HOST, "localhost")
                    .put(HttpClient.SETTINGS_PROXY_PORT, proxyServer.getPort())
                    .build();
            HttpClient httpClient = new HttpClient(settings, authRegistry, new SSLService(settings, environment));

            HttpRequest.Builder requestBuilder = HttpRequest.builder("localhost", webServer.getPort())
                    .method(HttpMethod.GET)
                    .path("/");

            HttpResponse response = httpClient.execute(requestBuilder.build());
            assertThat(response.status(), equalTo(200));
            assertThat(response.body().utf8ToString(), equalTo("fullProxiedContent"));

            // ensure we hit the proxyServer and not the webserver
            assertThat(webServer.getRequestCount(), equalTo(0));
            assertThat(proxyServer.getRequestCount(), equalTo(1));
        } finally {
            proxyServer.shutdown();
        }
    }

    public void testThatProxyCanBeOverriddenByRequest() throws Exception {
        // this test fakes a proxy server that sends a response instead of forwarding it to the mock web server
        MockWebServer proxyServer = startWebServer();
        proxyServer.enqueue(new MockResponse().setResponseCode(200).setBody("fullProxiedContent"));

        try {
            Settings settings = Settings.builder()
                    .put(HttpClient.SETTINGS_PROXY_HOST, "localhost")
                    .put(HttpClient.SETTINGS_PROXY_PORT, proxyServer.getPort() + 1)
                    .build();
            HttpClient httpClient = new HttpClient(settings, authRegistry, new SSLService(settings, environment));

            HttpRequest.Builder requestBuilder = HttpRequest.builder("localhost", webServer.getPort())
                    .method(HttpMethod.GET)
                    .proxy(new HttpProxy("localhost", proxyServer.getPort()))
                    .path("/");

            HttpResponse response = httpClient.execute(requestBuilder.build());
            assertThat(response.status(), equalTo(200));
            assertThat(response.body().utf8ToString(), equalTo("fullProxiedContent"));

            // ensure we hit the proxyServer and not the webserver
            assertThat(webServer.getRequestCount(), equalTo(0));
            assertThat(proxyServer.getRequestCount(), equalTo(1));
        } finally {
            proxyServer.shutdown();
        }
    }

    public void testThatUrlPathIsNotEncoded() throws Exception {
        // %2F is a slash that needs to be encoded to not be misinterpreted as a path
        String path = "/<logstash-{now%2Fd}>/_search";
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("foo"));
        HttpRequest request = HttpRequest.builder("localhost", webServer.getPort()).path(path).build();
        httpClient.execute(request);

        assertThat(webServer.getRequestCount(), is(1));

        RecordedRequest recordedRequest = webServer.takeRequest();
        // under no circumstances have a double encode of %2F => %25 (percent sign)
        assertThat(recordedRequest.getPath(), not(containsString("%25")));
        assertThat(recordedRequest.getPath(), equalTo(path));
    }

    public void testThatHttpClientFailsOnNonHttpResponse() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicReference<Exception> hasExceptionHappened = new AtomicReference();
        try (ServerSocket serverSocket = new ServerSocket(0, 50, InetAddress.getByName("localhost"))) {
            executor.execute(() -> {
                try (Socket socket = serverSocket.accept()) {
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                    in.readLine();
                    socket.getOutputStream().write("This is not a HTTP response".getBytes(StandardCharsets.UTF_8));
                    socket.getOutputStream().flush();
                } catch (Exception e) {
                    hasExceptionHappened.set(e);
                    logger.error((Supplier<?>) () -> new ParameterizedMessage("Error in writing non HTTP response"), e);
                }
            });
            HttpRequest request = HttpRequest.builder("localhost", serverSocket.getLocalPort()).path("/").build();
            IOException e = expectThrows(IOException.class, () -> httpClient.execute(request));
            assertThat(e.getMessage(), is("Not a valid HTTP response, no status code in response"));
            assertThat("A server side exception occured, but shouldnt", hasExceptionHappened.get(), is(nullValue()));
        } finally {
            terminate(executor);
        }
    }

    private MockWebServer startWebServer() throws IOException {
        MockWebServer mockWebServer = new MockWebServer();
        mockWebServer.setProtocolNegotiationEnabled(false);
        mockWebServer.start();
        return mockWebServer;
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
