/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.http;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import com.squareup.okhttp.mockwebserver.MockResponse;
import com.squareup.okhttp.mockwebserver.MockWebServer;
import com.squareup.okhttp.mockwebserver.RecordedRequest;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.base.Charsets;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.watcher.support.http.auth.HttpAuthFactory;
import org.elasticsearch.watcher.support.http.auth.HttpAuthRegistry;
import org.elasticsearch.watcher.support.http.auth.basic.BasicAuth;
import org.elasticsearch.watcher.support.http.auth.basic.BasicAuthFactory;
import org.elasticsearch.watcher.support.secret.SecretService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.BindException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

/**
 */
public class HttpClientTest extends ElasticsearchTestCase {

    private MockWebServer webServer;
    private HttpClient httpClient;
    private HttpAuthRegistry authRegistry;
    private SecretService secretService;

    private int webPort;

    @Before
    public void init() throws Exception {
        secretService = new SecretService.PlainText();
        authRegistry = new HttpAuthRegistry(ImmutableMap.<String, HttpAuthFactory>of(BasicAuth.TYPE, new BasicAuthFactory(secretService)));
        for (webPort = 9200; webPort < 9300; webPort++) {
            try {
                webServer = new MockWebServer();
                webServer.start(webPort);
                httpClient = new HttpClient(ImmutableSettings.EMPTY, authRegistry);
                return;
            } catch (BindException be) {
                logger.warn("port [{}] was already in use trying next port", webPort);
            }
        }
        throw new ElasticsearchException("unable to find open port between 9200 and 9300");
    }

    @After
    public void after() throws Exception {
        webServer.shutdown();
    }

    @Test
    @Repeat(iterations = 10)
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

        String headerKey = randomAsciiOfLength(3);
        String headerValue = randomAsciiOfLength(3);
        requestBuilder.setHeader(headerKey, headerValue);

        requestBuilder.body(randomAsciiOfLength(5));
        HttpRequest request = requestBuilder.build();

        HttpResponse response = httpClient.execute(request);
        RecordedRequest recordedRequest = webServer.takeRequest();


        assertThat(response.status(), equalTo(responseCode));
        assertThat(response.body().toUtf8(), equalTo(body));
        assertThat(webServer.getRequestCount(), equalTo(1));
        assertThat(recordedRequest.getBody().readString(Charsets.UTF_8), equalTo(request.body()));
        assertThat(recordedRequest.getPath().split("\\?")[0], equalTo(request.path()));
        assertThat(recordedRequest.getPath().split("\\?")[1], equalTo(paramKey + "=" + paramValue));
        assertThat(recordedRequest.getHeader(headerKey), equalTo(headerValue));
    }

    @Test
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

    @Test
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

    @Test
    public void testHttps() throws Exception {
        Path resource = Paths.get(HttpClientTest.class.getResource("/org/elasticsearch/shield/keystore/testnode.jks").toURI());
        HttpClient httpClient = new HttpClient(
                ImmutableSettings.builder()
                        .put(HttpClient.SETTINGS_SSL_TRUSTSTORE, resource.toString())
                        .put(HttpClient.SETTINGS_SSL_TRUSTSTORE_PASSWORD, "testnode")
                        .build(), authRegistry);
        webServer.useHttps(httpClient.getSslSocketFactory(), false);

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

    @Test
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


}
