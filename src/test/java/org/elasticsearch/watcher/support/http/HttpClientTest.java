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
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.watcher.support.http.auth.BasicAuth;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.BindException;

import static org.hamcrest.Matchers.equalTo;

/**
 */
public class HttpClientTest extends ElasticsearchTestCase {

    private MockWebServer webServer;
    private HttpClient httpClient;

    private int webPort = 9200;

    @Before
    public void init() throws Exception {
        while (webPort < 9300) {
            try {
                webServer = new MockWebServer();
                webServer.start(webPort);
                break;
            } catch (BindException be) {
                logger.warn("port [{}] was already in use trying next port", webPort);
                ++webPort;
            }
        }
        if (webPort == 9300) {
            throw new ElasticsearchException("unable to find open port between 9200 and 9300");
        }
        httpClient = new HttpClient(ImmutableSettings.EMPTY);
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


        HttpRequest request = new HttpRequest();
        request.method(HttpMethod.POST);
        request.host("localhost");
        request.port(webPort);
        request.path("/" + randomAsciiOfLength(5));
        String paramKey;
        String paramValue;
        request.params(MapBuilder.<String, String>newMapBuilder()
                .put(paramKey = randomAsciiOfLength(3), paramValue = randomAsciiOfLength(3))
                .map());
        String headerKey;
        String headerValue;
        request.headers(MapBuilder.<String, String>newMapBuilder()
                .put(headerKey = randomAsciiOfLength(3), headerValue = randomAsciiOfLength(3))
                .map());
        request.body(randomAsciiOfLength(5));
        HttpResponse response = httpClient.execute(request);
        RecordedRequest recordedRequest = webServer.takeRequest();

        assertThat(response.status(), equalTo(responseCode));
        assertThat(new String(response.body(), Charsets.UTF_8), equalTo(body));
        assertThat(webServer.getRequestCount(), equalTo(1));
        assertThat(recordedRequest.getBody().readString(Charsets.UTF_8), equalTo(request.body()));
        assertThat(recordedRequest.getPath().split("\\?")[0], equalTo(request.path()));
        assertThat(recordedRequest.getPath().split("\\?")[1], equalTo(paramKey + "=" + paramValue));
        assertThat(recordedRequest.getHeader(headerKey), equalTo(headerValue));
    }

    @Test
    public void testBasicAuth() throws Exception {
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("body"));
        HttpRequest request = new HttpRequest();
        request.method(HttpMethod.POST);
        request.host("localhost");
        request.port(webPort);
        request.path("/test");
        request.auth(new BasicAuth("user", "pass"));
        request.body("body");
        HttpResponse response = httpClient.execute(request);
        assertThat(response.status(), equalTo(200));
        assertThat(new String(response.body(), Charsets.UTF_8), equalTo("body"));
        RecordedRequest recordedRequest = webServer.takeRequest();
        assertThat(recordedRequest.getHeader("Authorization"), equalTo("Basic dXNlcjpwYXNz"));
    }

}
