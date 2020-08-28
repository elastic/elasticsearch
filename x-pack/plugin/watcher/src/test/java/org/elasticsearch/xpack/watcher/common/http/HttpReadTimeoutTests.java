/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.common.http;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.junit.After;
import org.junit.Before;

import java.net.SocketTimeoutException;

import static org.elasticsearch.xpack.watcher.common.http.HttpClientTests.mockClusterService;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class HttpReadTimeoutTests extends ESTestCase {

    private MockWebServer webServer = new MockWebServer();

    @Before
    public void init() throws Exception {
        webServer.start();
        webServer.enqueue(new MockResponse().setBeforeReplyDelay(TimeValue.timeValueSeconds(20)));
    }

    @After
    public void cleanup() throws Exception {
        webServer.close();
    }

    public void testDefaultTimeout() throws Exception {
        Environment environment = TestEnvironment.newEnvironment(Settings.builder().put("path.home", createTempDir()).build());
        HttpRequest request = HttpRequest.builder("localhost", webServer.getPort())
                .method(HttpMethod.POST)
                .path("/")
                .build();

        try (HttpClient httpClient = new HttpClient(Settings.EMPTY, new SSLService(environment),
            null, mockClusterService())) {
            long start = System.nanoTime();

            expectThrows(SocketTimeoutException.class, () ->  httpClient.execute(request));
            TimeValue timeout = TimeValue.timeValueNanos(System.nanoTime() - start);
            logger.info("http connection timed out after {}", timeout);

            // it's supposed to be 10, but we'll give it an error margin of 2 seconds
            assertThat(timeout.seconds(), greaterThan(8L));
            assertThat(timeout.seconds(), lessThan(12L));
        }
    }

    public void testDefaultTimeoutCustom() throws Exception {
        Environment environment = TestEnvironment.newEnvironment(Settings.builder().put("path.home", createTempDir()).build());

        HttpRequest request = HttpRequest.builder("localhost", webServer.getPort())
                .method(HttpMethod.POST)
                .path("/")
                .build();

        try (HttpClient httpClient = new HttpClient(Settings.builder()
            .put("xpack.http.default_read_timeout", "3s").build(), new SSLService(environment),
            null, mockClusterService())) {

            long start = System.nanoTime();
            expectThrows(SocketTimeoutException.class, () ->  httpClient.execute(request));
            TimeValue timeout = TimeValue.timeValueNanos(System.nanoTime() - start);
            logger.info("http connection timed out after {}", timeout);

            // it's supposed to be 3, but we'll give it an error margin of 2 seconds
            assertThat(timeout.seconds(), greaterThan(1L));
            assertThat(timeout.seconds(), lessThan(5L));
        }
    }

    public void testTimeoutCustomPerRequest() throws Exception {
        Environment environment = TestEnvironment.newEnvironment(Settings.builder().put("path.home", createTempDir()).build());

        HttpRequest request = HttpRequest.builder("localhost", webServer.getPort())
                .readTimeout(TimeValue.timeValueSeconds(3))
                .method(HttpMethod.POST)
                .path("/")
                .build();

        try (HttpClient httpClient = new HttpClient(Settings.builder()
            .put("xpack.http.default_read_timeout", "10s").build(), new SSLService(environment),
            null, mockClusterService())) {

            long start = System.nanoTime();
            expectThrows(SocketTimeoutException.class, () ->  httpClient.execute(request));
            TimeValue timeout = TimeValue.timeValueNanos(System.nanoTime() - start);
            logger.info("http connection timed out after {}", timeout);

            // it's supposed to be 3, but we'll give it an error margin of 2 seconds
            assertThat(timeout.seconds(), greaterThan(1L));
            assertThat(timeout.seconds(), lessThan(5L));
        }
    }
}
