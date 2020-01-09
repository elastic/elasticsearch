/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.common.http;

import org.apache.http.conn.ConnectTimeoutException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.Network;
import org.elasticsearch.xpack.core.ssl.SSLService;

import static org.elasticsearch.xpack.watcher.common.http.HttpClientTests.mockClusterService;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class HttpConnectionTimeoutTests extends ESTestCase {
    // setting an unroutable IP to simulate a connection timeout
    private static final String UNROUTABLE_IP = "192.168.255.255";

    @Network
    public void testDefaultTimeout() throws Exception {
        Environment environment = TestEnvironment.newEnvironment(Settings.builder().put("path.home", createTempDir()).build());
        HttpClient httpClient = new HttpClient(Settings.EMPTY, new SSLService(environment), null,
            mockClusterService());

        HttpRequest request = HttpRequest.builder(UNROUTABLE_IP, 12345)
                .method(HttpMethod.POST)
                .path("/" + randomAlphaOfLength(5))
                .build();

        long start = System.nanoTime();
        try {
            httpClient.execute(request);
            fail("expected timeout exception");
        } catch (ConnectTimeoutException ete) {
            TimeValue timeout = TimeValue.timeValueNanos(System.nanoTime() - start);
            logger.info("http connection timed out after {}", timeout);
            // it's supposed to be 10, but we'll give it an error margin of 2 seconds
            assertThat(timeout.seconds(), greaterThan(8L));
            assertThat(timeout.seconds(), lessThan(12L));
            // expected
        }
    }

    @Network
    public void testDefaultTimeoutCustom() throws Exception {
        Environment environment = TestEnvironment.newEnvironment(Settings.builder().put("path.home", createTempDir()).build());
        HttpClient httpClient = new HttpClient(Settings.builder()
                .put("xpack.http.default_connection_timeout", "5s").build(), new SSLService(environment), null,
            mockClusterService());

        HttpRequest request = HttpRequest.builder(UNROUTABLE_IP, 12345)
                .method(HttpMethod.POST)
                .path("/" + randomAlphaOfLength(5))
                .build();

        long start = System.nanoTime();
        try {
            httpClient.execute(request);
            fail("expected timeout exception");
        } catch (ConnectTimeoutException ete) {
            TimeValue timeout = TimeValue.timeValueNanos(System.nanoTime() - start);
            logger.info("http connection timed out after {}", timeout);
            // it's supposed to be 7, but we'll give it an error margin of 2 seconds
            assertThat(timeout.seconds(), greaterThan(3L));
            assertThat(timeout.seconds(), lessThan(7L));
            // expected
        }
    }

    @Network
    public void testTimeoutCustomPerRequest() throws Exception {
        Environment environment = TestEnvironment.newEnvironment(Settings.builder().put("path.home", createTempDir()).build());
        HttpClient httpClient = new HttpClient(Settings.builder()
                .put("xpack.http.default_connection_timeout", "10s").build(), new SSLService(environment), null,
            mockClusterService());

        HttpRequest request = HttpRequest.builder(UNROUTABLE_IP, 12345)
                .connectionTimeout(TimeValue.timeValueSeconds(5))
                .method(HttpMethod.POST)
                .path("/" + randomAlphaOfLength(5))
                .build();

        long start = System.nanoTime();
        try {
            httpClient.execute(request);
            fail("expected timeout exception");
        } catch (ConnectTimeoutException ete) {
            TimeValue timeout = TimeValue.timeValueNanos(System.nanoTime() - start);
            logger.info("http connection timed out after {}", timeout);
            // it's supposed to be 7, but we'll give it an error margin of 2 seconds
            assertThat(timeout.seconds(), greaterThan(3L));
            assertThat(timeout.seconds(), lessThan(7L));
            // expected
        }
    }
}
