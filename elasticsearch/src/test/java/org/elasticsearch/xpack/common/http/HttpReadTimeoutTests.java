/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.common.http;

import com.squareup.okhttp.mockwebserver.Dispatcher;
import com.squareup.okhttp.mockwebserver.MockResponse;
import com.squareup.okhttp.mockwebserver.MockWebServer;
import com.squareup.okhttp.mockwebserver.RecordedRequest;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.common.http.auth.HttpAuthRegistry;
import org.elasticsearch.xpack.ssl.SSLService;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.mockito.Mockito.mock;

public class HttpReadTimeoutTests extends ESTestCase {

    private MockWebServer webServer;

    @Before
    public void init() throws Exception {
        webServer = new MockWebServer();
        webServer.start();
    }

    @After
    public void cleanup() throws Exception {
        webServer.shutdown();
    }

    public void testDefaultTimeout() throws Exception {
        Environment environment = new Environment(Settings.builder().put("path.home", createTempDir()).build());
        HttpClient httpClient = new HttpClient(Settings.EMPTY, mock(HttpAuthRegistry.class),
                new SSLService(environment.settings(), environment));

        // we're not going to enqueue an response... so the server will just hang

        HttpRequest request = HttpRequest.builder("localhost", webServer.getPort())
                .method(HttpMethod.POST)
                .path("/" + randomAsciiOfLength(5))
                .build();

        long start = System.nanoTime();
        expectThrows(ElasticsearchTimeoutException.class, () ->  httpClient.execute(request));
        TimeValue timeout = TimeValue.timeValueNanos(System.nanoTime() - start);
        logger.info("http connection timed out after {}", timeout.format());

        // it's supposed to be 10, but we'll give it an error margin of 2 seconds
        assertThat(timeout.seconds(), greaterThan(8L));
        assertThat(timeout.seconds(), lessThan(12L));

        // lets enqueue a response to relese the server.
        webServer.enqueue(new MockResponse());
    }

    public void testDefaultTimeoutCustom() throws Exception {
        Environment environment = new Environment(Settings.builder().put("path.home", createTempDir()).build());

        HttpClient httpClient = new HttpClient(Settings.builder()
                .put("xpack.http.default_read_timeout", "3s").build()
                , mock(HttpAuthRegistry.class), new SSLService(environment.settings(), environment));

        final String path = '/' + randomAsciiOfLength(5);
        final CountDownLatch latch = new CountDownLatch(1);
        final TimeValue sleepTime = TimeValue.timeValueSeconds(10);
        webServer.setDispatcher(new CountDownLatchDispatcher(path, latch, sleepTime));

        HttpRequest request = HttpRequest.builder("localhost", webServer.getPort())
                .method(HttpMethod.POST)
                .path(path)
                .build();

        long start = System.nanoTime();
        expectThrows(ElasticsearchTimeoutException.class, () ->  httpClient.execute(request));
        TimeValue timeout = TimeValue.timeValueNanos(System.nanoTime() - start);
        logger.info("http connection timed out after {}", timeout.format());

        // it's supposed to be 3, but we'll give it an error margin of 2 seconds
        assertThat(timeout.seconds(), greaterThan(1L));
        assertThat(timeout.seconds(), lessThan(5L));

        if (!latch.await(sleepTime.seconds(), TimeUnit.SECONDS)) {
            // should never happen
            fail("waited too long for the response to be returned");
        }
    }

    public void testTimeoutCustomPerRequest() throws Exception {
        Environment environment = new Environment(Settings.builder().put("path.home", createTempDir()).build());

        HttpClient httpClient = new HttpClient(Settings.builder()
                .put("xpack.http.default_read_timeout", "10s").build()
                , mock(HttpAuthRegistry.class), new SSLService(environment.settings(), environment));

        final String path = '/' + randomAsciiOfLength(5);
        final CountDownLatch latch = new CountDownLatch(1);
        final TimeValue sleepTime = TimeValue.timeValueSeconds(10);
        webServer.setDispatcher(new CountDownLatchDispatcher(path, latch, sleepTime));

        HttpRequest request = HttpRequest.builder("localhost", webServer.getPort())
                .readTimeout(TimeValue.timeValueSeconds(5))
                .method(HttpMethod.POST)
                .path(path)
                .build();

        long start = System.nanoTime();
        expectThrows(ElasticsearchTimeoutException.class, () ->  httpClient.execute(request));
        TimeValue timeout = TimeValue.timeValueNanos(System.nanoTime() - start);
        logger.info("http connection timed out after {}", timeout.format());

        // it's supposed to be 5, but we'll give it an error margin of 2 seconds
        assertThat(timeout.seconds(), greaterThan(3L));
        assertThat(timeout.seconds(), lessThan(7L));

        if (!latch.await(sleepTime.seconds(), TimeUnit.SECONDS)) {
            // should never happen
            fail("waited too long for the response to be returned");
        }
    }

    private class CountDownLatchDispatcher extends Dispatcher {

        private final String path;
        private final CountDownLatch latch;
        private TimeValue sleepTime;

        public CountDownLatchDispatcher(String path, CountDownLatch latch, TimeValue sleepTime) {
            this.path = path;
            this.latch = latch;
            this.sleepTime = sleepTime;
        }

        @Override
        public MockResponse dispatch(RecordedRequest request) throws InterruptedException {
            if (path.equals(request.getPath())) {
                Thread.sleep(sleepTime.millis());
                latch.countDown();
            }
            return new MockResponse().setStatus("200");
        }
    }
}
