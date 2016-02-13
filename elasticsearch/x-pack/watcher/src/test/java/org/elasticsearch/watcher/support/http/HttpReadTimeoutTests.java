/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.http;

import com.squareup.okhttp.mockwebserver.Dispatcher;
import com.squareup.okhttp.mockwebserver.MockResponse;
import com.squareup.okhttp.mockwebserver.MockWebServer;
import com.squareup.okhttp.mockwebserver.RecordedRequest;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.support.http.auth.HttpAuthRegistry;
import org.junit.After;
import org.junit.Before;

import java.net.BindException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.mockito.Mockito.mock;

/**
 */
public class HttpReadTimeoutTests extends ESTestCase {

    private MockWebServer webServer;
    private int webPort;

    @Before
    public void init() throws Exception {
        for (webPort = 9200; webPort < 9300; webPort++) {
            try {
                webServer = new MockWebServer();
                webServer.start(webPort);
                return;
            } catch (BindException be) {
                logger.warn("port [{}] was already in use trying next port", webPort);
            }
        }
        throw new ElasticsearchException("unable to find open port between 9200 and 9300");
    }

    @After
    public void cleanup() throws Exception {
        webServer.shutdown();

    }

    public void testDefaultTimeout() throws Exception {
        Environment environment = new Environment(Settings.builder().put("path.home", createTempDir()).build());
        HttpClient httpClient = new HttpClient(Settings.EMPTY, mock(HttpAuthRegistry.class), environment).start();

        // we're not going to enqueue an response... so the server will just hang

        HttpRequest request = HttpRequest.builder("localhost", webPort)
                .method(HttpMethod.POST)
                .path("/" + randomAsciiOfLength(5))
                .build();

        long start = System.nanoTime();
        try {
            httpClient.execute(request);
            fail("expected read timeout after 10 seconds (default)");
        } catch (ElasticsearchTimeoutException ete) {
            // expected

            TimeValue timeout = TimeValue.timeValueNanos(System.nanoTime() - start);
            logger.info("http connection timed out after {}", timeout.format());

            // it's supposed to be 10, but we'll give it an error margin of 2 seconds
            assertThat(timeout.seconds(), greaterThan(8L));
            assertThat(timeout.seconds(), lessThan(12L));

            // lets enqueue a response to relese the server.
            webServer.enqueue(new MockResponse());
        }
    }

    public void testDefaultTimeoutCustom() throws Exception {
        Environment environment = new Environment(Settings.builder().put("path.home", createTempDir()).build());

        HttpClient httpClient = new HttpClient(Settings.builder()
                .put("watcher.http.default_read_timeout", "3s")
                .build()
                , mock(HttpAuthRegistry.class), environment).start();

        final CountDownLatch latch = new CountDownLatch(1);
        webServer.setDispatcher(new Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request) throws InterruptedException {
                Thread.sleep(5000);
                latch.countDown();
                return new MockResponse().setStatus("200");
            }
        });

        HttpRequest request = HttpRequest.builder("localhost", webPort)
                .method(HttpMethod.POST)
                .path("/" + randomAsciiOfLength(5))
                .build();

        long start = System.nanoTime();
        try {
            httpClient.execute(request);
            fail("expected read timeout after 5 seconds (default)");
        } catch (ElasticsearchTimeoutException ete) {
            // expected

            TimeValue timeout = TimeValue.timeValueNanos(System.nanoTime() - start);
            logger.info("http connection timed out after {}", timeout.format());

            // it's supposed to be 3, but we'll give it an error margin of 2 seconds
            assertThat(timeout.seconds(), greaterThan(1L));
            assertThat(timeout.seconds(), lessThan(5L));
        }

        if (!latch.await(10, TimeUnit.SECONDS)) {
            // should never happen
            fail("waited too long for the response to be returned");
        }
    }

    public void testTimeoutCustomPerRequest() throws Exception {
        Environment environment = new Environment(Settings.builder().put("path.home", createTempDir()).build());

        HttpClient httpClient = new HttpClient(Settings.builder()
                .put("watcher.http.default_read_timeout", "10s")
                .build()
                , mock(HttpAuthRegistry.class), environment).start();

        final CountDownLatch latch = new CountDownLatch(1);
        webServer.setDispatcher(new Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request) throws InterruptedException {
                Thread.sleep(5000);
                latch.countDown();
                return new MockResponse().setStatus("200");
            }
        });

        HttpRequest request = HttpRequest.builder("localhost", webPort)
                .readTimeout(TimeValue.timeValueSeconds(5))
                .method(HttpMethod.POST)
                .path("/" + randomAsciiOfLength(5))
                .build();

        long start = System.nanoTime();
        try {
            httpClient.execute(request);
            fail("expected read timeout after 5 seconds (default)");
        } catch (ElasticsearchTimeoutException ete) {
            // expected

            TimeValue timeout = TimeValue.timeValueNanos(System.nanoTime() - start);
            logger.info("http connection timed out after {}", timeout.format());

            // it's supposed to be 5, but we'll give it an error margin of 2 seconds
            assertThat(timeout.seconds(), greaterThan(3L));
            assertThat(timeout.seconds(), lessThan(7L));
        }

        if (!latch.await(7, TimeUnit.SECONDS)) {
            // should never happen
            fail("waited too long for the response to be returned");
        }
    }
}
