/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.ThreadPool;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_CLIENT_STATS_ENABLED;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_CLIENT_STATS_MAX_CLOSED_CHANNEL_AGE;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_CLIENT_STATS_MAX_CLOSED_CHANNEL_COUNT;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class HttpClientStatsTrackerTests extends ESTestCase {

    public void testCollectsNoStatsIfDisabled() {
        final Settings settings = Settings.builder().put(SETTING_HTTP_CLIENT_STATS_ENABLED.getKey(), false).build();
        final HttpClientStatsTracker httpClientStatsTracker = new HttpClientStatsTracker(
            settings,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            new FakeTimeThreadPool()
        );

        final HttpChannel httpChannel = randomHttpChannel();
        httpClientStatsTracker.addClientStats(httpChannel);
        assertThat(httpClientStatsTracker.getClientStats(), empty());

        httpClientStatsTracker.updateClientStats(randomHttpRequest(), httpChannel);
        assertThat(httpClientStatsTracker.getClientStats(), empty());

        httpChannel.close();
        assertThat(httpClientStatsTracker.getClientStats(), empty());

        httpClientStatsTracker.updateClientStats(randomHttpRequest(), randomHttpChannel());
        assertThat(httpClientStatsTracker.getClientStats(), empty());
    }

    public void testStatsCollection() {
        final Settings.Builder settings = Settings.builder();
        if (randomBoolean()) {
            settings.put(SETTING_HTTP_CLIENT_STATS_ENABLED.getKey(), true);
        }
        final long maxAgeMillis;
        if (randomBoolean()) {
            maxAgeMillis = randomLongBetween(1L, 1000000L);
            settings.put(SETTING_HTTP_CLIENT_STATS_MAX_CLOSED_CHANNEL_AGE.getKey(), TimeValue.timeValueMillis(maxAgeMillis));
        } else {
            maxAgeMillis = SETTING_HTTP_CLIENT_STATS_MAX_CLOSED_CHANNEL_AGE.get(Settings.EMPTY).millis();
        }

        final FakeTimeThreadPool threadPool = new FakeTimeThreadPool();

        final HttpClientStatsTracker httpClientStatsTracker = new HttpClientStatsTracker(
            settings.build(),
            new ClusterSettings(settings.build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool
        );

        threadPool.setRandomTime();
        final long openTimeMillis = threadPool.absoluteTimeInMillis();
        long requestLength = 0L;
        final HttpChannel httpChannel = randomHttpChannel();
        httpClientStatsTracker.addClientStats(httpChannel);
        {
            final List<HttpStats.ClientStats> clientsStats = httpClientStatsTracker.getClientStats();
            assertThat(clientsStats, hasSize(1));
            final HttpStats.ClientStats clientStats = clientsStats.get(0);
            assertThat(clientStats.remoteAddress(), equalTo(NetworkAddress.format(httpChannel.getRemoteAddress())));
            assertNull(clientStats.lastUri());
            assertThat(clientStats.requestCount(), equalTo(0L));
            assertThat(clientStats.requestSizeBytes(), equalTo(requestLength));
            assertThat(clientStats.closedTimeMillis(), equalTo(-1L));
            assertThat(clientStats.openedTimeMillis(), equalTo(openTimeMillis));

            assertNull(clientStats.agent());
            assertNull(clientStats.forwardedFor());
            assertNull(clientStats.opaqueId());
        }

        threadPool.setRandomTime();
        final HttpRequest httpRequest1 = randomHttpRequest();
        httpClientStatsTracker.updateClientStats(httpRequest1, httpChannel);
        {
            final List<HttpStats.ClientStats> clientsStats = httpClientStatsTracker.getClientStats();
            assertThat(clientsStats, hasSize(1));

            final HttpStats.ClientStats clientStats = clientsStats.get(0);
            assertThat(clientStats.remoteAddress(), equalTo(NetworkAddress.format(httpChannel.getRemoteAddress())));
            assertThat(clientStats.lastUri(), equalTo(httpRequest1.uri()));
            assertThat(clientStats.requestCount(), equalTo(1L));
            requestLength += httpRequest1.content().length();
            assertThat(clientStats.requestSizeBytes(), equalTo(requestLength));
            assertThat(clientStats.closedTimeMillis(), equalTo(-1L));
            assertThat(clientStats.openedTimeMillis(), equalTo(openTimeMillis));

            final Map<String, String> relevantHeaders = getRelevantHeaders(httpRequest1);
            assertThat(
                clientStats.agent(),
                equalTo(
                    Optional.empty()
                        .or(() -> Optional.ofNullable(relevantHeaders.get("x-elastic-product-origin")))
                        .or(() -> Optional.ofNullable(relevantHeaders.get("user-agent")))
                        .orElse(null)
                )
            );
            assertThat(clientStats.forwardedFor(), equalTo(relevantHeaders.get("x-forwarded-for")));
            assertThat(clientStats.opaqueId(), equalTo(relevantHeaders.get("x-opaque-id")));
        }

        threadPool.setRandomTime();
        final HttpRequest httpRequest2 = randomHttpRequest();
        httpClientStatsTracker.updateClientStats(httpRequest2, httpChannel);
        {
            final List<HttpStats.ClientStats> clientsStats = httpClientStatsTracker.getClientStats();
            assertThat(clientsStats, hasSize(1));

            final HttpStats.ClientStats clientStats = clientsStats.get(0);
            assertThat(clientStats.remoteAddress(), equalTo(NetworkAddress.format(httpChannel.getRemoteAddress())));
            assertThat(clientStats.lastUri(), equalTo(httpRequest2.uri()));
            assertThat(clientStats.requestCount(), equalTo(2L));
            requestLength += httpRequest2.content().length();
            assertThat(clientStats.requestSizeBytes(), equalTo(requestLength));
            assertThat(clientStats.closedTimeMillis(), equalTo(-1L));
            assertThat(clientStats.openedTimeMillis(), equalTo(openTimeMillis));

            final Map<String, String> relevantHeaders1 = getRelevantHeaders(httpRequest1);
            final Map<String, String> relevantHeaders2 = getRelevantHeaders(httpRequest2);
            assertThat(
                clientStats.agent(),
                equalTo(
                    Optional.empty()
                        .or(() -> Optional.ofNullable(relevantHeaders1.get("x-elastic-product-origin")))
                        .or(() -> Optional.ofNullable(relevantHeaders1.get("user-agent")))
                        .or(() -> Optional.ofNullable(relevantHeaders2.get("x-elastic-product-origin")))
                        .or(() -> Optional.ofNullable(relevantHeaders2.get("user-agent")))
                        .orElse(null)
                )
            );
            assertThat(
                clientStats.forwardedFor(),
                equalTo(
                    Optional.empty()
                        .or(() -> Optional.ofNullable(relevantHeaders1.get("x-forwarded-for")))
                        .or(() -> Optional.ofNullable(relevantHeaders2.get("x-forwarded-for")))
                        .orElse(null)
                )
            );
            assertThat(
                clientStats.opaqueId(),
                equalTo(
                    Optional.empty()
                        .or(() -> Optional.ofNullable(relevantHeaders1.get("x-opaque-id")))
                        .or(() -> Optional.ofNullable(relevantHeaders2.get("x-opaque-id")))
                        .orElse(null)
                )
            );
        }

        threadPool.setRandomTime();
        final long closeTimeMillis = threadPool.absoluteTimeInMillis();
        httpChannel.close();
        {
            final List<HttpStats.ClientStats> clientsStats = httpClientStatsTracker.getClientStats();
            assertThat(clientsStats, hasSize(1));

            final HttpStats.ClientStats clientStats = clientsStats.get(0);
            assertThat(clientStats.remoteAddress(), equalTo(NetworkAddress.format(httpChannel.getRemoteAddress())));
            assertThat(clientStats.lastUri(), equalTo(httpRequest2.uri()));
            assertThat(clientStats.requestCount(), equalTo(2L));
            assertThat(clientStats.requestSizeBytes(), equalTo(requestLength));
            assertThat(clientStats.closedTimeMillis(), equalTo(closeTimeMillis));
            assertThat(clientStats.openedTimeMillis(), equalTo(openTimeMillis));

            final Map<String, String> relevantHeaders1 = getRelevantHeaders(httpRequest1);
            final Map<String, String> relevantHeaders2 = getRelevantHeaders(httpRequest2);
            assertThat(
                clientStats.agent(),
                equalTo(
                    Optional.empty()
                        .or(() -> Optional.ofNullable(relevantHeaders1.get("x-elastic-product-origin")))
                        .or(() -> Optional.ofNullable(relevantHeaders1.get("user-agent")))
                        .or(() -> Optional.ofNullable(relevantHeaders2.get("x-elastic-product-origin")))
                        .or(() -> Optional.ofNullable(relevantHeaders2.get("user-agent")))
                        .orElse(null)
                )
            );
            assertThat(
                clientStats.forwardedFor(),
                equalTo(
                    Optional.empty()
                        .or(() -> Optional.ofNullable(relevantHeaders1.get("x-forwarded-for")))
                        .or(() -> Optional.ofNullable(relevantHeaders2.get("x-forwarded-for")))
                        .orElse(null)
                )
            );
            assertThat(
                clientStats.opaqueId(),
                equalTo(
                    Optional.empty()
                        .or(() -> Optional.ofNullable(relevantHeaders1.get("x-opaque-id")))
                        .or(() -> Optional.ofNullable(relevantHeaders2.get("x-opaque-id")))
                        .orElse(null)
                )
            );
        }

        threadPool.setCurrentTimeInMillis(
            threadPool.relativeTimeInMillis() + maxAgeMillis + 1 + (randomBoolean() ? 0L : randomLongBetween(0L, 1L << 60))
        );
        assertThat(httpClientStatsTracker.getClientStats(), empty());
    }

    public void testLimitsNumberOfClosedClients() throws InterruptedException {

        final Settings settings;
        final int closedClientLimit;
        {
            final Settings.Builder builder = Settings.builder();
            if (usually()) {
                closedClientLimit = scaledRandomIntBetween(1, 10000);
                builder.put(SETTING_HTTP_CLIENT_STATS_MAX_CLOSED_CHANNEL_COUNT.getKey(), closedClientLimit);
            } else {
                closedClientLimit = SETTING_HTTP_CLIENT_STATS_MAX_CLOSED_CHANNEL_COUNT.get(Settings.EMPTY);
            }
            settings = builder.build();
        }

        final FakeTimeThreadPool threadPool = new FakeTimeThreadPool();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final HttpClientStatsTracker httpClientStatsTracker = new HttpClientStatsTracker(settings, clusterSettings, threadPool);

        final Thread[] clientThreads = new Thread[between(1, 5)];
        final CyclicBarrier startBarrier = new CyclicBarrier(clientThreads.length + 1);
        final Semaphore operationPermits = new Semaphore(closedClientLimit * 2);

        // If we get stats while a channel is concurrently closed then the iteration through the list of closed channels may see more
        // stats than expected, even though it was never in a state that had too many channels, so we block closing while retrieving stats
        final ReadWriteLock closeLock = new ReentrantReadWriteLock(true);
        final Consumer<HttpChannel> closeUnderLock = httpChannel -> {
            closeLock.readLock().lock();
            httpChannel.close();
            closeLock.readLock().unlock();
        };

        for (int i = 0; i < clientThreads.length; i++) {
            clientThreads[i] = new Thread(() -> {
                try {
                    startBarrier.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                    throw new AssertionError("unexpected", e);
                }

                HttpChannel httpChannel = randomHttpChannel();
                httpClientStatsTracker.addClientStats(httpChannel);
                while (operationPermits.tryAcquire()) {
                    if (usually()) {
                        closeUnderLock.accept(httpChannel);
                        httpChannel = randomHttpChannel();
                        httpClientStatsTracker.addClientStats(httpChannel);
                    }

                    httpClientStatsTracker.updateClientStats(randomHttpRequest(), httpChannel);
                }
                closeUnderLock.accept(httpChannel);

            }, "client-thread-" + i);
            clientThreads[i].start();
        }

        final AtomicBoolean keepGoing = new AtomicBoolean(true);
        final Thread statsThread = new Thread(() -> {
            try {
                startBarrier.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                throw new AssertionError("unexpected", e);
            }

            while (keepGoing.get()) {
                closeLock.writeLock().lock();
                final List<HttpStats.ClientStats> clientStats = httpClientStatsTracker.getClientStats();
                closeLock.writeLock().unlock();
                assertThat(
                    clientStats.stream().filter(c -> c.closedTimeMillis() >= 0L).count(),
                    lessThanOrEqualTo((long) closedClientLimit)
                );
            }

        }, "stats-thread");
        statsThread.start();

        for (Thread clientThread : clientThreads) {
            clientThread.join();
        }
        keepGoing.set(false);
        statsThread.join();
    }

    public void testClearsStatsIfDisabledConcurrently() throws InterruptedException {
        final FakeTimeThreadPool threadPool = new FakeTimeThreadPool();
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final HttpClientStatsTracker httpClientStatsTracker = new HttpClientStatsTracker(Settings.EMPTY, clusterSettings, threadPool);

        final Thread[] clientThreads = new Thread[between(1, 5)];
        final CyclicBarrier startBarrier = new CyclicBarrier(clientThreads.length + 1);
        final boolean expectPruning = randomBoolean();
        final Semaphore operationPermits = new Semaphore(
            between(
                1,
                expectPruning
                    ? SETTING_HTTP_CLIENT_STATS_MAX_CLOSED_CHANNEL_COUNT.get(Settings.EMPTY) - 1
                    : SETTING_HTTP_CLIENT_STATS_MAX_CLOSED_CHANNEL_COUNT.get(Settings.EMPTY) * 2
            )
        );
        for (int i = 0; i < clientThreads.length; i++) {
            clientThreads[i] = new Thread(() -> {
                try {
                    startBarrier.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                    throw new AssertionError("unexpected", e);
                }

                HttpChannel httpChannel = randomHttpChannel();
                httpClientStatsTracker.addClientStats(httpChannel);
                while (operationPermits.tryAcquire()) {
                    if (randomBoolean()) {
                        httpChannel.close();
                        httpChannel = randomHttpChannel();
                        httpClientStatsTracker.addClientStats(httpChannel);
                    }

                    httpClientStatsTracker.updateClientStats(randomHttpRequest(), httpChannel);
                }
                httpChannel.close();

            }, "client-thread-" + i);
            clientThreads[i].start();
        }

        try {
            startBarrier.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
            throw new AssertionError("unexpected", e);
        }
        clusterSettings.applySettings(Settings.builder().put(SETTING_HTTP_CLIENT_STATS_ENABLED.getKey(), false).build());

        try {
            assertThat(httpClientStatsTracker.getClientStats(), empty());

            // starts collecting stats again
            clusterSettings.applySettings(Settings.builder().put(SETTING_HTTP_CLIENT_STATS_ENABLED.getKey(), true).build());

            final HttpChannel httpChannel = randomHttpChannel();
            httpClientStatsTracker.addClientStats(httpChannel);
            if (expectPruning == false && randomBoolean()) {
                // won't be pruned, the clock is not moving and we don't open enough channels to hit the limit
                httpChannel.close();
            }
            assertTrue(
                httpClientStatsTracker.getClientStats()
                    .stream()
                    .anyMatch(cs -> cs.remoteAddress().equals(NetworkAddress.format(httpChannel.getRemoteAddress())))
            );
        } finally {
            for (Thread clientThread : clientThreads) {
                clientThread.join();
            }
        }
    }

    private Map<String, String> getRelevantHeaders(HttpRequest httpRequest) {
        final Map<String, String> headers = Maps.newMapWithExpectedSize(4);
        final String[] relevantHeaderNames = new String[] { "user-agent", "x-elastic-product-origin", "x-forwarded-for", "x-opaque-id" };
        for (Map.Entry<String, List<String>> header : httpRequest.getHeaders().entrySet()) {
            if (header.getValue().size() > 0) {
                for (String relevantHeaderName : relevantHeaderNames) {
                    if (header.getKey().equalsIgnoreCase(relevantHeaderName)) {
                        headers.putIfAbsent(relevantHeaderName, header.getValue().get(0));
                    }
                }
            }
        }
        return headers;
    }

    private HttpRequest randomHttpRequest() {
        final Map<String, List<String>> headers = new HashMap<>();
        putRandomHeader("user-agent", headers);
        putRandomHeader("x-elastic-product-origin", headers);
        putRandomHeader("x-forwarded-for", headers);
        putRandomHeader("x-opaque-id", headers);
        return new FakeRestRequest.FakeHttpRequest(
            randomFrom(RestRequest.Method.values()),
            randomAlphaOfLength(10),
            new BytesArray(randomByteArrayOfLength(between(0, 20))),
            headers
        );
    }

    private static void putRandomHeader(String key, Map<String, List<String>> headers) {
        if (randomBoolean()) {
            headers.put(randomizeCase(key), randomList(1, 5, () -> randomAlphaOfLengthBetween(5, 15)));
        }
    }

    private static String randomizeCase(String s) {
        final char[] chars = s.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            chars[i] = randomizeCase(chars[i]);
        }
        return new String(chars);
    }

    private static char randomizeCase(char c) {
        return switch (between(1, 3)) {
            case 1 -> Character.toUpperCase(c);
            case 2 -> Character.toLowerCase(c);
            default -> c;
        };
    }

    private HttpChannel randomHttpChannel() {
        return new FakeRestRequest.FakeHttpChannel(new InetSocketAddress(randomIp(randomBoolean()), randomIntBetween(1, 65535)));
    }

    private static class FakeTimeThreadPool extends ThreadPool {

        private long currentTimeInMillis;
        private final long absoluteTimeOffset = randomLong();

        FakeTimeThreadPool() {
            super(Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "test").build());
            stopCachedTimeThread();
            setRandomTime();
        }

        @Override
        public long relativeTimeInMillis() {
            return currentTimeInMillis;
        }

        @Override
        public long absoluteTimeInMillis() {
            return currentTimeInMillis + absoluteTimeOffset;
        }

        void setCurrentTimeInMillis(long currentTimeInMillis) {
            this.currentTimeInMillis = currentTimeInMillis;
        }

        void setRandomTime() {
            // absolute time needs to be nonnegative
            currentTimeInMillis = randomNonNegativeLong() - absoluteTimeOffset;
        }
    }
}
