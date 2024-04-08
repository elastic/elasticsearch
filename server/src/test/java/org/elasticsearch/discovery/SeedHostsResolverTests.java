/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery;

import org.apache.logging.log4j.Level;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.netty4.Netty4Transport;
import org.elasticsearch.transport.netty4.SharedGroupFactory;
import org.junit.After;
import org.junit.Before;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.IsNull.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SeedHostsResolverTests extends ESTestCase {

    private List<TransportAddress> transportAddresses;
    private SeedHostsResolver seedHostsResolver;
    private ThreadPool threadPool;
    // close in reverse order as opened
    private Stack<Closeable> closeables;

    @Before
    public void startResolver() {
        threadPool = new TestThreadPool("node");
        transportAddresses = new ArrayList<>();
        closeables = new Stack<>();

        TransportService transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(threadPool);
        recreateSeedHostsResolver(transportService);
    }

    private void recreateSeedHostsResolver(TransportService transportService) {
        recreateSeedHostsResolver(transportService, Settings.EMPTY);
    }

    private void recreateSeedHostsResolver(TransportService transportService, Settings settings) {
        if (seedHostsResolver != null) {
            seedHostsResolver.stop();
        }
        seedHostsResolver = new SeedHostsResolver("test_node", settings, transportService, hostsResolver -> transportAddresses);
        seedHostsResolver.start();
    }

    @After
    public void stopResolver() throws IOException {
        seedHostsResolver.stop();
        try {
            logger.info("shutting down...");
            // JDK stack is broken, it does not iterate in the expected order (http://bugs.java.com/bugdatabase/view_bug.do?bug_id=4475301)
            final List<Closeable> reverse = new ArrayList<>();
            while (closeables.isEmpty() == false) {
                reverse.add(closeables.pop());
            }
            IOUtils.close(reverse);
        } finally {
            terminate(threadPool);
        }
    }

    public void testResolvesAddressesInBackgroundAndIgnoresConcurrentCalls() throws Exception {
        final AtomicReference<List<TransportAddress>> resolvedAddressesRef = new AtomicReference<>();
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch endLatch = new CountDownLatch(1);

        final int addressCount = randomIntBetween(0, 5);
        for (int i = 0; i < addressCount; i++) {
            transportAddresses.add(buildNewFakeTransportAddress());
        }

        seedHostsResolver.resolveConfiguredHosts(resolvedAddresses -> {
            safeAwait(startLatch);
            resolvedAddressesRef.set(resolvedAddresses);
            endLatch.countDown();
        });

        seedHostsResolver.resolveConfiguredHosts(resolvedAddresses -> { throw new AssertionError("unexpected concurrent resolution"); });

        assertThat(resolvedAddressesRef.get(), nullValue());
        startLatch.countDown();
        assertTrue(endLatch.await(30, TimeUnit.SECONDS));
        assertThat(resolvedAddressesRef.get(), equalTo(transportAddresses));
    }

    public void testRemovingLocalAddresses() {
        final NetworkService networkService = new NetworkService(Collections.emptyList());
        final InetAddress loopbackAddress = InetAddress.getLoopbackAddress();
        final Transport transport = new Netty4Transport(
            Settings.EMPTY,
            TransportVersion.current(),
            threadPool,
            networkService,
            PageCacheRecycler.NON_RECYCLING_INSTANCE,
            new NamedWriteableRegistry(Collections.emptyList()),
            new NoneCircuitBreakerService(),
            new SharedGroupFactory(Settings.EMPTY)
        ) {

            @Override
            public BoundTransportAddress boundAddress() {
                return new BoundTransportAddress(
                    new TransportAddress[] { new TransportAddress(loopbackAddress, 9300), new TransportAddress(loopbackAddress, 9301) },
                    new TransportAddress(loopbackAddress, 9302)
                );
            }
        };
        closeables.push(transport);
        final TransportService transportService = new TransportService(
            Settings.EMPTY,
            transport,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );
        closeables.push(transportService);
        recreateSeedHostsResolver(transportService);
        List<String> hosts = IntStream.range(9300, 9310).mapToObj(port -> NetworkAddress.format(loopbackAddress) + ":" + port).toList();
        final List<TransportAddress> transportAddresses = seedHostsResolver.resolveHosts(hosts);
        assertThat(transportAddresses, hasSize(7));
        final Set<Integer> ports = new HashSet<>();
        for (final TransportAddress address : transportAddresses) {
            assertTrue(address.address().getAddress().isLoopbackAddress());
            ports.add(address.getPort());
        }
        assertThat(ports, equalTo(IntStream.range(9303, 9310).boxed().collect(Collectors.toSet())));
    }

    public void testUnknownHost() {
        final NetworkService networkService = new NetworkService(Collections.emptyList());
        final String hostname = randomAlphaOfLength(8);
        final UnknownHostException unknownHostException = new UnknownHostException(hostname);
        final Transport transport = new Netty4Transport(
            Settings.EMPTY,
            TransportVersion.current(),
            threadPool,
            networkService,
            PageCacheRecycler.NON_RECYCLING_INSTANCE,
            new NamedWriteableRegistry(Collections.emptyList()),
            new NoneCircuitBreakerService(),
            new SharedGroupFactory(Settings.EMPTY)
        ) {

            @Override
            public BoundTransportAddress boundAddress() {
                return new BoundTransportAddress(
                    new TransportAddress[] { new TransportAddress(InetAddress.getLoopbackAddress(), 9300) },
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9300)
                );
            }

            @Override
            public TransportAddress[] addressesFromString(String address) throws UnknownHostException {
                throw unknownHostException;
            }

        };
        closeables.push(transport);

        final TransportService transportService = new TransportService(
            Settings.EMPTY,
            transport,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );
        closeables.push(transportService);
        recreateSeedHostsResolver(transportService);

        final MockLogAppender appender = new MockLogAppender();
        appender.addExpectation(
            new MockLogAppender.ExceptionSeenEventExpectation(
                getTestName(),
                SeedHostsResolver.class.getCanonicalName(),
                Level.WARN,
                "failed to resolve host [" + hostname + "]",
                UnknownHostException.class,
                unknownHostException.getMessage()
            )
        );

        try (var ignored = appender.capturing(SeedHostsResolver.class)) {
            assertThat(seedHostsResolver.resolveHosts(Collections.singletonList(hostname)), empty());
            appender.assertAllExpectationsMatched();
        }
    }

    public void testResolveTimeout() {
        final NetworkService networkService = new NetworkService(Collections.emptyList());
        final CountDownLatch latch = new CountDownLatch(1);
        final Transport transport = new Netty4Transport(
            Settings.EMPTY,
            TransportVersion.current(),
            threadPool,
            networkService,
            PageCacheRecycler.NON_RECYCLING_INSTANCE,
            new NamedWriteableRegistry(Collections.emptyList()),
            new NoneCircuitBreakerService(),
            new SharedGroupFactory(Settings.EMPTY)
        ) {

            @Override
            public BoundTransportAddress boundAddress() {
                return new BoundTransportAddress(
                    new TransportAddress[] { new TransportAddress(InetAddress.getLoopbackAddress(), 9500) },
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9500)
                );
            }

            @Override
            public TransportAddress[] addressesFromString(String address) throws UnknownHostException {
                if ("hostname1".equals(address)) {
                    return new TransportAddress[] { new TransportAddress(TransportAddress.META_ADDRESS, 9300) };
                } else if ("hostname2".equals(address)) {
                    try {
                        latch.await();
                        return new TransportAddress[] { new TransportAddress(TransportAddress.META_ADDRESS, 9300) };
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    throw new UnknownHostException(address);
                }
            }

        };
        closeables.push(transport);

        final TransportService transportService = new TransportService(
            Settings.EMPTY,
            transport,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );
        closeables.push(transportService);
        recreateSeedHostsResolver(transportService);

        final MockLogAppender appender = new MockLogAppender();
        appender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                getTestName(),
                SeedHostsResolver.class.getCanonicalName(),
                Level.WARN,
                "timed out after [*] ([discovery.seed_resolver.timeout]=["
                    + SeedHostsResolver.getResolveTimeout(Settings.EMPTY)
                    + "]) resolving host [hostname2]"
            )
        );

        try (var ignored = appender.capturing(SeedHostsResolver.class)) {
            assertThat(seedHostsResolver.resolveHosts(Arrays.asList("hostname1", "hostname2")), hasSize(1));
            appender.assertAllExpectationsMatched();
        } finally {
            latch.countDown();
        }
    }

    public void testCancellationOnClose() throws InterruptedException {
        final NetworkService networkService = new NetworkService(Collections.emptyList());
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch conditionLatch = new CountDownLatch(1);
        final Transport transport = new Netty4Transport(
            Settings.EMPTY,
            TransportVersion.current(),
            threadPool,
            networkService,
            PageCacheRecycler.NON_RECYCLING_INSTANCE,
            new NamedWriteableRegistry(Collections.emptyList()),
            new NoneCircuitBreakerService(),
            new SharedGroupFactory(Settings.EMPTY)
        ) {

            @Override
            public BoundTransportAddress boundAddress() {
                return new BoundTransportAddress(
                    new TransportAddress[] { new TransportAddress(InetAddress.getLoopbackAddress(), 9500) },
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9500)
                );
            }

            @Override
            public TransportAddress[] addressesFromString(String address) throws UnknownHostException {
                if ("hostname1".equals(address)) {
                    return new TransportAddress[] { new TransportAddress(TransportAddress.META_ADDRESS, 9300) };
                } else if ("hostname2".equals(address)) {
                    try {
                        conditionLatch.countDown();
                        latch.await();
                        throw new AssertionError("should never be called");
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    throw new UnknownHostException(address);
                }
            }

        };
        closeables.push(transport);

        final TransportService transportService = new TransportService(
            Settings.EMPTY,
            transport,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );
        closeables.push(transportService);
        recreateSeedHostsResolver(
            transportService,
            Settings.builder().put(SeedHostsResolver.DISCOVERY_SEED_RESOLVER_TIMEOUT_SETTING.getKey(), "10m").build()
        );

        final PlainActionFuture<List<TransportAddress>> fut = new PlainActionFuture<>();
        threadPool.generic().execute((() -> fut.onResponse(seedHostsResolver.resolveHosts(Arrays.asList("hostname1", "hostname2")))));

        conditionLatch.await();
        seedHostsResolver.stop();
        assertThat(FutureUtils.get(fut, 10, TimeUnit.SECONDS), hasSize(0));
    }

    public void testInvalidHosts() {
        final Transport transport = new Netty4Transport(
            Settings.EMPTY,
            TransportVersion.current(),
            threadPool,
            new NetworkService(Collections.emptyList()),
            PageCacheRecycler.NON_RECYCLING_INSTANCE,
            new NamedWriteableRegistry(Collections.emptyList()),
            new NoneCircuitBreakerService(),
            new SharedGroupFactory(Settings.EMPTY)
        ) {
            @Override
            public BoundTransportAddress boundAddress() {
                return new BoundTransportAddress(
                    new TransportAddress[] { new TransportAddress(InetAddress.getLoopbackAddress(), 9300) },
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9300)
                );
            }
        };
        closeables.push(transport);

        final TransportService transportService = new TransportService(
            Settings.EMPTY,
            transport,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );
        closeables.push(transportService);
        recreateSeedHostsResolver(transportService);

        final MockLogAppender appender = new MockLogAppender();
        appender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                getTestName(),
                SeedHostsResolver.class.getCanonicalName(),
                Level.WARN,
                "failed to resolve host [127.0.0.1:9300:9300]"
            )
        );

        try (var ignored = appender.capturing(SeedHostsResolver.class)) {
            final List<TransportAddress> transportAddresses = seedHostsResolver.resolveHosts(
                Arrays.asList("127.0.0.1:9300:9300", "127.0.0.1:9301")
            );
            assertThat(transportAddresses, hasSize(1)); // only one of the two is valid and will be used
            assertThat(transportAddresses.get(0).getAddress(), equalTo("127.0.0.1"));
            assertThat(transportAddresses.get(0).getPort(), equalTo(9301));
            appender.assertAllExpectationsMatched();
        }
    }
}
