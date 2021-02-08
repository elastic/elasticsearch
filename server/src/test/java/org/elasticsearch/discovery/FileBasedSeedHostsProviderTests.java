/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.nio.MockNioTransport;
import org.junit.After;
import org.junit.Before;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.elasticsearch.discovery.FileBasedSeedHostsProvider.UNICAST_HOSTS_FILE;

public class FileBasedSeedHostsProviderTests extends ESTestCase {

    private ThreadPool threadPool;
    private ExecutorService executorService;
    private MockTransportService transportService;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(FileBasedSeedHostsProviderTests.class.getName());
        executorService = Executors.newSingleThreadExecutor();
        createTransportSvc();
    }

    @After
    public void tearDown() throws Exception {
        try {
            terminate(executorService);
        } finally {
            try {
                terminate(threadPool);
            } finally {
                super.tearDown();
            }
        }
    }

    private void createTransportSvc() {
        final MockNioTransport transport = new MockNioTransport(Settings.EMPTY, Version.CURRENT, threadPool,
            new NetworkService(Collections.emptyList()),
            PageCacheRecycler.NON_RECYCLING_INSTANCE,
            new NamedWriteableRegistry(Collections.emptyList()),
            new NoneCircuitBreakerService()) {
            @Override
            public BoundTransportAddress boundAddress() {
                return new BoundTransportAddress(
                    new TransportAddress[]{new TransportAddress(InetAddress.getLoopbackAddress(), 9300)},
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9300)
                );
            }
        };
        transportService = new MockTransportService(Settings.EMPTY, transport, threadPool, TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            null);
    }

    public void testBuildDynamicNodes() throws Exception {
        final List<String> hostEntries = Arrays.asList("#comment, should be ignored", "192.168.0.1", "192.168.0.2:9305", "255.255.23.15");
        final List<TransportAddress> nodes = setupAndRunHostProvider(hostEntries);
        assertEquals(hostEntries.size() - 1, nodes.size()); // minus 1 because we are ignoring the first line that's a comment
        assertEquals("192.168.0.1", nodes.get(0).getAddress());
        assertEquals(9300, nodes.get(0).getPort());
        assertEquals("192.168.0.2", nodes.get(1).getAddress());
        assertEquals(9305, nodes.get(1).getPort());
        assertEquals("255.255.23.15", nodes.get(2).getAddress());
        assertEquals(9300, nodes.get(2).getPort());
    }

    public void testEmptyUnicastHostsFile() throws Exception {
        final List<String> hostEntries = Collections.emptyList();
        final List<TransportAddress> addresses = setupAndRunHostProvider(hostEntries);
        assertEquals(0, addresses.size());
    }

    public void testUnicastHostsDoesNotExist() {
        final FileBasedSeedHostsProvider fileBasedSeedHostsProvider = new FileBasedSeedHostsProvider(createTempDir().toAbsolutePath());
        SeedHostsResolver seedHostsResolver = new SeedHostsResolver("test", Settings.EMPTY, transportService, fileBasedSeedHostsProvider);
        seedHostsResolver.start();
        List<TransportAddress> results = fileBasedSeedHostsProvider.getSeedAddresses(seedHostsResolver);
        seedHostsResolver.stop();
        assertEquals(0, results.size());
    }

    public void testInvalidHostEntries() throws Exception {
        final List<String> hostEntries = Collections.singletonList("192.168.0.1:9300:9300");
        final List<TransportAddress> addresses = setupAndRunHostProvider(hostEntries);
        assertEquals(0, addresses.size());
    }

    public void testSomeInvalidHostEntries() throws Exception {
        final List<String> hostEntries = Arrays.asList("192.168.0.1:9300:9300", "192.168.0.1:9301");
        final List<TransportAddress> addresses = setupAndRunHostProvider(hostEntries);
        assertEquals(1, addresses.size()); // only one of the two is valid and will be used
        assertEquals("192.168.0.1", addresses.get(0).getAddress());
        assertEquals(9301, addresses.get(0).getPort());
    }

    // sets up the config dir, writes to the unicast hosts file in the config dir,
    // and then runs the file-based unicast host provider to get the list of discovery nodes
    private List<TransportAddress> setupAndRunHostProvider(final List<String> hostEntries) throws IOException {
        final Path homeDir = createTempDir();
        final Path configPath = randomBoolean() ? homeDir.resolve("config") : createTempDir();
        Files.createDirectories(configPath);
        try (BufferedWriter writer = Files.newBufferedWriter(configPath.resolve(UNICAST_HOSTS_FILE))) {
            writer.write(String.join("\n", hostEntries));
        }

        FileBasedSeedHostsProvider fileBasedSeedHostsProvider = new FileBasedSeedHostsProvider(configPath);
        SeedHostsResolver seedHostsResolver = new SeedHostsResolver("test", Settings.EMPTY, transportService, fileBasedSeedHostsProvider);
        seedHostsResolver.start();
        List<TransportAddress> results = fileBasedSeedHostsProvider.getSeedAddresses(seedHostsResolver);
        seedHostsResolver.stop();
        return results;
    }
}
