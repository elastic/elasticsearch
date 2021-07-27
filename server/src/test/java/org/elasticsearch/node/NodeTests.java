/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.node;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine.Searcher;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.CircuitBreakerPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.MockHttpTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.test.NodeRoles.dataNode;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

@LuceneTestCase.SuppressFileSystems(value = "ExtrasFS")
public class NodeTests extends ESTestCase {

    public static class CheckPlugin extends Plugin {
        public static final BootstrapCheck CHECK = context -> BootstrapCheck.BootstrapCheckResult.success();

        @Override
        public List<BootstrapCheck> getBootstrapChecks() {
            return Collections.singletonList(CHECK);
        }
    }

    private List<Class<? extends Plugin>> basePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>();
        plugins.add(getTestTransportPlugin());
        plugins.add(MockHttpTransport.TestPlugin.class);
        return plugins;
    }

    public void testLoadPluginBootstrapChecks() throws IOException {
        final String name = randomBoolean() ? randomAlphaOfLength(10) : null;
        Settings.Builder settings = baseSettings();
        if (name != null) {
            settings.put(Node.NODE_NAME_SETTING.getKey(), name);
        }
        AtomicBoolean executed = new AtomicBoolean(false);
        List<Class<? extends Plugin>> plugins = basePlugins();
        plugins.add(CheckPlugin.class);
        try (Node node = new MockNode(settings.build(), plugins) {
            @Override
            protected void validateNodeBeforeAcceptingRequests(BootstrapContext context, BoundTransportAddress boundTransportAddress,
                                                               List<BootstrapCheck> bootstrapChecks) throws NodeValidationException {
                assertEquals(1, bootstrapChecks.size());
                assertSame(CheckPlugin.CHECK, bootstrapChecks.get(0));
                executed.set(true);
                throw new NodeValidationException("boom");
            }
        }) {
            expectThrows(NodeValidationException.class, () -> node.start());
            assertTrue(executed.get());
        }
    }

    public void testNodeAttributes() throws IOException {
        String attr = randomAlphaOfLength(5);
        Settings.Builder settings = baseSettings().put(Node.NODE_ATTRIBUTES.getKey() + "test_attr", attr);
        try (Node node = new MockNode(settings.build(), basePlugins())) {
            final Settings nodeSettings = randomBoolean() ? node.settings() : node.getEnvironment().settings();
            assertEquals(attr, Node.NODE_ATTRIBUTES.getAsMap(nodeSettings).get("test_attr"));
        }

        // leading whitespace not allowed
        attr = " leading";
        settings = baseSettings().put(Node.NODE_ATTRIBUTES.getKey() + "test_attr", attr);
        try (Node node = new MockNode(settings.build(), basePlugins())) {
            fail("should not allow a node attribute with leading whitespace");
        } catch (IllegalArgumentException e) {
            assertEquals("node.attr.test_attr cannot have leading or trailing whitespace [ leading]", e.getMessage());
        }

        // trailing whitespace not allowed
        attr = "trailing ";
        settings = baseSettings().put(Node.NODE_ATTRIBUTES.getKey() + "test_attr", attr);
        try (Node node = new MockNode(settings.build(), basePlugins())) {
            fail("should not allow a node attribute with trailing whitespace");
        } catch (IllegalArgumentException e) {
            assertEquals("node.attr.test_attr cannot have leading or trailing whitespace [trailing ]", e.getMessage());
        }
    }

    public void testServerNameNodeAttribute() throws IOException {
        String attr = "valid-hostname";
        Settings.Builder settings = baseSettings().put(Node.NODE_ATTRIBUTES.getKey() + "server_name", attr);
        int i = 0;
        try (Node node = new MockNode(settings.build(), basePlugins())) {
            final Settings nodeSettings = randomBoolean() ? node.settings() : node.getEnvironment().settings();
            assertEquals(attr, Node.NODE_ATTRIBUTES.getAsMap(nodeSettings).get("server_name"));
        }

        // non-LDH hostname not allowed
        attr = "invalid_hostname";
        settings = baseSettings().put(Node.NODE_ATTRIBUTES.getKey() + "server_name", attr);
        try (Node node = new MockNode(settings.build(), basePlugins())) {
            fail("should not allow a server_name attribute with an underscore");
        } catch (IllegalArgumentException e) {
            assertEquals("invalid node.attr.server_name [invalid_hostname]", e.getMessage());
        }
    }

    private static Settings.Builder baseSettings() {
        final Path tempDir = createTempDir();
        return Settings.builder()
            .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), InternalTestCluster.clusterName("single-node-cluster", randomLong()))
            .put(Environment.PATH_HOME_SETTING.getKey(), tempDir)
            .put(NetworkModule.TRANSPORT_TYPE_KEY, getTestTransportType())
            .put(dataNode());
    }

    public void testCloseOnOutstandingTask() throws Exception {
        Node node = new MockNode(baseSettings().build(), basePlugins());
        node.start();
        ThreadPool threadpool = node.injector().getInstance(ThreadPool.class);
        AtomicBoolean shouldRun = new AtomicBoolean(true);
        final CountDownLatch threadRunning = new CountDownLatch(1);
        threadpool.executor(ThreadPool.Names.SEARCH).execute(() -> {
            threadRunning.countDown();
            while (shouldRun.get()) ;
        });
        threadRunning.await();
        node.close();
        shouldRun.set(false);
        assertTrue(node.awaitClose(10L, TimeUnit.SECONDS));
    }

    public void testCloseRaceWithTaskExecution() throws Exception {
        Node node = new MockNode(baseSettings().build(), basePlugins());
        node.start();
        ThreadPool threadpool = node.injector().getInstance(ThreadPool.class);
        AtomicBoolean shouldRun = new AtomicBoolean(true);
        final CountDownLatch running = new CountDownLatch(3);
        Thread submitThread = new Thread(() -> {
            running.countDown();
            try {
                running.await();
            } catch (InterruptedException e) {
                throw new AssertionError("interrupted while waiting", e);
            }
            try {
                threadpool.executor(ThreadPool.Names.SEARCH).execute(() -> {
                    while (shouldRun.get()) ;
                });
            } catch (RejectedExecutionException e) {
                assertThat(e.getMessage(), containsString("[Terminated,"));
            }
        });
        Thread closeThread = new Thread(() -> {
            running.countDown();
            try {
                running.await();
            } catch (InterruptedException e) {
                throw new AssertionError("interrupted while waiting", e);
            }
            try {
                node.close();
            } catch (IOException e) {
                throw new AssertionError("node close failed", e);
            }
        });
        submitThread.start();
        closeThread.start();
        running.countDown();
        running.await();

        submitThread.join();
        closeThread.join();

        shouldRun.set(false);
        assertTrue(node.awaitClose(10L, TimeUnit.SECONDS));
    }

    public void testAwaitCloseTimeoutsOnNonInterruptibleTask() throws Exception {
        Node node = new MockNode(baseSettings().build(), basePlugins());
        node.start();
        ThreadPool threadpool = node.injector().getInstance(ThreadPool.class);
        AtomicBoolean shouldRun = new AtomicBoolean(true);
        final CountDownLatch threadRunning = new CountDownLatch(1);
        threadpool.executor(ThreadPool.Names.SEARCH).execute(() -> {
            threadRunning.countDown();
            while (shouldRun.get()) ;
        });
        threadRunning.await();
        node.close();
        assertFalse(node.awaitClose(0, TimeUnit.MILLISECONDS));
        shouldRun.set(false);
        assertTrue(node.awaitClose(10L, TimeUnit.SECONDS));
    }

    public void testCloseOnInterruptibleTask() throws Exception {
        Node node = new MockNode(baseSettings().build(), basePlugins());
        node.start();
        ThreadPool threadpool = node.injector().getInstance(ThreadPool.class);
        final CountDownLatch threadRunning = new CountDownLatch(1);
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(1);
        final AtomicBoolean interrupted = new AtomicBoolean(false);
        threadpool.executor(ThreadPool.Names.SEARCH).execute(() -> {
            threadRunning.countDown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                interrupted.set(true);
                Thread.currentThread().interrupt();
            } finally {
                finishLatch.countDown();
            }
        });
        threadRunning.await();
        node.close();
        // close should not interrupt ongoing tasks
        assertFalse(interrupted.get());
        // but awaitClose should
        node.awaitClose(0, TimeUnit.SECONDS);
        finishLatch.await();
        assertTrue(interrupted.get());
    }

    public void testCloseOnLeakedIndexReaderReference() throws Exception {
        Node node = new MockNode(baseSettings().build(), basePlugins());
        node.start();
        IndicesService indicesService = node.injector().getInstance(IndicesService.class);
        assertAcked(node.client().admin().indices().prepareCreate("test")
            .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0)));
        IndexService indexService = indicesService.iterator().next();
        IndexShard shard = indexService.getShard(0);
        Searcher searcher = shard.acquireSearcher("test");
        node.close();

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> node.awaitClose(10L, TimeUnit.SECONDS));
        searcher.close();
        assertThat(e.getMessage(), containsString("Something is leaking index readers or store references"));
    }

    public void testCloseOnLeakedStoreReference() throws Exception {
        Node node = new MockNode(baseSettings().build(), basePlugins());
        node.start();
        IndicesService indicesService = node.injector().getInstance(IndicesService.class);
        assertAcked(node.client().admin().indices().prepareCreate("test")
            .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0)));
        IndexService indexService = indicesService.iterator().next();
        IndexShard shard = indexService.getShard(0);
        shard.store().incRef();
        node.close();

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> node.awaitClose(10L, TimeUnit.SECONDS));
        shard.store().decRef();
        assertThat(e.getMessage(), containsString("Something is leaking index readers or store references"));
    }

    public void testCreateWithCircuitBreakerPlugins() throws IOException {
        Settings.Builder settings = baseSettings()
            .put("breaker.test_breaker.limit", "50b");
        List<Class<? extends Plugin>> plugins = basePlugins();
        plugins.add(MockCircuitBreakerPlugin.class);
        try (Node node = new MockNode(settings.build(), plugins)) {
            CircuitBreakerService service = node.injector().getInstance(CircuitBreakerService.class);
            assertThat(service.getBreaker("test_breaker"), is(not(nullValue())));
            assertThat(service.getBreaker("test_breaker").getLimit(), equalTo(50L));
            CircuitBreakerPlugin breakerPlugin = node.getPluginsService().filterPlugins(CircuitBreakerPlugin.class).get(0);
            assertTrue(breakerPlugin instanceof MockCircuitBreakerPlugin);
            assertSame("plugin circuit breaker instance is not the same as breaker service's instance",
                ((MockCircuitBreakerPlugin) breakerPlugin).myCircuitBreaker.get(),
                service.getBreaker("test_breaker"));
        }
    }

    public static class MockCircuitBreakerPlugin extends Plugin implements CircuitBreakerPlugin {

        private SetOnce<CircuitBreaker> myCircuitBreaker = new SetOnce<>();

        public MockCircuitBreakerPlugin() {}

        @Override
        public BreakerSettings getCircuitBreaker(Settings settings) {
            return BreakerSettings.updateFromSettings(
                new BreakerSettings("test_breaker",
                    100L,
                    1.0d,
                    CircuitBreaker.Type.MEMORY,
                    CircuitBreaker.Durability.TRANSIENT),
                settings);
        }

        @Override
        public void setCircuitBreaker(CircuitBreaker circuitBreaker) {
            assertThat(circuitBreaker.getName(), equalTo("test_breaker"));
            myCircuitBreaker.set(circuitBreaker);
        }
    }


    interface MockRestApiVersion {
        RestApiVersion minimumRestCompatibilityVersion();
    }

    static MockRestApiVersion MockCompatibleVersion = mock(MockRestApiVersion.class);

    static NamedXContentRegistry.Entry v7CompatibleEntries = new NamedXContentRegistry.Entry(Integer.class,
        new ParseField("name"), mock(ContextParser.class));
    static NamedXContentRegistry.Entry v8CompatibleEntries = new NamedXContentRegistry.Entry(Integer.class,
        new ParseField("name2"), mock(ContextParser.class));

    public static class TestRestCompatibility1 extends Plugin {

        @Override
        public List<NamedXContentRegistry.Entry> getNamedXContentForCompatibility() {
            // real plugin will use CompatibleVersion.minimumRestCompatibilityVersion()
            if (/*RestApiVersion.minimumSupported() == */
                MockCompatibleVersion.minimumRestCompatibilityVersion().equals(RestApiVersion.V_7)) {
                //return set of N-1 entries
                return List.of(v7CompatibleEntries);
            }
            // after major release, new compatible apis can be added before the old ones are removed.
            if (/*RestApiVersion.minimumSupported() == */
                MockCompatibleVersion.minimumRestCompatibilityVersion().equals(RestApiVersion.V_8)) {
                return List.of(v8CompatibleEntries);

            }
            return super.getNamedXContentForCompatibility();
        }
    }

    // This test shows an example on how multiple compatible namedxcontent can be present at the same time.
    public void testLoadingMultipleRestCompatibilityPlugins() throws IOException {

        Mockito.when(MockCompatibleVersion.minimumRestCompatibilityVersion())
            .thenReturn(RestApiVersion.V_7);

        {
            Settings.Builder settings = baseSettings();

            // throw an exception when two plugins are registered
            List<Class<? extends Plugin>> plugins = basePlugins();
            plugins.add(TestRestCompatibility1.class);

            try (Node node = new MockNode(settings.build(), plugins)) {
                List<NamedXContentRegistry.Entry> compatibleNamedXContents = node.getCompatibleNamedXContents()
                    .collect(Collectors.toList());
                assertThat(compatibleNamedXContents, contains(v7CompatibleEntries));
            }
        }
        // after version bump CompatibleVersion.minimumRestCompatibilityVersion() will return V_8
        Mockito.when(MockCompatibleVersion.minimumRestCompatibilityVersion())
            .thenReturn(RestApiVersion.V_8);
        {
            Settings.Builder settings = baseSettings();

            // throw an exception when two plugins are registered
            List<Class<? extends Plugin>> plugins = basePlugins();
            plugins.add(TestRestCompatibility1.class);

            try (Node node = new MockNode(settings.build(), plugins)) {
                List<NamedXContentRegistry.Entry> compatibleNamedXContents = node.getCompatibleNamedXContents()
                    .collect(Collectors.toList());;
                assertThat(compatibleNamedXContents, contains(v8CompatibleEntries));
            }
        }
    }


}
