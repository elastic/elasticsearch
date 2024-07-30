/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.node;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.env.Environment;
import org.elasticsearch.gateway.PersistedClusterStateService;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine.Searcher;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.recovery.plan.RecoveryPlannerService;
import org.elasticsearch.indices.recovery.plan.ShardSnapshotsService;
import org.elasticsearch.plugins.CircuitBreakerPlugin;
import org.elasticsearch.plugins.ClusterCoordinationPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsServiceTests;
import org.elasticsearch.plugins.RecoveryPlannerPlugin;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.MockHttpTransport;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ContextParser;
import org.elasticsearch.xcontent.MediaType;
import org.elasticsearch.xcontent.NamedObjectNotFoundException;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.test.NodeRoles.dataNode;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

@LuceneTestCase.SuppressFileSystems(value = "ExtrasFS")
public class NodeTests extends ESTestCase {

    public static class CheckPlugin extends Plugin {
        public static final BootstrapCheck CHECK = new BootstrapCheck() {
            @Override
            public BootstrapCheckResult check(BootstrapContext context) {
                return BootstrapCheck.BootstrapCheckResult.success();
            }

            @Override
            public ReferenceDocs referenceDocs() {
                return ReferenceDocs.BOOTSTRAP_CHECKS;
            }
        };

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
            protected void validateNodeBeforeAcceptingRequests(
                BootstrapContext context,
                BoundTransportAddress boundTransportAddress,
                List<BootstrapCheck> bootstrapChecks
            ) throws NodeValidationException {
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
            // default the watermarks low values to prevent tests from failing on nodes without enough disk space
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "1b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "1b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "1b")
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
            while (shouldRun.get())
                ;
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
                    while (shouldRun.get())
                        ;
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
            while (shouldRun.get())
                ;
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
        assertAcked(node.client().admin().indices().prepareCreate("test").setSettings(indexSettings(1, 0)));
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
        assertAcked(node.client().admin().indices().prepareCreate("test").setSettings(indexSettings(1, 0)));
        IndexService indexService = indicesService.iterator().next();
        IndexShard shard = indexService.getShard(0);
        shard.store().incRef();
        node.close();

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> node.awaitClose(10L, TimeUnit.SECONDS));
        shard.store().decRef();
        assertThat(e.getMessage(), containsString("Something is leaking index readers or store references"));
    }

    public void testStartOnClosedTransport() throws IOException {
        try (Node node = new MockNode(baseSettings().build(), basePlugins())) {
            node.prepareForClose();
            expectThrows(AssertionError.class, node::start);    // this would be IllegalStateException in a real Node with assertions off
        }
    }

    public void testCreateWithCircuitBreakerPlugins() throws IOException {
        Settings.Builder settings = baseSettings().put("breaker.test_breaker.limit", "50b");
        List<Class<? extends Plugin>> plugins = basePlugins();
        plugins.add(MockCircuitBreakerPlugin.class);
        try (Node node = new MockNode(settings.build(), plugins)) {
            CircuitBreakerService service = node.injector().getInstance(CircuitBreakerService.class);
            assertThat(service.getBreaker("test_breaker"), is(not(nullValue())));
            assertThat(service.getBreaker("test_breaker").getLimit(), equalTo(50L));
            CircuitBreakerPlugin breakerPlugin = node.getPluginsService().filterPlugins(CircuitBreakerPlugin.class).findFirst().get();
            assertTrue(breakerPlugin instanceof MockCircuitBreakerPlugin);
            assertSame(
                "plugin circuit breaker instance is not the same as breaker service's instance",
                ((MockCircuitBreakerPlugin) breakerPlugin).myCircuitBreaker.get(),
                service.getBreaker("test_breaker")
            );
        }
    }

    /**
     * TODO: Remove this test once classpath plugins are fully moved to MockNode.
     * In production, plugin name clashes are checked in a completely different way.
     * See {@link PluginsServiceTests#testPluginNameClash()}
     */
    public void testNodeFailsToStartWhenThereAreMultipleRecoveryPlannerPluginsLoaded() {
        List<Class<? extends Plugin>> plugins = basePlugins();
        plugins.add(MockRecoveryPlannerPlugin.class);
        plugins.add(MockRecoveryPlannerPlugin.class);
        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> new MockNode(baseSettings().build(), plugins));
        assertThat(exception.getMessage(), containsString("Duplicate key org.elasticsearch.node.NodeTests$MockRecoveryPlannerPlugin"));
    }

    public void testHeadersToCopyInTaskManagerAreTheSameAsDeclaredInTask() throws IOException {
        Settings.Builder settings = baseSettings();
        try (Node node = new MockNode(settings.build(), basePlugins())) {
            final TransportService transportService = node.injector().getInstance(TransportService.class);
            final Set<String> taskHeaders = transportService.getTaskManager().getTaskHeaders();
            assertThat(taskHeaders, containsInAnyOrder(Task.HEADERS_TO_COPY.toArray()));
        }
    }

    public static class MockPluginWithAltImpl extends Plugin {
        private final boolean randomBool;
        private static boolean startCalled = false;
        private static boolean stopCalled = false;
        private static boolean closeCalled = false;

        public MockPluginWithAltImpl() {
            this.randomBool = randomBoolean();
        }

        interface MyInterface extends LifecycleComponent {
            String get();

        }

        static class Foo extends AbstractLifecycleComponent implements MyInterface {
            @Override
            public String get() {
                return "foo";
            }

            @Override
            protected void doStart() {
                startCalled = true;
            }

            @Override
            protected void doStop() {
                stopCalled = true;
            }

            @Override
            protected void doClose() throws IOException {
                closeCalled = true;
            }
        }

        static class Bar extends AbstractLifecycleComponent implements MyInterface {
            @Override
            public String get() {
                return "bar";
            }

            @Override
            protected void doStart() {
                startCalled = true;
            }

            @Override
            protected void doStop() {
                stopCalled = true;
            }

            @Override
            protected void doClose() throws IOException {
                closeCalled = true;
            }
        }

        @Override
        public Collection<?> createComponents(PluginServices services) {
            List<Object> components = new ArrayList<>();
            components.add(new PluginComponentBinding<>(MyInterface.class, getRandomBool() ? new Foo() : new Bar()));
            return components;
        }

        public boolean getRandomBool() {
            return this.randomBool;
        }

    }

    public static class MockRecoveryPlannerPlugin extends Plugin implements RecoveryPlannerPlugin {
        public MockRecoveryPlannerPlugin() {}

        @Override
        public Optional<RecoveryPlannerService> createRecoveryPlannerService(ShardSnapshotsService shardSnapshotsService) {
            return Optional.of(mock(RecoveryPlannerService.class));
        }
    }

    public static class MockCircuitBreakerPlugin extends Plugin implements CircuitBreakerPlugin {

        private SetOnce<CircuitBreaker> myCircuitBreaker = new SetOnce<>();

        public MockCircuitBreakerPlugin() {}

        @Override
        public BreakerSettings getCircuitBreaker(Settings settings) {
            return BreakerSettings.updateFromSettings(
                new BreakerSettings("test_breaker", 100L, 1.0d, CircuitBreaker.Type.MEMORY, CircuitBreaker.Durability.TRANSIENT),
                settings
            );
        }

        @Override
        public void setCircuitBreaker(CircuitBreaker circuitBreaker) {
            assertThat(circuitBreaker.getName(), equalTo("test_breaker"));
            myCircuitBreaker.set(circuitBreaker);
        }
    }

    @SuppressWarnings("unchecked")
    private static ContextParser<Object, Integer> mockContextParser() {
        return mock(ContextParser.class);
    }

    static NamedXContentRegistry.Entry compatibleEntries = new NamedXContentRegistry.Entry(
        Integer.class,
        new ParseField("name").forRestApiVersion(RestApiVersion.equalTo(RestApiVersion.minimumSupported())),
        mockContextParser()
    );
    static NamedXContentRegistry.Entry currentVersionEntries = new NamedXContentRegistry.Entry(
        Integer.class,
        new ParseField("name2").forRestApiVersion(RestApiVersion.onOrAfter(RestApiVersion.minimumSupported())),
        mockContextParser()
    );

    public static class TestRestCompatibility1 extends Plugin {
        @Override
        public List<NamedXContentRegistry.Entry> getNamedXContent() {
            return List.of(compatibleEntries);
        }
    }

    public static class TestRestCompatibility2 extends Plugin {
        @Override
        public List<NamedXContentRegistry.Entry> getNamedXContent() {
            return List.of(currentVersionEntries);
        }
    }

    // This test shows an example on how multiple plugins register namedXContent entries
    public void testLoadingMultipleRestCompatibilityPlugins() throws IOException {
        Settings.Builder settings = baseSettings();
        List<Class<? extends Plugin>> plugins = basePlugins();
        plugins.add(TestRestCompatibility1.class);
        plugins.add(TestRestCompatibility2.class);

        try (Node node = new MockNode(settings.build(), plugins)) {
            final NamedXContentRegistry namedXContentRegistry = node.namedXContentRegistry;
            RestRequest compatibleRequest = request(namedXContentRegistry, RestApiVersion.minimumSupported());
            try (XContentParser p = compatibleRequest.contentParser()) {
                NamedXContentRegistry.Entry field = namedXContentRegistry.lookupParser(Integer.class, "name", p);
                assertTrue(RestApiVersion.minimumSupported().matches(field.restApiCompatibility));

                field = namedXContentRegistry.lookupParser(Integer.class, "name2", p);
                assertTrue(RestApiVersion.current().matches(field.restApiCompatibility));
            }

            RestRequest currentRequest = request(namedXContentRegistry, RestApiVersion.current());
            try (XContentParser p = currentRequest.contentParser()) {
                NamedXContentRegistry.Entry field = namedXContentRegistry.lookupParser(Integer.class, "name2", p);
                assertTrue(RestApiVersion.minimumSupported().matches(field.restApiCompatibility));

                expectThrows(NamedObjectNotFoundException.class, () -> namedXContentRegistry.lookupParser(Integer.class, "name", p));
            }
        }
    }

    static class AdditionalSettingsPlugin1 extends Plugin {
        @Override
        public Settings additionalSettings() {
            return Settings.builder()
                .put("foo.bar", "1")
                .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.MMAPFS.getSettingsKey())
                .build();
        }
    }

    static class AdditionalSettingsPlugin2 extends Plugin {
        @Override
        public Settings additionalSettings() {
            return Settings.builder().put("foo.bar", "2").build();
        }
    }

    public void testAdditionalSettings() {
        Map<String, Plugin> pluginMap = Map.of(AdditionalSettingsPlugin1.class.getName(), new AdditionalSettingsPlugin1());
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .put("my.setting", "test")
            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.NIOFS.getSettingsKey())
            .build();

        Settings newSettings = Node.mergePluginSettings(pluginMap, settings);
        assertEquals("test", newSettings.get("my.setting")); // previous settings still exist
        assertEquals("1", newSettings.get("foo.bar")); // added setting exists
        // does not override pre existing settings
        assertEquals(IndexModule.Type.NIOFS.getSettingsKey(), newSettings.get(IndexModule.INDEX_STORE_TYPE_SETTING.getKey()));
    }

    public void testAdditionalSettingsClash() {
        Map<String, Plugin> pluginMap = Map.of(
            AdditionalSettingsPlugin1.class.getName(),
            new AdditionalSettingsPlugin1(),
            AdditionalSettingsPlugin2.class.getName(),
            new AdditionalSettingsPlugin2()
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> Node.mergePluginSettings(pluginMap, Settings.EMPTY)
        );
        String msg = e.getMessage();
        assertTrue(msg, msg.contains("Cannot have additional setting [foo.bar]"));
        assertTrue(msg, msg.contains("plugin [" + AdditionalSettingsPlugin1.class.getName()));
        assertTrue(msg, msg.contains("plugin [" + AdditionalSettingsPlugin2.class.getName()));
    }

    public void testPluginComponentInterfaceBinding() throws IOException, NodeValidationException {
        List<Class<? extends Plugin>> plugins = basePlugins();
        plugins.add(MockPluginWithAltImpl.class);
        try (Node node = new MockNode(baseSettings().build(), plugins)) {
            MockPluginWithAltImpl.MyInterface myInterface = node.injector().getInstance(MockPluginWithAltImpl.MyInterface.class);
            MockPluginWithAltImpl plugin = node.getPluginsService().filterPlugins(MockPluginWithAltImpl.class).findFirst().get();
            if (plugin.getRandomBool()) {
                assertThat(myInterface, instanceOf(MockPluginWithAltImpl.Foo.class));
                assertThat(myInterface.get(), equalTo("foo"));
            } else {
                assertThat(myInterface, instanceOf(MockPluginWithAltImpl.Bar.class));
                assertThat(myInterface.get(), equalTo("bar"));
            }
            node.start();
            assertTrue(MockPluginWithAltImpl.startCalled);
        }
        assertTrue(MockPluginWithAltImpl.stopCalled);
        assertTrue(MockPluginWithAltImpl.closeCalled);
    }

    private RestRequest request(NamedXContentRegistry namedXContentRegistry, RestApiVersion restApiVersion) throws IOException {
        String mediaType = XContentType.VND_JSON.toParsedMediaType()
            .responseContentTypeHeader(Map.of(MediaType.COMPATIBLE_WITH_PARAMETER_NAME, String.valueOf(restApiVersion.major)));
        List<String> mediaTypeList = Collections.singletonList(mediaType);
        XContentBuilder b = XContentBuilder.builder(XContentType.JSON.xContent()).startObject().endObject();

        return new FakeRestRequest.Builder(namedXContentRegistry).withContent(
            BytesReference.bytes(b),
            RestRequest.parseContentType(mediaTypeList)
        ).withPath("/foo").withHeaders(Map.of("Content-Type", mediaTypeList, "Accept", mediaTypeList)).build();
    }

    private static class BaseTestClusterCoordinationPlugin extends Plugin implements ClusterCoordinationPlugin {

        public PersistedClusterStateService persistedClusterStateService;

        @Override
        public Optional<PersistedClusterStateServiceFactory> getPersistedClusterStateServiceFactory() {
            return Optional.of(
                (
                    nodeEnvironment,
                    namedXContentRegistry,
                    clusterSettings,
                    threadPool,
                    compatibilityVersions) -> persistedClusterStateService = new PersistedClusterStateService(
                        nodeEnvironment,
                        namedXContentRegistry,
                        clusterSettings,
                        threadPool.relativeTimeInMillisSupplier()
                    )
            );
        }
    }

    public static class TestClusterCoordinationPlugin1 extends BaseTestClusterCoordinationPlugin {}

    public static class TestClusterCoordinationPlugin2 extends BaseTestClusterCoordinationPlugin {}

    public void testPluggablePersistedClusterStateServiceValidation() throws IOException {
        assertThat(
            expectThrows(
                IllegalStateException.class,
                () -> new MockNode(
                    baseSettings().build(),
                    List.of(TestClusterCoordinationPlugin1.class, TestClusterCoordinationPlugin2.class, getTestTransportPlugin())
                )
            ).getMessage(),
            containsString("A single " + ClusterCoordinationPlugin.PersistedClusterStateServiceFactory.class.getName() + " was expected")
        );

        try (Node node = new MockNode(baseSettings().build(), List.of(TestClusterCoordinationPlugin1.class, getTestTransportPlugin()))) {

            for (final var plugin : node.getPluginsService().filterPlugins(BaseTestClusterCoordinationPlugin.class).toList()) {
                assertSame(
                    Objects.requireNonNull(plugin.persistedClusterStateService),
                    node.injector().getInstance(PersistedClusterStateService.class)
                );
            }
        }
    }
}
