/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.discovery;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.coordination.LeaderHeartbeatService;
import org.elasticsearch.cluster.coordination.PreVoteCollector;
import org.elasticsearch.cluster.coordination.Reconfigurator;
import org.elasticsearch.cluster.coordination.StatefulPreVoteCollector;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.service.ClusterApplier;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.gateway.GatewayMetaState;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.plugins.ClusterCoordinationPlugin;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class DiscoveryModuleTests extends ESTestCase {

    private TransportService transportService;
    private NamedWriteableRegistry namedWriteableRegistry;
    private MasterService masterService;
    private ClusterApplier clusterApplier;
    private ClusterSettings clusterSettings;
    private GatewayMetaState gatewayMetaState;

    public interface DummyHostsProviderPlugin extends DiscoveryPlugin {
        Map<String, Supplier<SeedHostsProvider>> impl();

        @Override
        default Map<String, Supplier<SeedHostsProvider>> getSeedHostProviders(
            TransportService transportService,
            NetworkService networkService
        ) {
            return impl();
        }
    }

    @Before
    public void setupDummyServices() {
        transportService = MockTransportService.createNewService(
            Settings.EMPTY,
            VersionInformation.CURRENT,
            TransportVersion.current(),
            mock(ThreadPool.class),
            null
        );
        masterService = mock(MasterService.class);
        namedWriteableRegistry = new NamedWriteableRegistry(Collections.emptyList());
        clusterApplier = mock(ClusterApplier.class);
        clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        gatewayMetaState = mock(GatewayMetaState.class);
    }

    @After
    public void clearDummyServices() throws IOException {
        IOUtils.close(transportService);
    }

    private DiscoveryModule newModule(
        Settings settings,
        List<DiscoveryPlugin> discoveryPlugins,
        List<ClusterCoordinationPlugin> clusterCoordinationPlugins
    ) {
        return new DiscoveryModule(
            settings,
            transportService,
            null,
            namedWriteableRegistry,
            null,
            masterService,
            clusterApplier,
            clusterSettings,
            discoveryPlugins,
            clusterCoordinationPlugins,
            null,
            createTempDir().toAbsolutePath(),
            gatewayMetaState,
            mock(RerouteService.class),
            null,
            new NoneCircuitBreakerService()
        );
    }

    public void testDefaults() {
        newModule(Settings.EMPTY, List.of(), List.of());
        // just checking it doesn't throw
    }

    public void testUnknownDiscovery() {
        Settings settings = Settings.builder().put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), "dne").build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> newModule(settings, List.of(), List.of()));
        assertEquals("Unknown discovery type [dne]", e.getMessage());
    }

    public void testSeedProviders() {
        Settings settings = Settings.builder().put(DiscoveryModule.DISCOVERY_SEED_PROVIDERS_SETTING.getKey(), "custom").build();
        AtomicBoolean created = new AtomicBoolean(false);
        DummyHostsProviderPlugin plugin = () -> Collections.singletonMap("custom", () -> {
            created.set(true);
            return hostsResolver -> Collections.emptyList();
        });
        newModule(settings, List.of(plugin), List.of());
        assertTrue(created.get());
    }

    public void testUnknownSeedsProvider() {
        Settings settings = Settings.builder().put(DiscoveryModule.DISCOVERY_SEED_PROVIDERS_SETTING.getKey(), "dne").build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> newModule(settings, List.of(), List.of()));
        assertEquals("Unknown seed providers [dne]", e.getMessage());
    }

    public void testDuplicateSeedsProvider() {
        DummyHostsProviderPlugin plugin1 = () -> Collections.singletonMap("dup", () -> null);
        DummyHostsProviderPlugin plugin2 = () -> Collections.singletonMap("dup", () -> null);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> newModule(Settings.EMPTY, List.of(plugin1, plugin2), List.of())
        );
        assertEquals("Cannot register seed provider [dup] twice", e.getMessage());
    }

    public void testSettingsSeedsProvider() {
        DummyHostsProviderPlugin plugin = () -> Collections.singletonMap("settings", () -> null);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> newModule(Settings.EMPTY, List.of(plugin), List.of())
        );
        assertEquals("Cannot register seed provider [settings] twice", e.getMessage());
    }

    public void testMultipleSeedsProviders() {
        AtomicBoolean created1 = new AtomicBoolean(false);
        DummyHostsProviderPlugin plugin1 = () -> Collections.singletonMap("provider1", () -> {
            created1.set(true);
            return hostsResolver -> Collections.emptyList();
        });
        AtomicBoolean created2 = new AtomicBoolean(false);
        DummyHostsProviderPlugin plugin2 = () -> Collections.singletonMap("provider2", () -> {
            created2.set(true);
            return hostsResolver -> Collections.emptyList();
        });
        AtomicBoolean created3 = new AtomicBoolean(false);
        DummyHostsProviderPlugin plugin3 = () -> Collections.singletonMap("provider3", () -> {
            created3.set(true);
            return hostsResolver -> Collections.emptyList();
        });
        Settings settings = Settings.builder()
            .putList(DiscoveryModule.DISCOVERY_SEED_PROVIDERS_SETTING.getKey(), "provider1", "provider3")
            .build();
        newModule(settings, List.of(plugin1, plugin2, plugin3), List.of());
        assertTrue(created1.get());
        assertFalse(created2.get());
        assertTrue(created3.get());
    }

    public void testLazyConstructionSeedsProvider() {
        DummyHostsProviderPlugin plugin = () -> Collections.singletonMap("custom", () -> {
            throw new AssertionError("created hosts provider which was not selected");
        });
        newModule(Settings.EMPTY, List.of(plugin), List.of());
    }

    public void testJoinValidator() {
        BiConsumer<DiscoveryNode, ClusterState> consumer = (a, b) -> {};
        DiscoveryModule module = newModule(
            Settings.builder().put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), DiscoveryModule.MULTI_NODE_DISCOVERY_TYPE).build(),
            List.of(),
            List.of(new ClusterCoordinationPlugin() {
                @Override
                public BiConsumer<DiscoveryNode, ClusterState> getJoinValidator() {
                    return consumer;
                }
            })
        );
        Coordinator coordinator = module.getCoordinator();
        Collection<BiConsumer<DiscoveryNode, ClusterState>> onJoinValidators = coordinator.getOnJoinValidators();
        assertEquals(2, onJoinValidators.size());
        assertTrue(onJoinValidators.contains(consumer));
    }

    public void testLegacyDiscoveryType() {
        newModule(
            Settings.builder()
                .put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), DiscoveryModule.LEGACY_MULTI_NODE_DISCOVERY_TYPE)
                .build(),
            List.of(),
            List.of()
        );
        assertCriticalWarnings(
            "Support for setting [discovery.type] to [zen] is deprecated and will be removed in a future version. Set this setting to "
                + "[multi-node] instead."
        );
    }

    public void testRejectsMultipleReconfigurators() {
        assertThat(
            expectThrows(
                IllegalStateException.class,
                () -> DiscoveryModule.getReconfigurator(
                    Settings.EMPTY,
                    new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                    List.of(
                        new BaseTestClusterCoordinationPlugin(),
                        new TestClusterCoordinationPlugin1(),
                        new TestClusterCoordinationPlugin2()
                    )
                )
            ).getMessage(),
            containsString("multiple reconfigurator factories found")
        );

        assertThat(
            DiscoveryModule.getReconfigurator(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                List.of(new BaseTestClusterCoordinationPlugin())
            ),
            is(BaseTestClusterCoordinationPlugin.reconfiguratorInstance)
        );
    }

    public void testRejectsMultiplePreVoteCollectorFactories() {
        assertThat(
            expectThrows(
                IllegalStateException.class,
                () -> DiscoveryModule.getPreVoteCollectorFactory(
                    List.of(new BaseTestClusterCoordinationPlugin(), new TestClusterCoordinationPlugin1() {
                        @Override
                        public Optional<ReconfiguratorFactory> getReconfiguratorFactory() {
                            return Optional.empty();
                        }
                    }, new TestClusterCoordinationPlugin2() {
                        @Override
                        public Optional<ReconfiguratorFactory> getReconfiguratorFactory() {
                            return Optional.empty();
                        }
                    })
                )
            ).getMessage(),
            containsString("multiple pre-vote collector factories found")
        );

        assertThat(
            DiscoveryModule.getPreVoteCollectorFactory(List.of(new BaseTestClusterCoordinationPlugin())),
            is(BaseTestClusterCoordinationPlugin.preVoteCollectorFactory)
        );
    }

    public void testRejectsMultipleLeaderHeartbeatServices() {
        assertThat(
            expectThrows(
                IllegalStateException.class,
                () -> DiscoveryModule.getLeaderHeartbeatService(
                    Settings.EMPTY,
                    List.of(new BaseTestClusterCoordinationPlugin(), new TestClusterCoordinationPlugin1() {
                        @Override
                        public Optional<ReconfiguratorFactory> getReconfiguratorFactory() {
                            return Optional.empty();
                        }

                        @Override
                        public Optional<PreVoteCollector.Factory> getPreVoteCollectorFactory() {
                            return Optional.empty();
                        }
                    }, new TestClusterCoordinationPlugin2() {
                        @Override
                        public Optional<ReconfiguratorFactory> getReconfiguratorFactory() {
                            return Optional.empty();
                        }

                        @Override
                        public Optional<PreVoteCollector.Factory> getPreVoteCollectorFactory() {
                            return Optional.empty();
                        }
                    })
                )
            ).getMessage(),
            containsString("multiple leader heart beat service factories found")
        );

        assertThat(
            DiscoveryModule.getLeaderHeartbeatService(Settings.EMPTY, List.of(new BaseTestClusterCoordinationPlugin())),
            is(BaseTestClusterCoordinationPlugin.leaderHeartbeatServiceInstance)
        );
    }

    private static class BaseTestClusterCoordinationPlugin extends Plugin implements ClusterCoordinationPlugin {
        static Reconfigurator reconfiguratorInstance;
        static PreVoteCollector.Factory preVoteCollectorFactory = StatefulPreVoteCollector::new;
        static LeaderHeartbeatService leaderHeartbeatServiceInstance = LeaderHeartbeatService.NO_OP;

        @Override
        public Optional<ReconfiguratorFactory> getReconfiguratorFactory() {
            return Optional.of((settings, clusterSettings) -> reconfiguratorInstance = new Reconfigurator(settings, clusterSettings));
        }

        @Override
        public Optional<PreVoteCollector.Factory> getPreVoteCollectorFactory() {
            return Optional.of(preVoteCollectorFactory);
        }

        @Override
        public Optional<LeaderHeartbeatService> getLeaderHeartbeatService(Settings settings) {
            return Optional.of(leaderHeartbeatServiceInstance);
        }
    }

    public static class TestClusterCoordinationPlugin1 extends BaseTestClusterCoordinationPlugin {}

    public static class TestClusterCoordinationPlugin2 extends BaseTestClusterCoordinationPlugin {}
}
