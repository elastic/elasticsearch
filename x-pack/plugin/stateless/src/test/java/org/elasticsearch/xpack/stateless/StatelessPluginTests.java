/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.stateless;

import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.shared.DefaultEvictionPolicy;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.store.ThreadLocalDirectoryMetricHolder;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.cache.EvictionPolicyFactory;
import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;
import org.elasticsearch.xpack.stateless.engine.StatelessReaderHeapBreaker;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectoryMetrics;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;
import org.elasticsearch.xpack.stateless.recovery.TransportStatelessPrimaryRelocationAction;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.xpack.stateless.StatelessPlugin.STATELESS_ENABLED;
import static org.elasticsearch.xpack.stateless.StatelessPlugin.STATELESS_ROLES;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

public class StatelessPluginTests extends ESTestCase {

    private static StatelessPlugin createStatelessPlugin(Settings settings, License.OperationMode mode, boolean active) {
        final var plugin = new StatelessPlugin(settings) {
            protected XPackLicenseState getLicenseState() {
                return new XPackLicenseState(System::currentTimeMillis, new XPackLicenseStatus(mode, active, null));
            }
        };
        plugin.checkLicense();
        return plugin;
    }

    private static StatelessPlugin createStatelessPlugin(Settings settings) {
        return createStatelessPlugin(settings, randomBoolean() ? License.OperationMode.ENTERPRISE : License.OperationMode.TRIAL, true);
    }

    public void testValidLicense() throws Exception {
        final var settings = Settings.builder().put(STATELESS_ENABLED.getKey(), true).build();
        final var licenseActive = randomBoolean();
        final Runnable runnable = () -> createStatelessPlugin(
            settings,
            randomBoolean() ? License.OperationMode.ENTERPRISE : License.OperationMode.TRIAL,
            licenseActive
        );
        if (licenseActive == false) {
            expectThrows(IllegalStateException.class, runnable::run);
        } else {
            runnable.run();
        }
    }

    public void testInvalidLicense() throws Exception {
        final var settings = Settings.builder().put(STATELESS_ENABLED.getKey(), true).build();
        final License.OperationMode invalidMode = randomFrom(
            License.OperationMode.PLATINUM,
            License.OperationMode.GOLD,
            License.OperationMode.STANDARD,
            License.OperationMode.BASIC
        );
        final Runnable runnable = () -> createStatelessPlugin(settings, invalidMode, randomBoolean());
        expectThrows(IllegalStateException.class, runnable::run);
    }

    public void testSettingsWithValidStatelessRole() throws Exception {
        final var nodeSettings = Settings.builder()
            .put(STATELESS_ENABLED.getKey(), true)
            .putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), List.of(randomFrom(STATELESS_ROLES).roleName()))
            .build();
        createStatelessPlugin(nodeSettings);
    }

    public void testSettingsWithBothIndexAndSearchRolesFail() throws Exception {
        final var nodeSettings = Settings.builder()
            .put(STATELESS_ENABLED.getKey(), true)
            .putList(
                NodeRoleSettings.NODE_ROLES_SETTING.getKey(),
                List.of(DiscoveryNodeRole.INDEX_ROLE.roleName(), DiscoveryNodeRole.SEARCH_ROLE.roleName())
            )
            .build();
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> createStatelessPlugin(nodeSettings));
        assertThat(ex.getMessage(), containsString("does not support a node with more than 1 role of"));
    }

    public void testSettingsWithNonStatelessRoleAndStatelessEnabledFail() throws Exception {
        final var nonStatelessRoles = DiscoveryNodeRole.roles()
            .stream()
            .filter(r -> r.canContainData() && STATELESS_ROLES.contains(r) == false)
            .collect(Collectors.toSet());
        final var nodeSettings = Settings.builder()
            .put(STATELESS_ENABLED.getKey(), true)
            .putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), List.of(randomFrom(nonStatelessRoles).roleName()))
            .build();
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> createStatelessPlugin(nodeSettings));
        assertThat(ex.getMessage(), containsString("does not support node roles"));
    }

    public void testSettingsWithDefaultDiskThresholdEnabled() throws Exception {
        final var nodeSettings = Settings.builder()
            .put(STATELESS_ENABLED.getKey(), true)
            .putList(
                NodeRoleSettings.NODE_ROLES_SETTING.getKey(),
                randomFrom(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.INDEX_ROLE, DiscoveryNodeRole.SEARCH_ROLE).roleName()
            )
            .build();
        final var plugin = createStatelessPlugin(nodeSettings);
        assertThat(
            plugin.additionalSettings().get(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey()),
            equalTo("false")
        );

        final var nodeInvalidSettings = Settings.builder()
            .put(STATELESS_ENABLED.getKey(), true)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
            .build();
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> createStatelessPlugin(nodeInvalidSettings));
        assertThat(ex.getMessage(), containsString("does not support cluster.routing.allocation.disk.threshold_enabled"));
    }

    public void testReaderHeapBreakerWiring() throws Exception {
        final var settings = Settings.builder()
            .put(STATELESS_ENABLED.getKey(), true)
            .putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), List.of(DiscoveryNodeRole.SEARCH_ROLE.roleName()))
            .build();
        final var plugin = createStatelessPlugin(settings);

        // Setting must be registered so cluster settings recognises it.
        assertTrue(
            "LIMIT_SETTING must be advertised by getSettings()",
            plugin.getSettings().contains(StatelessReaderHeapBreaker.LIMIT_SETTING)
        );

        // Default is -1 (no enforcement).
        assertEquals(-1L, StatelessReaderHeapBreaker.LIMIT_SETTING.get(Settings.EMPTY).getBytes());

        // CircuitBreakerPlugin contract: getCircuitBreaker produces the right BreakerSettings.
        var breakerSettings = plugin.getCircuitBreaker(Settings.EMPTY);
        assertEquals(StatelessReaderHeapBreaker.NAME, breakerSettings.getName());
        assertEquals(-1L, breakerSettings.getLimit());
        assertEquals(CircuitBreaker.Type.MEMORY, breakerSettings.getType());
        assertEquals(CircuitBreaker.Durability.TRANSIENT, breakerSettings.getDurability());

        // setCircuitBreaker must accept a breaker whose name matches; an unrelated breaker would trip the assert.
        plugin.setCircuitBreaker(new NoopCircuitBreaker(StatelessReaderHeapBreaker.NAME));
    }

    public void testDataStreamLifecycleSettings() throws Exception {
        final var nodeSettings = Settings.builder()
            .put(STATELESS_ENABLED.getKey(), true)
            .putList(
                NodeRoleSettings.NODE_ROLES_SETTING.getKey(),
                randomFrom(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.INDEX_ROLE, DiscoveryNodeRole.SEARCH_ROLE).roleName()
            )
            .build();
        final var plugin = createStatelessPlugin(nodeSettings);
        assertThat(plugin.additionalSettings().get(StatelessPlugin.DATA_STREAMS_LIFECYCLE_ONLY_MODE.getKey()), equalTo("true"));
        assertThat(plugin.additionalSettings().get(StatelessPlugin.FAILURE_STORE_REFRESH_INTERVAL_SETTING.getKey()), equalTo("30s"));

        final var nodeInvalidSettings = Settings.builder()
            .put(STATELESS_ENABLED.getKey(), true)
            .put(StatelessPlugin.DATA_STREAMS_LIFECYCLE_ONLY_MODE.getKey(), false)
            .build();
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> createStatelessPlugin(nodeInvalidSettings));
        assertThat(ex.getMessage(), containsString("does not support setting data_streams.lifecycle_only.mode to false"));
    }

    public void testIdLookupPrewarmEnabledSettingIsRegistered() {
        final var plugin = createStatelessPlugin(Settings.builder().put(STATELESS_ENABLED.getKey(), true).build());
        assertThat(plugin.getSettings(), hasItem(TransportStatelessPrimaryRelocationAction.ID_LOOKUP_PREWARM_MAX_SEGMENTS_SETTING));
    }

    public void testEvictionPolicyFactoryIsInstalledOnTheCache() throws IOException {
        final long regionSize = SharedBytes.PAGE_SIZE;
        final var settings = Settings.builder()
            .put(STATELESS_ENABLED.getKey(), true)
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.SEARCH_ROLE.roleName())
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize).getStringRep())
            .put("path.home", createTempDir())
            .build();
        final var marker = new DefaultEvictionPolicy<FileCacheKey>();
        final var taskQueue = new DeterministicTaskQueue();
        final var clusterService = TestUtils.mockClusterService(settings);
        final var plugin = new StatelessPlugin(settings) {
            @Override
            protected XPackLicenseState getLicenseState() {
                return new XPackLicenseState(System::currentTimeMillis, new XPackLicenseStatus(License.OperationMode.TRIAL, true, null));
            }

            StatelessSharedBlobCacheService openCache(NodeEnvironment environment) {
                return createSharedBlobCacheService(
                    environment,
                    settings,
                    taskQueue.getThreadPool(),
                    new BlobCacheMetrics(MeterRegistry.NOOP, TestUtils.NOOP_TIME_PROVIDER),
                    clusterService,
                    TestUtils.mockIndicesService(clusterService),
                    new ThreadLocalDirectoryMetricHolder<>(BlobStoreCacheDirectoryMetrics::new)
                );
            }
        };
        plugin.checkLicense();
        plugin.loadExtensions(evictionPolicyLoader((nodeSettings, unusedClusterService, indicesService, timeProvider) -> marker));
        try (
            var environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = plugin.openCache(environment)
        ) {
            assertSame(marker, cacheService.getEvictionPolicy());
        }
    }

    public void testDuplicateEvictionPolicyFactoryIsRejected() {
        final var plugin = createStatelessPlugin(Settings.builder().put(STATELESS_ENABLED.getKey(), true).build());
        final EvictionPolicyFactory factory = (nodeSettings, clusterService, indicesService, timeProvider) -> {
            throw new AssertionError("factory should not create a policy");
        };
        final var e = expectThrows(IllegalStateException.class, () -> plugin.loadExtensions(evictionPolicyLoader(factory, factory)));
        assertThat(e.getMessage(), containsString(EvictionPolicyFactory.class.getName()));
    }

    private static ExtensiblePlugin.ExtensionLoader evictionPolicyLoader(EvictionPolicyFactory... factories) {
        return new ExtensiblePlugin.ExtensionLoader() {
            @Override
            public <T> List<T> loadExtensions(Class<T> extensionPointType) {
                if (extensionPointType == EvictionPolicyFactory.class) {
                    return List.of(factories).stream().map(extensionPointType::cast).toList();
                }
                return List.of();
            }
        };
    }

}
