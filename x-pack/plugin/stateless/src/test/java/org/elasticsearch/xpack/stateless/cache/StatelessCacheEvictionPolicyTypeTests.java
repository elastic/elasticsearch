/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.blobcache.shared.DefaultEvictionPolicy;
import org.elasticsearch.blobcache.shared.EvictionPolicy;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.TestUtils;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

import java.util.Set;

import static org.elasticsearch.xpack.stateless.cache.PinnedWindowEvictionPolicy.PINNED_WINDOW_DURATION_SETTING;
import static org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SEARCH_SETTING;
import static org.hamcrest.Matchers.instanceOf;

public class StatelessCacheEvictionPolicyTypeTests extends ESTestCase {

    public void testCreateEvictionPolicyReturnsDefaultOnIndexNode() {
        Settings.Builder settingsBuilder = Settings.builder()
            .put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.INDEX_ROLE.roleName())
            .put(StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING.getKey(), randomBoolean());
        if (randomBoolean()) {
            settingsBuilder.put(
                StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SEARCH_SETTING.getKey(),
                StatelessCacheEvictionPolicyType.INDEX_AGE
            );
        }
        final var policy = createEvictionPolicy(settingsBuilder.build());
        assertThat(policy, instanceOf(DefaultEvictionPolicy.class));
    }

    public void testCreateEvictionPolicyReturnsDefaultWhenBoostEnabledButPolicyAlwaysOnSearchNode() {
        Settings settings = Settings.builder()
            .put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.SEARCH_ROLE.roleName())
            .put(StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING.getKey(), randomBoolean())
            .put(
                StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SEARCH_SETTING.getKey(),
                StatelessCacheEvictionPolicyType.ALWAYS
            )
            .build();
        final var policy = createEvictionPolicy(settings);
        assertThat(policy, instanceOf(DefaultEvictionPolicy.class));
    }

    public void testCreateEvictionPolicyReturnsIndexAgePolicyWhenExplicitlyConfiguredOnSearchNode() {
        Settings settings = Settings.builder()
            .put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.SEARCH_ROLE.roleName())
            .put(StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING.getKey(), randomBoolean())
            .put(
                StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SEARCH_SETTING.getKey(),
                StatelessCacheEvictionPolicyType.INDEX_AGE
            )
            .build();
        final var policy = createEvictionPolicy(settings);
        assertThat(policy, instanceOf(IndexAgeEvictionPolicy.class));
    }

    public void testCreateEvictionPolicyReturnsPinnedWindowPolicyWhenBoostEnabledOnSearchNode() {
        final Settings settings = Settings.builder()
            .put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.SEARCH_ROLE.roleName())
            .put(StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING.getKey(), true)
            .build();
        final var policy = createEvictionPolicy(settings);
        assertThat(policy, instanceOf(PinnedWindowEvictionPolicy.class));
    }

    public void testEvictionPolicyCanBeChangedDynamicallyOnSearchNode() {
        final var settings = Settings.builder()
            .put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.SEARCH_ROLE.roleName())
            .put(STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SEARCH_SETTING.getKey(), StatelessCacheEvictionPolicyType.ALWAYS)
            .build();
        final var clusterService = createClusterService(settings);
        final var switchingPolicy = new SwitchingEvictionPolicy(
            settings,
            clusterService,
            TestUtils.mockIndicesService(clusterService),
            clusterService.threadPool()
        );

        assertThat(switchingPolicy.getDelegate(), instanceOf(DefaultEvictionPolicy.class));

        clusterService.getClusterSettings()
            .applySettings(
                Settings.builder()
                    .put(
                        STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SEARCH_SETTING.getKey(),
                        StatelessCacheEvictionPolicyType.INDEX_AGE
                    )
                    .build()
            );

        assertThat(switchingPolicy.getDelegate(), instanceOf(IndexAgeEvictionPolicy.class));
    }

    private static EvictionPolicy<FileCacheKey> createEvictionPolicy(Settings settings) {
        final var clusterService = createClusterService(settings);
        return StatelessCacheEvictionPolicyType.createEvictionPolicy(
            settings,
            clusterService,
            TestUtils.mockIndicesService(clusterService),
            clusterService.threadPool()
        );
    }

    private static ClusterService createClusterService(Settings settings) {
        final var deterministicTaskQueue = new DeterministicTaskQueue();
        return ClusterServiceUtils.createClusterService(deterministicTaskQueue.getThreadPool(), createClusterSettings(settings));
    }

    private static ClusterSettings createClusterSettings(Settings settings) {
        Set<Setting<?>> clusterSettings = Sets.newHashSet(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterSettings.add(PINNED_WINDOW_DURATION_SETTING);
        clusterSettings.add(STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SEARCH_SETTING);
        return new ClusterSettings(settings, clusterSettings);
    }
}
