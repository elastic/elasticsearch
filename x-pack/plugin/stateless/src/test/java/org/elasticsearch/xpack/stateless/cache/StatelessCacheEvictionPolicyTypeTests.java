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
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

import java.util.Set;

import static org.elasticsearch.xpack.stateless.cache.PinnedWindowEvictionPolicy.PINNED_WINDOW_DURATION_SETTING;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class StatelessCacheEvictionPolicyTypeTests extends ESTestCase {

    public void testCreateEvictionPolicyReturnsDefaultWhenSettingDisabled() {
        Settings.Builder settingsBuilder = Settings.builder()
            .put(StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING.getKey(), false);
        if (randomBoolean()) {
            settingsBuilder.put(
                StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SETTING.getKey(),
                StatelessCacheEvictionPolicyType.INDEX_AGE
            );
        }
        EvictionPolicy<FileCacheKey> policy = StatelessCacheEvictionPolicyType.createEvictionPolicy(
            settingsBuilder.build(),
            mock(ClusterService.class)
        );
        assertThat(policy, instanceOf(DefaultEvictionPolicy.class));
    }

    public void testCreateEvictionPolicyReturnsDefaultWhenBoostEnabledButPolicyAlways() {
        Settings settings = Settings.builder()
            .put(StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING.getKey(), true)
            .put(
                StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SETTING.getKey(),
                StatelessCacheEvictionPolicyType.ALWAYS
            )
            .build();
        EvictionPolicy<FileCacheKey> policy = StatelessCacheEvictionPolicyType.createEvictionPolicy(settings, mock(ClusterService.class));
        assertThat(policy, instanceOf(DefaultEvictionPolicy.class));
    }

    public void testCreateEvictionPolicyReturnsIndexAgePolicyWhenExplicitlyConfigured() {
        Settings settings = Settings.builder()
            .put(StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING.getKey(), true)
            .put(
                StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SETTING.getKey(),
                StatelessCacheEvictionPolicyType.INDEX_AGE
            )
            .build();
        EvictionPolicy<FileCacheKey> policy = StatelessCacheEvictionPolicyType.createEvictionPolicy(settings, mock(ClusterService.class));
        assertThat(policy, instanceOf(IndexAgeEvictionPolicy.class));
    }

    public void testCreateEvictionPolicyReturnsPinnedWindowPolicyWhenBoostEnabledOnSearchNode() {
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        final ClusterSettings clusterSettings = createClusterSettings(Settings.EMPTY);
        final Settings settings = Settings.builder()
            .put(StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING.getKey(), true)
            .put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.SEARCH_ROLE.roleName())
            .build();
        try (var clusterService = ClusterServiceUtils.createClusterService(taskQueue.getThreadPool(), clusterSettings)) {
            EvictionPolicy<FileCacheKey> policy = StatelessCacheEvictionPolicyType.createEvictionPolicy(settings, clusterService);
            assertThat(policy, instanceOf(PinnedWindowEvictionPolicy.class));
        }
    }

    public void testCreateEvictionPolicyReturnsDefaultPolicyWhenBoostEnabledOnIndexNode() {
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        final ClusterSettings clusterSettings = createClusterSettings(Settings.EMPTY);
        final Settings settings = Settings.builder()
            .put(StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING.getKey(), true)
            .put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.INDEX_ROLE.roleName())
            .build();
        try (var clusterService = ClusterServiceUtils.createClusterService(taskQueue.getThreadPool(), clusterSettings)) {
            EvictionPolicy<FileCacheKey> policy = StatelessCacheEvictionPolicyType.createEvictionPolicy(settings, clusterService);
            assertThat(policy, instanceOf(DefaultEvictionPolicy.class));
        }
    }

    private static ClusterSettings createClusterSettings(Settings settings) {
        Set<Setting<?>> clusterSettings = Sets.newHashSet(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterSettings.add(PINNED_WINDOW_DURATION_SETTING);
        return new ClusterSettings(settings, clusterSettings);
    }
}
