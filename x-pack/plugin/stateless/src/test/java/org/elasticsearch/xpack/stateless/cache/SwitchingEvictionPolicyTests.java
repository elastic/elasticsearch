/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.blobcache.shared.DefaultEvictionPolicy;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.TestUtils;

import static org.elasticsearch.xpack.stateless.cache.PinnedWindowEvictionPolicy.PINNED_WINDOW_DURATION_SETTING;
import static org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SEARCH_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class SwitchingEvictionPolicyTests extends ESTestCase {

    public void testOldPolicyIsClosedOnPolicySwitch() {
        Settings settings = Settings.builder()
            .put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.SEARCH_ROLE.roleName())
            .put(STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SEARCH_SETTING.getKey(), StatelessCacheEvictionPolicyType.PINNED_WINDOW)
            .build();
        final var clusterSettings = createClusterSettings(settings);
        final var switchingPolicy = createSwitchingEvictionPolicy(clusterSettings, settings);

        assertThat(switchingPolicy.getDelegate(), instanceOf(PinnedWindowEvictionPolicy.class));
        final var oldDelegate = (PinnedWindowEvictionPolicy) switchingPolicy.getDelegate();
        final var initialPinnedDuration = oldDelegate.getPinnedWindowDuration();
        assertThat(initialPinnedDuration, equalTo(PINNED_WINDOW_DURATION_SETTING.get(Settings.EMPTY)));

        // Switch to a different policy — the old PinnedWindowEvictionPolicy should be closed
        settings = Settings.builder()
            .put(settings)
            .put(STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SEARCH_SETTING.getKey(), StatelessCacheEvictionPolicyType.ALWAYS)
            .build();
        clusterSettings.applySettings(settings);
        assertThat(switchingPolicy.getDelegate(), instanceOf(DefaultEvictionPolicy.class));

        // The old policy's PINNED_WINDOW_DURATION_SETTING watcher was removed by close(), so further
        // changes to the setting do not update it
        TimeValue expectedDuration = TimeValue.timeValueHours(between(24, 96));
        settings = Settings.builder().put(settings).put(PINNED_WINDOW_DURATION_SETTING.getKey(), expectedDuration).build();
        clusterSettings.applySettings(settings);
        assertThat(oldDelegate.getPinnedWindowDuration(), equalTo(initialPinnedDuration));

        // Switch back to pinned window policy again, it picks up the latest setting value
        settings = Settings.builder()
            .put(settings)
            .put(STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SEARCH_SETTING.getKey(), StatelessCacheEvictionPolicyType.PINNED_WINDOW)
            .build();
        clusterSettings.applySettings(settings);
        assertThat(switchingPolicy.getDelegate(), instanceOf(PinnedWindowEvictionPolicy.class));
        final var newDelegate = (PinnedWindowEvictionPolicy) switchingPolicy.getDelegate();
        assertThat(newDelegate.getPinnedWindowDuration(), equalTo(expectedDuration));

        // Dynamically updatable again
        expectedDuration = TimeValue.timeValueHours(between(100, 200));
        settings = Settings.builder().put(settings).put(PINNED_WINDOW_DURATION_SETTING.getKey(), expectedDuration).build();
        clusterSettings.applySettings(settings);
        assertThat(newDelegate.getPinnedWindowDuration(), equalTo(expectedDuration));

        // The old delegate is still closed and does not update
        assertThat(oldDelegate.getPinnedWindowDuration(), equalTo(initialPinnedDuration));
    }

    public void testCloseUnregistersSettingsUpdaterAndClosesDelegate() {
        Settings settings = Settings.builder()
            .put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.SEARCH_ROLE.roleName())
            .put(STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SEARCH_SETTING.getKey(), StatelessCacheEvictionPolicyType.PINNED_WINDOW)
            .build();
        final var clusterSettings = createClusterSettings(settings);
        final var switchingPolicy = createSwitchingEvictionPolicy(clusterSettings, settings);

        assertThat(switchingPolicy.getDelegate(), instanceOf(PinnedWindowEvictionPolicy.class));
        final var delegate = (PinnedWindowEvictionPolicy) switchingPolicy.getDelegate();
        final var initialPinnedDuration = delegate.getPinnedWindowDuration();

        switchingPolicy.close();
        if (randomBoolean()) {
            switchingPolicy.close(); // close should be idempotent for the settings updater
        }

        // Policy-type setting changes must not swap in a new delegate after close
        settings = Settings.builder()
            .put(settings)
            .put(STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SEARCH_SETTING.getKey(), StatelessCacheEvictionPolicyType.ALWAYS)
            .build();
        clusterSettings.applySettings(settings);
        assertThat(switchingPolicy.getDelegate(), instanceOf(PinnedWindowEvictionPolicy.class));
        assertSame(delegate, switchingPolicy.getDelegate());

        // Delegate was closed, so its duration watcher no longer updates
        settings = Settings.builder().put(settings).put(PINNED_WINDOW_DURATION_SETTING.getKey(), TimeValue.timeValueHours(48)).build();
        clusterSettings.applySettings(settings);
        assertThat(delegate.getPinnedWindowDuration(), equalTo(initialPinnedDuration));
    }

    private static SwitchingEvictionPolicy createSwitchingEvictionPolicy(ClusterSettings clusterSettings, Settings settings) {
        final var taskQueue = new DeterministicTaskQueue();
        final var threadPool = taskQueue.getThreadPool();
        final var clusterService = ClusterServiceUtils.createClusterService(threadPool, clusterSettings);
        final var indicesService = TestUtils.mockIndicesService(clusterService);
        return new SwitchingEvictionPolicy(settings, clusterService, indicesService, threadPool);
    }

    private static ClusterSettings createClusterSettings(Settings settings) {
        var settingSet = Sets.newHashSet(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settingSet.add(PINNED_WINDOW_DURATION_SETTING);
        settingSet.add(STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SEARCH_SETTING);
        return new ClusterSettings(settings, settingSet);
    }
}
