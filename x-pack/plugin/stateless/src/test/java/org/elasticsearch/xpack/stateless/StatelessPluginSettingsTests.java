/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class StatelessPluginSettingsTests extends ESTestCase {

    public void testDisabledByDefault() {
        assertThat(StatelessPlugin.STATELESS_ENABLED.get(Settings.EMPTY), is(false));
    }

    public void testStatelessDefaultSharedCachedSize() {
        StatelessPlugin searchNodeStateless = new TestUtils.StatelessPluginWithTrialLicense(
            Settings.builder()
                .put(StatelessPlugin.STATELESS_ENABLED.getKey(), true)
                .put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.SEARCH_ROLE.roleName())
                .build()
        );
        Settings searchNodeSettings = searchNodeStateless.additionalSettings();
        assertThat(searchNodeSettings.get(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey()), equalTo("90%"));
        assertThat(searchNodeSettings.get(SharedBlobCacheService.SHARED_CACHE_SIZE_MAX_HEADROOM_SETTING.getKey()), equalTo("250GB"));

        StatelessPlugin searchNodeStatelessWithManualSetting = new TestUtils.StatelessPluginWithTrialLicense(
            Settings.builder()
                .put(StatelessPlugin.STATELESS_ENABLED.getKey(), true)
                .put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.SEARCH_ROLE.roleName())
                .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), "65%")
                .build()
        );
        Settings searchNodeWithManualSettingSettings = searchNodeStatelessWithManualSetting.additionalSettings();
        assertFalse(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.exists(searchNodeWithManualSettingSettings));
        assertFalse(SharedBlobCacheService.SHARED_CACHE_SIZE_MAX_HEADROOM_SETTING.exists(searchNodeWithManualSettingSettings));

        StatelessPlugin indexNodeStateless = new TestUtils.StatelessPluginWithTrialLicense(
            Settings.builder()
                .put(StatelessPlugin.STATELESS_ENABLED.getKey(), true)
                .put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.INDEX_ROLE.roleName())
                .build()
        );
        Settings indexNodeSettings = indexNodeStateless.additionalSettings();
        assertThat(indexNodeSettings.get(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey()), equalTo("50%"));
        assertThat(indexNodeSettings.get(SharedBlobCacheService.SHARED_CACHE_SIZE_MAX_HEADROOM_SETTING.getKey()), equalTo("-1"));
    }

    public void testDiskUsageBalanceFactorSettingIsZeroForStateless() {
        var statelessNode = new TestUtils.StatelessPluginWithTrialLicense(
            Settings.builder()
                .put(StatelessPlugin.STATELESS_ENABLED.getKey(), true)
                .put(
                    NodeRoleSettings.NODE_ROLES_SETTING.getKey(),
                    randomFrom(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.INDEX_ROLE, DiscoveryNodeRole.SEARCH_ROLE).roleName()
                )
                .build()
        );
        assertThat(
            statelessNode.additionalSettings().get(BalancedShardsAllocator.DISK_USAGE_BALANCE_FACTOR_SETTING.getKey()),
            equalTo("0")
        );
    }
}
