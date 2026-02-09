/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService;

import java.util.Collection;

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

    public void testOfflineWarmingDisabledByDefault() {
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
            statelessNode.additionalSettings().get(SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_ENABLED_SETTING.getKey()),
            equalTo("false")
        );
    }

    private static Settings statelessSettings(Collection<DiscoveryNodeRole> roles) {
        final Settings.Builder builder = Settings.builder();
        builder.put(StatelessPlugin.STATELESS_ENABLED.getKey(), true);
        if (roles != null) {
            builder.putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), roles.stream().map(DiscoveryNodeRole::roleName).toList());
        }
        return builder.build();
    }
}
