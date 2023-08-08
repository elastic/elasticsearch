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

package co.elastic.elasticsearch.stateless;

import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.test.ESTestCase;

import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class StatelessSettingsTests extends ESTestCase {

    public void testDisabledByDefault() {
        assertThat(Stateless.STATELESS_ENABLED.get(Settings.EMPTY), is(false));
    }

    public void testStatelessNotEnabled() {
        var settings = Settings.builder();
        if (randomBoolean()) {
            settings.put(Stateless.STATELESS_ENABLED.getKey(), false);
        }
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> new Stateless(settings.build()));
        assertThat(exception.getMessage(), containsString("stateless is not enabled"));
    }

    public void testNonStatelessDataRolesNotAllowed() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new Stateless(
                statelessSettings(
                    List.of(
                        randomFrom(
                            DiscoveryNodeRole.roles()
                                .stream()
                                .filter(r -> r.canContainData() && Stateless.STATELESS_ROLES.contains(r) == false)
                                .toList()
                        )
                    )
                )
            )
        );
        assertThat(exception.getMessage(), containsString("stateless does not support roles ["));
    }

    public void testStatelessDefaultSharedCachedSize() {
        Stateless searchNodeStateless = new Stateless(
            Settings.builder()
                .put(Stateless.STATELESS_ENABLED.getKey(), true)
                .put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.SEARCH_ROLE.roleName())
                .build()
        );
        Settings searchNodeSettings = searchNodeStateless.additionalSettings();
        assertThat(searchNodeSettings.get(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey()), equalTo("90%"));
        assertThat(searchNodeSettings.get(SharedBlobCacheService.SHARED_CACHE_SIZE_MAX_HEADROOM_SETTING.getKey()), equalTo("250GB"));

        Stateless searchNodeStatelessWithManualSetting = new Stateless(
            Settings.builder()
                .put(Stateless.STATELESS_ENABLED.getKey(), true)
                .put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.SEARCH_ROLE.roleName())
                .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), "65%")
                .build()
        );
        Settings searchNodeWithManualSettingSettings = searchNodeStatelessWithManualSetting.additionalSettings();
        assertFalse(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.exists(searchNodeWithManualSettingSettings));
        assertFalse(SharedBlobCacheService.SHARED_CACHE_SIZE_MAX_HEADROOM_SETTING.exists(searchNodeWithManualSettingSettings));

        Stateless indexNodeStateless = new Stateless(
            Settings.builder()
                .put(Stateless.STATELESS_ENABLED.getKey(), true)
                .put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.INDEX_ROLE.roleName())
                .build()
        );
        Settings indexNodeSettings = indexNodeStateless.additionalSettings();
        assertThat(indexNodeSettings.get(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey()), equalTo("50%"));
        assertThat(indexNodeSettings.get(SharedBlobCacheService.SHARED_CACHE_SIZE_MAX_HEADROOM_SETTING.getKey()), equalTo("-1"));
    }

    public void testDefaultDiskThresholdEnabledSetting() {
        {
            var statelessNode = new Stateless(
                Settings.builder()
                    .put(Stateless.STATELESS_ENABLED.getKey(), true)
                    .put(
                        NodeRoleSettings.NODE_ROLES_SETTING.getKey(),
                        randomFrom(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.INDEX_ROLE, DiscoveryNodeRole.SEARCH_ROLE).roleName()
                    )
                    .build()
            );
            assertThat(
                statelessNode.additionalSettings()
                    .get(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey()),
                equalTo("false")
            );
        }
        {
            var exception = expectThrows(
                IllegalArgumentException.class,
                () -> new Stateless(
                    Settings.builder()
                        .put(Stateless.STATELESS_ENABLED.getKey(), true)
                        .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
                        .build()
                )
            );
            assertThat(exception.getMessage(), equalTo("cluster.routing.allocation.disk.threshold_enabled cannot be enabled"));
        }
    }

    private static Settings statelessSettings(Collection<DiscoveryNodeRole> roles) {
        final Settings.Builder builder = Settings.builder();
        builder.put(Stateless.STATELESS_ENABLED.getKey(), true);
        if (roles != null) {
            builder.putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), roles.stream().map(DiscoveryNodeRole::roleName).toList());
        }
        return builder.build();
    }
}
