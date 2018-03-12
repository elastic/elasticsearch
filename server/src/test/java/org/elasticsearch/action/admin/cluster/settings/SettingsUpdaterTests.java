/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.admin.cluster.settings;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.settings.AbstractScopedSettings.ARCHIVED_SETTINGS_PREFIX;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

public class SettingsUpdaterTests extends ESTestCase {


    public void testUpdateSetting() {
        AtomicReference<Float> index = new AtomicReference<>();
        AtomicReference<Float> shard = new AtomicReference<>();
        ClusterState.Builder builder = ClusterState.builder(new ClusterName("foo"));
        ClusterSettings settingsService = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settingsService.addSettingsUpdateConsumer(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING, index::set);
        settingsService.addSettingsUpdateConsumer(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING, shard::set);
        SettingsUpdater updater = new SettingsUpdater(settingsService);
        MetaData.Builder metaData = MetaData.builder()
            .persistentSettings(Settings.builder().put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 1.5)
                .put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 2.5).build())
            .transientSettings(Settings.builder().put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 3.5)
                .put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 4.5).build());
        ClusterState build = builder.metaData(metaData).build();
        ClusterState clusterState = updater.updateSettings(build, Settings.builder().put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 0.5).build(),
            Settings.builder().put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 0.4).build(), logger);
        assertNotSame(clusterState, build);
        assertEquals(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.get(clusterState.metaData().persistentSettings()), 0.4, 0.1);
        assertEquals(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.get(clusterState.metaData().persistentSettings()), 2.5, 0.1);
        assertEquals(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.get(clusterState.metaData().transientSettings()), 0.5, 0.1);
        assertEquals(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.get(clusterState.metaData().transientSettings()), 4.5, 0.1);

        clusterState = updater.updateSettings(clusterState, Settings.builder().putNull("cluster.routing.*").build(),
            Settings.EMPTY, logger);
        assertEquals(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.get(clusterState.metaData().persistentSettings()), 0.4, 0.1);
        assertEquals(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.get(clusterState.metaData().persistentSettings()), 2.5, 0.1);
        assertFalse(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.exists(clusterState.metaData().transientSettings()));
        assertFalse(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.exists(clusterState.metaData().transientSettings()));

        clusterState = updater.updateSettings(clusterState,
            Settings.EMPTY,  Settings.builder().putNull("cluster.routing.*").put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 10.0).build(), logger);

        assertEquals(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.get(clusterState.metaData().persistentSettings()), 10.0, 0.1);
        assertFalse(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.exists(clusterState.metaData().persistentSettings()));
        assertFalse(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.exists(clusterState.metaData().transientSettings()));
        assertFalse(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.exists(clusterState.metaData().transientSettings()));
        assertNull("updater only does a dryRun", index.get());
        assertNull("updater only does a dryRun", shard.get());
    }

    public void testAllOrNothing() {
        ClusterState.Builder builder = ClusterState.builder(new ClusterName("foo"));
        ClusterSettings settingsService = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        AtomicReference<Float> index = new AtomicReference<>();
        AtomicReference<Float> shard = new AtomicReference<>();
        settingsService.addSettingsUpdateConsumer(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING, index::set);
        settingsService.addSettingsUpdateConsumer(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING, shard::set);
        SettingsUpdater updater = new SettingsUpdater(settingsService);
        MetaData.Builder metaData = MetaData.builder()
            .persistentSettings(Settings.builder().put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 1.5)
                .put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 2.5).build())
            .transientSettings(Settings.builder().put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 3.5)
                .put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 4.5).build());
        ClusterState build = builder.metaData(metaData).build();

        try {
            updater.updateSettings(build, Settings.builder().put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), "not a float").build(),
                Settings.builder().put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), "not a float").put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 1.0f).build(), logger);
            fail("all or nothing");
        } catch (IllegalArgumentException ex) {
            logger.info("", ex);
            assertEquals("Failed to parse value [not a float] for setting [cluster.routing.allocation.balance.index]", ex.getMessage());
        }
        assertNull("updater only does a dryRun", index.get());
        assertNull("updater only does a dryRun", shard.get());
    }

    public void testClusterBlock() {
        ClusterState.Builder builder = ClusterState.builder(new ClusterName("foo"));
        ClusterSettings settingsService = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        AtomicReference<Float> index = new AtomicReference<>();
        AtomicReference<Float> shard = new AtomicReference<>();
        settingsService.addSettingsUpdateConsumer(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING, index::set);
        settingsService.addSettingsUpdateConsumer(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING, shard::set);
        SettingsUpdater updater = new SettingsUpdater(settingsService);
        MetaData.Builder metaData = MetaData.builder()
            .persistentSettings(Settings.builder().put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 1.5)
                .put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 2.5).build())
            .transientSettings(Settings.builder().put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 3.5)
                .put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 4.5).build());
        ClusterState build = builder.metaData(metaData).build();

        ClusterState clusterState = updater.updateSettings(build, Settings.builder().put(MetaData.SETTING_READ_ONLY_SETTING.getKey(), true).build(),
            Settings.builder().put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 1.6).put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 1.0f).build(), logger);
        assertEquals(clusterState.blocks().global().size(), 1);
        assertEquals(clusterState.blocks().global().iterator().next(), MetaData.CLUSTER_READ_ONLY_BLOCK);

        clusterState = updater.updateSettings(build, Settings.EMPTY,
            Settings.builder().put(MetaData.SETTING_READ_ONLY_SETTING.getKey(), false).build(), logger);
        assertEquals(clusterState.blocks().global().size(), 0);


        clusterState = updater.updateSettings(build, Settings.builder().put(MetaData.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), true).build(),
            Settings.builder().put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 1.6).put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 1.0f).build(), logger);
        assertEquals(clusterState.blocks().global().size(), 1);
        assertEquals(clusterState.blocks().global().iterator().next(), MetaData.CLUSTER_READ_ONLY_ALLOW_DELETE_BLOCK);
        clusterState = updater.updateSettings(build, Settings.EMPTY,
            Settings.builder().put(MetaData.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), false).build(), logger);
        assertEquals(clusterState.blocks().global().size(), 0);

    }

    public void testDeprecationLogging() {
        Setting<String> deprecatedSetting =
                Setting.simpleString("deprecated.setting", Property.Dynamic, Property.NodeScope, Property.Deprecated);
        final Settings settings = Settings.builder().put("deprecated.setting", "foo").build();
        final Set<Setting<?>> settingsSet =
                Stream.concat(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(), Stream.of(deprecatedSetting)).collect(Collectors.toSet());
        final ClusterSettings clusterSettings = new ClusterSettings(settings, settingsSet);
        clusterSettings.addSettingsUpdateConsumer(deprecatedSetting, s -> {});
        final SettingsUpdater settingsUpdater = new SettingsUpdater(clusterSettings);
        final ClusterState clusterState =
                ClusterState.builder(new ClusterName("foo")).metaData(MetaData.builder().persistentSettings(settings).build()).build();

        final Settings toApplyDebug = Settings.builder().put("logger.org.elasticsearch", "debug").build();
        final ClusterState afterDebug = settingsUpdater.updateSettings(clusterState, toApplyDebug, Settings.EMPTY, logger);
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { deprecatedSetting });

        final Settings toApplyUnset = Settings.builder().putNull("logger.org.elasticsearch").build();
        final ClusterState afterUnset = settingsUpdater.updateSettings(afterDebug, toApplyUnset, Settings.EMPTY, logger);
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { deprecatedSetting });

        // we also check that if no settings are changed, deprecation logging still occurs
        settingsUpdater.updateSettings(afterUnset, toApplyUnset, Settings.EMPTY, logger);
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { deprecatedSetting });
    }

    public void testUpdateWithUnknownAndSettings() {
        final int numberOfDynamicSettings = randomIntBetween(2, 8);
        final List<Setting<String>> dynamicSettings = new ArrayList<>();
        for (int i = 0; i < numberOfDynamicSettings; i++) {
            final Setting<String> dynamicSetting = Setting.simpleString("dynamic.setting" + i, Property.Dynamic, Property.NodeScope);
            dynamicSettings.add(dynamicSetting);
        }

        final Setting<String> invalidSetting = Setting.simpleString(
                "invalid.setting",
                (value, settings) -> {
                    throw new IllegalArgumentException("invalid");
                },
                Property.NodeScope);

        final Settings.Builder existingPersistentSettings = Settings.builder();
        final Settings.Builder existingTransientSettings = Settings.builder();

        if (randomBoolean()) {
            existingPersistentSettings.put("invalid.setting", "value");
        } else {
            existingTransientSettings.put("invalid.setting", "value");
        }

        if (randomBoolean()) {
            existingPersistentSettings.put("unknown.setting", "value");
        } else {
            existingTransientSettings.put("unknown.setting", "value");
        }

        final Set<Setting<?>> knownSettings =
                Stream.concat(
                        ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(),
                        Stream.concat(dynamicSettings.stream(), Stream.of(invalidSetting)))
                        .collect(Collectors.toSet());
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, knownSettings);
        for (final Setting<String> dynamicSetting : dynamicSettings) {
            clusterSettings.addSettingsUpdateConsumer(dynamicSetting, s -> {});
        }
        final SettingsUpdater settingsUpdater = new SettingsUpdater(clusterSettings);
        final MetaData.Builder metaDataBuilder =
                MetaData.builder()
                        .persistentSettings(existingPersistentSettings.build())
                        .transientSettings(existingTransientSettings.build());
        final ClusterState clusterState = ClusterState.builder(new ClusterName("cluster")).metaData(metaDataBuilder).build();
        final Settings.Builder persistentToApply = Settings.builder();
        final Settings.Builder transientToApply = Settings.builder();
        for (final Setting<String> dynamicSetting : dynamicSettings) {
            if (randomBoolean()) {
                persistentToApply.put(dynamicSetting.getKey(), "value");
            } else {
                transientToApply.put(dynamicSetting.getKey(), "value");
            }
        }
        final ClusterState clusterStateAfterUpdate;
        clusterStateAfterUpdate =
                settingsUpdater.updateSettings(clusterState, transientToApply.build(), persistentToApply.build(), logger);

        if (existingPersistentSettings.keys().contains("invalid.setting")) {
            assertThat(
                    clusterStateAfterUpdate.metaData().persistentSettings().keySet(),
                    hasItem(ARCHIVED_SETTINGS_PREFIX + "invalid.setting"));
        } else {
            assertThat(
                    clusterStateAfterUpdate.metaData().transientSettings().keySet(),
                    hasItem(ARCHIVED_SETTINGS_PREFIX + "invalid.setting"));
        }

        if (existingPersistentSettings.keys().contains("unknown.setting")) {
            assertThat(
                    clusterStateAfterUpdate.metaData().persistentSettings().keySet(),
                    hasItem(ARCHIVED_SETTINGS_PREFIX + "unknown.setting"));
        } else {
            assertThat(
                    clusterStateAfterUpdate.metaData().transientSettings().keySet(),
                    hasItem(ARCHIVED_SETTINGS_PREFIX + "unknown.setting"));
        }

        for (final Setting<String> dynamicSetting : dynamicSettings) {
            if (persistentToApply.keys().contains(dynamicSetting.getKey())) {
                assertThat(clusterStateAfterUpdate.metaData().persistentSettings().keySet(), hasItem(dynamicSetting.getKey()));
            } else {
                assertThat(clusterStateAfterUpdate.metaData().transientSettings().keySet(), hasItem(dynamicSetting.getKey()));
            }
        }

    }

}
