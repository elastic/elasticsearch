/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.settings;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.elasticsearch.common.settings.AbstractScopedSettings.ARCHIVED_SETTINGS_PREFIX;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

public class SettingsUpdaterTests extends ESTestCase {

    public void testUpdateSetting() {
        AtomicReference<Float> index = new AtomicReference<>();
        AtomicReference<Float> shard = new AtomicReference<>();
        ClusterState.Builder builder = ClusterState.builder(new ClusterName("foo"));
        ClusterSettings settingsService = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settingsService.addSettingsUpdateConsumer(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING, index::set);
        settingsService.addSettingsUpdateConsumer(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING, shard::set);
        SettingsUpdater updater = new SettingsUpdater(settingsService);
        Metadata.Builder metadata = Metadata.builder()
            .persistentSettings(
                Settings.builder()
                    .put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 1.5)
                    .put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 2.5)
                    .build()
            )
            .transientSettings(
                Settings.builder()
                    .put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 3.5)
                    .put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 4.5)
                    .build()
            );
        ClusterState build = builder.metadata(metadata).build();
        ClusterState clusterState = updater.updateSettings(
            build,
            Settings.builder().put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 0.5).build(),
            Settings.builder().put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 0.4).build(),
            logger
        );
        assertNotSame(clusterState, build);
        assertEquals(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.get(clusterState.metadata().persistentSettings()), 0.4, 0.1);
        assertEquals(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.get(clusterState.metadata().persistentSettings()), 2.5, 0.1);
        assertEquals(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.get(clusterState.metadata().transientSettings()), 0.5, 0.1);
        assertEquals(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.get(clusterState.metadata().transientSettings()), 4.5, 0.1);

        clusterState = updater.updateSettings(
            clusterState,
            Settings.builder().putNull("cluster.routing.*").build(),
            Settings.EMPTY,
            logger
        );
        assertEquals(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.get(clusterState.metadata().persistentSettings()), 0.4, 0.1);
        assertEquals(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.get(clusterState.metadata().persistentSettings()), 2.5, 0.1);
        assertFalse(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.exists(clusterState.metadata().transientSettings()));
        assertFalse(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.exists(clusterState.metadata().transientSettings()));

        clusterState = updater.updateSettings(
            clusterState,
            Settings.EMPTY,
            Settings.builder()
                .putNull("cluster.routing.*")
                .put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 10.0)
                .build(),
            logger
        );

        assertEquals(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.get(clusterState.metadata().persistentSettings()), 10.0, 0.1);
        assertFalse(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.exists(clusterState.metadata().persistentSettings()));
        assertFalse(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.exists(clusterState.metadata().transientSettings()));
        assertFalse(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.exists(clusterState.metadata().transientSettings()));
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
        Metadata.Builder metadata = Metadata.builder()
            .persistentSettings(
                Settings.builder()
                    .put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 1.5)
                    .put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 2.5)
                    .build()
            )
            .transientSettings(
                Settings.builder()
                    .put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 3.5)
                    .put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 4.5)
                    .build()
            );
        ClusterState build = builder.metadata(metadata).build();

        try {
            updater.updateSettings(
                build,
                Settings.builder().put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), "not a float").build(),
                Settings.builder()
                    .put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), "not a float")
                    .put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 1.0f)
                    .build(),
                logger
            );
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
        Metadata.Builder metadata = Metadata.builder()
            .persistentSettings(
                Settings.builder()
                    .put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 1.5)
                    .put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 2.5)
                    .build()
            )
            .transientSettings(
                Settings.builder()
                    .put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 3.5)
                    .put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 4.5)
                    .build()
            );
        ClusterState build = builder.metadata(metadata).build();

        ClusterState clusterState = updater.updateSettings(
            build,
            Settings.builder().put(Metadata.SETTING_READ_ONLY_SETTING.getKey(), true).build(),
            Settings.builder()
                .put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 1.6)
                .put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 1.0f)
                .build(),
            logger
        );
        assertEquals(clusterState.blocks().global().size(), 1);
        assertEquals(clusterState.blocks().global().iterator().next(), Metadata.CLUSTER_READ_ONLY_BLOCK);

        clusterState = updater.updateSettings(
            build,
            Settings.EMPTY,
            Settings.builder().put(Metadata.SETTING_READ_ONLY_SETTING.getKey(), false).build(),
            logger
        );
        assertEquals(clusterState.blocks().global().size(), 0);

        clusterState = updater.updateSettings(
            build,
            Settings.builder().put(Metadata.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), true).build(),
            Settings.builder()
                .put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 1.6)
                .put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 1.0f)
                .build(),
            logger
        );
        assertEquals(clusterState.blocks().global().size(), 1);
        assertEquals(clusterState.blocks().global().iterator().next(), Metadata.CLUSTER_READ_ONLY_ALLOW_DELETE_BLOCK);
        clusterState = updater.updateSettings(
            build,
            Settings.EMPTY,
            Settings.builder().put(Metadata.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), false).build(),
            logger
        );
        assertEquals(clusterState.blocks().global().size(), 0);

    }

    public void testDeprecationLogging() {
        Setting<String> deprecatedSetting = Setting.simpleString(
            "deprecated.setting",
            Property.Dynamic,
            Property.NodeScope,
            Property.DeprecatedWarning
        );
        final Settings settings = Settings.builder().put("deprecated.setting", "foo").build();
        final Set<Setting<?>> settingsSet = Stream.concat(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(), Stream.of(deprecatedSetting))
            .collect(Collectors.toSet());
        final ClusterSettings clusterSettings = new ClusterSettings(settings, settingsSet);
        clusterSettings.addSettingsUpdateConsumer(deprecatedSetting, s -> {});
        final SettingsUpdater settingsUpdater = new SettingsUpdater(clusterSettings);
        final ClusterState clusterState = ClusterState.builder(new ClusterName("foo"))
            .metadata(Metadata.builder().persistentSettings(settings).build())
            .build();

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
        // we will randomly apply some new dynamic persistent and transient settings
        final int numberOfDynamicSettings = randomIntBetween(1, 8);
        final List<Setting<String>> dynamicSettings = new ArrayList<>(numberOfDynamicSettings);
        for (int i = 0; i < numberOfDynamicSettings; i++) {
            final Setting<String> dynamicSetting = Setting.simpleString("dynamic.setting" + i, Property.Dynamic, Property.NodeScope);
            dynamicSettings.add(dynamicSetting);
        }

        // these are invalid settings that exist as either persistent or transient settings
        final int numberOfInvalidSettings = randomIntBetween(0, 7);
        final List<Setting<String>> invalidSettings = invalidSettings(numberOfInvalidSettings);

        // these are unknown settings that exist as either persistent or transient settings
        final int numberOfUnknownSettings = randomIntBetween(0, 7);
        final List<Setting<String>> unknownSettings = unknownSettings(numberOfUnknownSettings);

        final Settings.Builder existingPersistentSettings = Settings.builder();
        final Settings.Builder existingTransientSettings = Settings.builder();

        for (final Setting<String> dynamicSetting : dynamicSettings) {
            switch (randomIntBetween(0, 2)) {
                case 0:
                    existingPersistentSettings.put(dynamicSetting.getKey(), "existing_value");
                    break;
                case 1:
                    existingTransientSettings.put(dynamicSetting.getKey(), "existing_value");
                    break;
                case 2:
                    break;
            }
        }

        for (final Setting<String> invalidSetting : invalidSettings) {
            if (randomBoolean()) {
                existingPersistentSettings.put(invalidSetting.getKey(), "value");
            } else {
                existingTransientSettings.put(invalidSetting.getKey(), "value");
            }
        }

        for (final Setting<String> unknownSetting : unknownSettings) {
            if (randomBoolean()) {
                existingPersistentSettings.put(unknownSetting.getKey(), "value");
            } else {
                existingTransientSettings.put(unknownSetting.getKey(), "value");
            }
        }

        // register all the known settings (note that we do not register the unknown settings)
        final Set<Setting<?>> knownSettings = Stream.concat(
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(),
            Stream.concat(dynamicSettings.stream(), invalidSettings.stream())
        ).collect(Collectors.toSet());
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, knownSettings);
        for (final Setting<String> dynamicSetting : dynamicSettings) {
            clusterSettings.addSettingsUpdateConsumer(dynamicSetting, s -> {});
        }
        final SettingsUpdater settingsUpdater = new SettingsUpdater(clusterSettings);
        final Metadata.Builder metadataBuilder = Metadata.builder()
            .persistentSettings(existingPersistentSettings.build())
            .transientSettings(existingTransientSettings.build());
        final ClusterState clusterState = ClusterState.builder(new ClusterName("cluster")).metadata(metadataBuilder).build();

        // prepare the dynamic settings update
        final Settings.Builder persistentToApply = Settings.builder();
        final Settings.Builder transientToApply = Settings.builder();
        for (final Setting<String> dynamicSetting : dynamicSettings) {
            switch (randomIntBetween(0, 2)) {
                case 0:
                    persistentToApply.put(dynamicSetting.getKey(), "new_value");
                    break;
                case 1:
                    transientToApply.put(dynamicSetting.getKey(), "new_value");
                    break;
                case 2:
                    break;
            }
        }

        if (transientToApply.keys().isEmpty() && persistentToApply.keys().isEmpty()) {
            // force a settings update otherwise our assertions below will fail
            if (randomBoolean()) {
                persistentToApply.put(dynamicSettings.get(0).getKey(), "new_value");
            } else {
                transientToApply.put(dynamicSettings.get(0).getKey(), "new_value");
            }
        }

        final ClusterState clusterStateAfterUpdate = settingsUpdater.updateSettings(
            clusterState,
            transientToApply.build(),
            persistentToApply.build(),
            logger
        );

        // the invalid settings should be archived and not present in non-archived form
        for (final Setting<String> invalidSetting : invalidSettings) {
            if (existingPersistentSettings.keys().contains(invalidSetting.getKey())) {
                assertThat(
                    clusterStateAfterUpdate.metadata().persistentSettings().keySet(),
                    hasItem(ARCHIVED_SETTINGS_PREFIX + invalidSetting.getKey())
                );
            } else {
                assertThat(
                    clusterStateAfterUpdate.metadata().transientSettings().keySet(),
                    hasItem(ARCHIVED_SETTINGS_PREFIX + invalidSetting.getKey())
                );
            }
            assertThat(clusterStateAfterUpdate.metadata().persistentSettings().keySet(), not(hasItem(invalidSetting.getKey())));
            assertThat(clusterStateAfterUpdate.metadata().transientSettings().keySet(), not(hasItem(invalidSetting.getKey())));
        }

        // the unknown settings should be archived and not present in non-archived form
        for (final Setting<String> unknownSetting : unknownSettings) {
            if (existingPersistentSettings.keys().contains(unknownSetting.getKey())) {
                assertThat(
                    clusterStateAfterUpdate.metadata().persistentSettings().keySet(),
                    hasItem(ARCHIVED_SETTINGS_PREFIX + unknownSetting.getKey())
                );
            } else {
                assertThat(
                    clusterStateAfterUpdate.metadata().transientSettings().keySet(),
                    hasItem(ARCHIVED_SETTINGS_PREFIX + unknownSetting.getKey())
                );
            }
            assertThat(clusterStateAfterUpdate.metadata().persistentSettings().keySet(), not(hasItem(unknownSetting.getKey())));
            assertThat(clusterStateAfterUpdate.metadata().transientSettings().keySet(), not(hasItem(unknownSetting.getKey())));
        }

        // the dynamic settings should be applied
        for (final Setting<String> dynamicSetting : dynamicSettings) {
            if (persistentToApply.keys().contains(dynamicSetting.getKey())) {
                assertThat(clusterStateAfterUpdate.metadata().persistentSettings().keySet(), hasItem(dynamicSetting.getKey()));
                assertThat(clusterStateAfterUpdate.metadata().persistentSettings().get(dynamicSetting.getKey()), equalTo("new_value"));
            } else if (transientToApply.keys().contains(dynamicSetting.getKey())) {
                assertThat(clusterStateAfterUpdate.metadata().transientSettings().keySet(), hasItem(dynamicSetting.getKey()));
                assertThat(clusterStateAfterUpdate.metadata().transientSettings().get(dynamicSetting.getKey()), equalTo("new_value"));
            } else {
                if (existingPersistentSettings.keys().contains(dynamicSetting.getKey())) {
                    assertThat(clusterStateAfterUpdate.metadata().persistentSettings().keySet(), hasItem(dynamicSetting.getKey()));
                    assertThat(
                        clusterStateAfterUpdate.metadata().persistentSettings().get(dynamicSetting.getKey()),
                        equalTo("existing_value")
                    );
                } else if (existingTransientSettings.keys().contains(dynamicSetting.getKey())) {
                    assertThat(clusterStateAfterUpdate.metadata().transientSettings().keySet(), hasItem(dynamicSetting.getKey()));
                    assertThat(
                        clusterStateAfterUpdate.metadata().transientSettings().get(dynamicSetting.getKey()),
                        equalTo("existing_value")
                    );
                } else {
                    assertThat(clusterStateAfterUpdate.metadata().persistentSettings().keySet(), not(hasItem(dynamicSetting.getKey())));
                    assertThat(clusterStateAfterUpdate.metadata().transientSettings().keySet(), not(hasItem(dynamicSetting.getKey())));
                }
            }
        }
    }

    public void testRemovingArchivedSettingsDoesNotRemoveNonArchivedInvalidOrUnknownSettings() {
        // these are settings that are archived in the cluster state as either persistent or transient settings
        final int numberOfArchivedSettings = randomIntBetween(1, 8);
        final List<Setting<String>> archivedSettings = new ArrayList<>(numberOfArchivedSettings);
        for (int i = 0; i < numberOfArchivedSettings; i++) {
            final Setting<String> archivedSetting = Setting.simpleString("setting", Property.NodeScope);
            archivedSettings.add(archivedSetting);
        }

        // these are invalid settings that exist as either persistent or transient settings
        final int numberOfInvalidSettings = randomIntBetween(0, 7);
        final List<Setting<String>> invalidSettings = invalidSettings(numberOfInvalidSettings);

        // these are unknown settings that exist as either persistent or transient settings
        final int numberOfUnknownSettings = randomIntBetween(0, 7);
        final List<Setting<String>> unknownSettings = unknownSettings(numberOfUnknownSettings);

        final Settings.Builder existingPersistentSettings = Settings.builder();
        final Settings.Builder existingTransientSettings = Settings.builder();

        for (final Setting<String> archivedSetting : archivedSettings) {
            if (randomBoolean()) {
                existingPersistentSettings.put(ARCHIVED_SETTINGS_PREFIX + archivedSetting.getKey(), "value");
            } else {
                existingTransientSettings.put(ARCHIVED_SETTINGS_PREFIX + archivedSetting.getKey(), "value");
            }
        }

        for (final Setting<String> invalidSetting : invalidSettings) {
            if (randomBoolean()) {
                existingPersistentSettings.put(invalidSetting.getKey(), "value");
            } else {
                existingTransientSettings.put(invalidSetting.getKey(), "value");
            }
        }

        for (final Setting<String> unknownSetting : unknownSettings) {
            if (randomBoolean()) {
                existingPersistentSettings.put(unknownSetting.getKey(), "value");
            } else {
                existingTransientSettings.put(unknownSetting.getKey(), "value");
            }
        }

        // register all the known settings (not that we do not register the unknown settings)
        final Set<Setting<?>> knownSettings = Stream.concat(
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(),
            Stream.concat(archivedSettings.stream(), invalidSettings.stream())
        ).collect(Collectors.toSet());
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, knownSettings);
        final SettingsUpdater settingsUpdater = new SettingsUpdater(clusterSettings);
        final Metadata.Builder metadataBuilder = Metadata.builder()
            .persistentSettings(existingPersistentSettings.build())
            .transientSettings(existingTransientSettings.build());
        final ClusterState clusterState = ClusterState.builder(new ClusterName("cluster")).metadata(metadataBuilder).build();

        final Settings.Builder persistentToApply = Settings.builder().put("archived.*", (String) null);
        final Settings.Builder transientToApply = Settings.builder().put("archived.*", (String) null);

        final ClusterState clusterStateAfterUpdate = settingsUpdater.updateSettings(
            clusterState,
            transientToApply.build(),
            persistentToApply.build(),
            logger
        );

        // existing archived settings are removed
        for (final Setting<String> archivedSetting : archivedSettings) {
            if (existingPersistentSettings.keys().contains(ARCHIVED_SETTINGS_PREFIX + archivedSetting.getKey())) {
                assertThat(
                    clusterStateAfterUpdate.metadata().persistentSettings().keySet(),
                    not(hasItem(ARCHIVED_SETTINGS_PREFIX + archivedSetting.getKey()))
                );
            } else {
                assertThat(
                    clusterStateAfterUpdate.metadata().transientSettings().keySet(),
                    not(hasItem(ARCHIVED_SETTINGS_PREFIX + archivedSetting.getKey()))
                );
            }
        }

        // the invalid settings should be archived and not present in non-archived form
        for (final Setting<String> invalidSetting : invalidSettings) {
            if (existingPersistentSettings.keys().contains(invalidSetting.getKey())) {
                assertThat(
                    clusterStateAfterUpdate.metadata().persistentSettings().keySet(),
                    hasItem(ARCHIVED_SETTINGS_PREFIX + invalidSetting.getKey())
                );
            } else {
                assertThat(
                    clusterStateAfterUpdate.metadata().transientSettings().keySet(),
                    hasItem(ARCHIVED_SETTINGS_PREFIX + invalidSetting.getKey())
                );
            }
            assertThat(clusterStateAfterUpdate.metadata().persistentSettings().keySet(), not(hasItem(invalidSetting.getKey())));
            assertThat(clusterStateAfterUpdate.metadata().transientSettings().keySet(), not(hasItem(invalidSetting.getKey())));
        }

        // the unknown settings should be archived and not present in non-archived form
        for (final Setting<String> unknownSetting : unknownSettings) {
            if (existingPersistentSettings.keys().contains(unknownSetting.getKey())) {
                assertThat(
                    clusterStateAfterUpdate.metadata().persistentSettings().keySet(),
                    hasItem(ARCHIVED_SETTINGS_PREFIX + unknownSetting.getKey())
                );
            } else {
                assertThat(
                    clusterStateAfterUpdate.metadata().transientSettings().keySet(),
                    hasItem(ARCHIVED_SETTINGS_PREFIX + unknownSetting.getKey())
                );
            }
            assertThat(clusterStateAfterUpdate.metadata().persistentSettings().keySet(), not(hasItem(unknownSetting.getKey())));
            assertThat(clusterStateAfterUpdate.metadata().transientSettings().keySet(), not(hasItem(unknownSetting.getKey())));
        }
    }

    private static List<Setting<String>> unknownSettings(int numberOfUnknownSettings) {
        final List<Setting<String>> unknownSettings = new ArrayList<>(numberOfUnknownSettings);
        for (int i = 0; i < numberOfUnknownSettings; i++) {
            unknownSettings.add(Setting.simpleString("unknown.setting" + i, Property.NodeScope));
        }
        return unknownSettings;
    }

    private static List<Setting<String>> invalidSettings(int numberOfInvalidSettings) {
        final List<Setting<String>> invalidSettings = new ArrayList<>(numberOfInvalidSettings);
        for (int i = 0; i < numberOfInvalidSettings; i++) {
            invalidSettings.add(randomBoolean() ? invalidInIsolationSetting(i) : invalidWithDependenciesSetting(i));
        }
        return invalidSettings;
    }

    private static Setting<String> invalidInIsolationSetting(int index) {
        return Setting.simpleString("invalid.setting" + index, new Setting.Validator<>() {

            @Override
            public void validate(final String value) {
                throw new IllegalArgumentException("Invalid in isolation setting");
            }

        }, Property.NodeScope);
    }

    private static Setting<String> invalidWithDependenciesSetting(int index) {
        return Setting.simpleString("invalid.setting" + index, new Setting.Validator<>() {

            @Override
            public void validate(final String value) {}

            @Override
            public void validate(final String value, final Map<Setting<?>, Object> settings) {
                throw new IllegalArgumentException("Invalid with dependencies setting");
            }

        }, Property.NodeScope);
    }

    private static class FooLowSettingValidator implements Setting.Validator<Integer> {

        @Override
        public void validate(final Integer value) {}

        @Override
        public void validate(final Integer low, final Map<Setting<?>, Object> settings) {
            if (settings.containsKey(SETTING_FOO_HIGH) && low > (int) settings.get(SETTING_FOO_HIGH)) {
                throw new IllegalArgumentException("[low]=" + low + " is higher than [high]=" + settings.get(SETTING_FOO_HIGH));
            }
        }

        @Override
        public Iterator<Setting<?>> settings() {
            final List<Setting<?>> settings = List.of(SETTING_FOO_HIGH);
            return settings.iterator();
        }

    }

    private static class FooHighSettingValidator implements Setting.Validator<Integer> {

        @Override
        public void validate(final Integer value) {

        }

        @Override
        public void validate(final Integer high, final Map<Setting<?>, Object> settings) {
            if (settings.containsKey(SETTING_FOO_LOW) && high < (int) settings.get(SETTING_FOO_LOW)) {
                throw new IllegalArgumentException("[high]=" + high + " is lower than [low]=" + settings.get(SETTING_FOO_LOW));
            }
        }

        @Override
        public Iterator<Setting<?>> settings() {
            final List<Setting<?>> settings = List.of(SETTING_FOO_LOW);
            return settings.iterator();
        }

    }

    private static final Setting<Integer> SETTING_FOO_LOW = new Setting<>(
        "foo.low",
        "10",
        Integer::valueOf,
        new FooLowSettingValidator(),
        Property.Dynamic,
        Setting.Property.NodeScope
    );
    private static final Setting<Integer> SETTING_FOO_HIGH = new Setting<>(
        "foo.high",
        "100",
        Integer::valueOf,
        new FooHighSettingValidator(),
        Property.Dynamic,
        Setting.Property.NodeScope
    );

    public void testUpdateOfValidationDependentSettings() {
        final ClusterSettings settings = new ClusterSettings(Settings.EMPTY, new HashSet<>(asList(SETTING_FOO_LOW, SETTING_FOO_HIGH)));
        final SettingsUpdater updater = new SettingsUpdater(settings);
        final Metadata.Builder metadata = Metadata.builder().persistentSettings(Settings.EMPTY).transientSettings(Settings.EMPTY);

        ClusterState cluster = ClusterState.builder(new ClusterName("cluster")).metadata(metadata).build();

        cluster = updater.updateSettings(cluster, Settings.builder().put(SETTING_FOO_LOW.getKey(), 20).build(), Settings.EMPTY, logger);
        assertThat(cluster.getMetadata().settings().get(SETTING_FOO_LOW.getKey()), equalTo("20"));

        cluster = updater.updateSettings(cluster, Settings.builder().put(SETTING_FOO_HIGH.getKey(), 40).build(), Settings.EMPTY, logger);
        assertThat(cluster.getMetadata().settings().get(SETTING_FOO_LOW.getKey()), equalTo("20"));
        assertThat(cluster.getMetadata().settings().get(SETTING_FOO_HIGH.getKey()), equalTo("40"));

        cluster = updater.updateSettings(cluster, Settings.builder().put(SETTING_FOO_LOW.getKey(), 5).build(), Settings.EMPTY, logger);
        assertThat(cluster.getMetadata().settings().get(SETTING_FOO_LOW.getKey()), equalTo("5"));
        assertThat(cluster.getMetadata().settings().get(SETTING_FOO_HIGH.getKey()), equalTo("40"));

        cluster = updater.updateSettings(cluster, Settings.builder().put(SETTING_FOO_HIGH.getKey(), 8).build(), Settings.EMPTY, logger);
        assertThat(cluster.getMetadata().settings().get(SETTING_FOO_LOW.getKey()), equalTo("5"));
        assertThat(cluster.getMetadata().settings().get(SETTING_FOO_HIGH.getKey()), equalTo("8"));

        final ClusterState finalCluster = cluster;
        Exception exception = expectThrows(
            IllegalArgumentException.class,
            () -> updater.updateSettings(finalCluster, Settings.builder().put(SETTING_FOO_HIGH.getKey(), 2).build(), Settings.EMPTY, logger)
        );

        assertThat(
            exception.getMessage(),
            either(equalTo("[high]=2 is lower than [low]=5")).or(equalTo("[low]=5 is higher than [high]=2"))
        );
    }

}
