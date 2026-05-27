/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;

import java.util.Set;

import static org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotSettings.RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING;
import static org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotSettings.STATELESS_SNAPSHOT_ENABLED_SETTING;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class StatelessSnapshotSettingsTests extends ESTestCase {

    public void testEnabledWithRelocationIsValid() {
        final var clusterSettings = createClusterSettings(Settings.EMPTY);
        initializeAndWatch(clusterSettings);

        clusterSettings.applySettings(
            Settings.builder()
                .put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), "enabled")
                .put(RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING.getKey(), true)
                .build()
        );
    }

    public void testDisabledWithRelocationIsInvalid() {
        final var clusterSettings = createClusterSettings(Settings.EMPTY);
        initializeAndWatch(clusterSettings);

        final var builder = Settings.builder().put(RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING.getKey(), true);
        if (randomBoolean()) {
            builder.put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), "disabled");
        }
        assertInvalidSettingsCombination(clusterSettings, builder.build());
    }

    public void testReadFromObjectStoreWithRelocationIsInvalid() {
        final boolean hasExistingSetting = randomBoolean();
        final var clusterSettings = createClusterSettings(
            hasExistingSetting
                ? Settings.builder().put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), "read_from_object_store").build()
                : Settings.EMPTY
        );
        initializeAndWatch(clusterSettings);

        final var builder = Settings.builder().put(RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING.getKey(), true);
        if (hasExistingSetting && randomBoolean()) {
            builder.put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), "read_from_object_store");
        }
        assertInvalidSettingsCombination(clusterSettings, builder.build());
    }

    private static void assertInvalidSettingsCombination(ClusterSettings clusterSettings, Settings newSettings) {
        final var e = expectThrows(IllegalArgumentException.class, () -> clusterSettings.applySettings(newSettings));
        assertThat(e.getMessage(), anyOf(containsString("cannot be [true] unless"), containsString("illegal value can't update")));
        if (e.getMessage().contains("cannot be [true] unless") == false) {
            assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
            assertThat(e.getCause().getMessage(), containsString("cannot be [true] unless"));
        }
    }

    private static void initializeAndWatch(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(STATELESS_SNAPSHOT_ENABLED_SETTING, value -> {});
        clusterSettings.initializeAndWatch(RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING, value -> {});
    }

    private static ClusterSettings createClusterSettings(Settings settings) {
        final Set<Setting<?>> settingsSet = Sets.addToCopy(
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS,
            STATELESS_SNAPSHOT_ENABLED_SETTING,
            RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING
        );
        return new ClusterSettings(settings, settingsSet);
    }
}
