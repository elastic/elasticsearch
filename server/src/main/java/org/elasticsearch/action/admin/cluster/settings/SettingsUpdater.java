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

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;

import java.util.Map;

import static org.elasticsearch.cluster.ClusterState.builder;
import static org.elasticsearch.common.settings.AbstractScopedSettings.ARCHIVED_SETTINGS_PREFIX;

/**
 * Updates transient and persistent cluster state settings if there are any changes
 * due to the update.
 */
final class SettingsUpdater {
    final Settings.Builder transientUpdates = Settings.builder();
    final Settings.Builder persistentUpdates = Settings.builder();
    private final ClusterSettings clusterSettings;

    SettingsUpdater(ClusterSettings clusterSettings) {
        this.clusterSettings = clusterSettings;
    }

    synchronized Settings getTransientUpdates() {
        return transientUpdates.build();
    }

    synchronized Settings getPersistentUpdate() {
        return persistentUpdates.build();
    }

    synchronized ClusterState updateSettings(
            final ClusterState currentState, final Settings transientToApply, final Settings persistentToApply, final Logger logger) {
        boolean changed = false;

        /*
         * Our cluster state could have unknown or invalid settings that are known and valid in a previous version of Elasticsearch. We can
         * end up in this situation during a rolling upgrade where the previous version will infect the current version of Elasticsearch
         * with settings that the current version either no longer knows about or now considers to have invalid values. When the current
         * version of Elasticsearch becomes infected with a cluster state containing such settings, we need to skip validating such settings
         * and instead archive them. Consequently, for the current transient and persistent settings in the cluster state we do the
         * following:
         *  - split existing settings instance into two with the known and valid settings in one, and the unknown or invalid in another
         *    (note that existing archived settings are included in the known and valid settings)
         *  - validate the incoming settings update combined with the existing known and valid settings
         *  - merge in the archived unknown or invalid settings
         */
        final Tuple<Settings, Settings> partitionedTransientSettings =
                partitionKnownAndValidSettings(currentState.metadata().transientSettings(), "transient", logger);
        final Settings knownAndValidTransientSettings = partitionedTransientSettings.v1();
        final Settings unknownOrInvalidTransientSettings = partitionedTransientSettings.v2();
        final Settings.Builder transientSettings = Settings.builder().put(knownAndValidTransientSettings);
        changed |= clusterSettings.updateDynamicSettings(transientToApply, transientSettings, transientUpdates, "transient");

        final Tuple<Settings, Settings> partitionedPersistentSettings =
                partitionKnownAndValidSettings(currentState.metadata().persistentSettings(), "persistent", logger);
        final Settings knownAndValidPersistentSettings = partitionedPersistentSettings.v1();
        final Settings unknownOrInvalidPersistentSettings = partitionedPersistentSettings.v2();
        final Settings.Builder persistentSettings = Settings.builder().put(knownAndValidPersistentSettings);
        changed |= clusterSettings.updateDynamicSettings(persistentToApply, persistentSettings, persistentUpdates, "persistent");

        final ClusterState clusterState;
        if (changed) {
            Settings transientFinalSettings = transientSettings.build();
            Settings persistentFinalSettings = persistentSettings.build();
            // both transient and persistent settings must be consistent by itself we can't allow dependencies to be
            // in either of them otherwise a full cluster restart will break the settings validation
            clusterSettings.validate(transientFinalSettings, true);
            clusterSettings.validate(persistentFinalSettings, true);

            Metadata.Builder metadata = Metadata.builder(currentState.metadata())
                    .transientSettings(Settings.builder().put(transientFinalSettings).put(unknownOrInvalidTransientSettings).build())
                    .persistentSettings(Settings.builder().put(persistentFinalSettings).put(unknownOrInvalidPersistentSettings).build());

            ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
            boolean updatedReadOnly = Metadata.SETTING_READ_ONLY_SETTING.get(metadata.persistentSettings())
                    || Metadata.SETTING_READ_ONLY_SETTING.get(metadata.transientSettings());
            if (updatedReadOnly) {
                blocks.addGlobalBlock(Metadata.CLUSTER_READ_ONLY_BLOCK);
            } else {
                blocks.removeGlobalBlock(Metadata.CLUSTER_READ_ONLY_BLOCK);
            }
            boolean updatedReadOnlyAllowDelete = Metadata.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.get(metadata.persistentSettings())
                    || Metadata.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.get(metadata.transientSettings());
            if (updatedReadOnlyAllowDelete) {
                blocks.addGlobalBlock(Metadata.CLUSTER_READ_ONLY_ALLOW_DELETE_BLOCK);
            } else {
                blocks.removeGlobalBlock(Metadata.CLUSTER_READ_ONLY_ALLOW_DELETE_BLOCK);
            }
            clusterState = builder(currentState).metadata(metadata).blocks(blocks).build();
        } else {
            clusterState = currentState;
        }

        /*
         * Now we try to apply things and if they are invalid we fail. This dry run will validate, parse settings, and trigger deprecation
         * logging, but will not actually apply them.
         */
        final Settings settings = clusterState.metadata().settings();
        clusterSettings.validateUpdate(settings);

        return clusterState;
    }

    /**
     * Partitions the settings into those that are known and valid versus those that are unknown or invalid. The resulting tuple contains
     * the known and valid settings in the first component and the unknown or invalid settings in the second component. Note that archived
     * settings contained in the settings to partition are included in the first component.
     *
     * @param settings     the settings to partition
     * @param settingsType a string to identify the settings (for logging)
     * @param logger       a logger to sending warnings to
     * @return the partitioned settings
     */
    private Tuple<Settings, Settings> partitionKnownAndValidSettings(
            final Settings settings, final String settingsType, final Logger logger) {
        final Settings existingArchivedSettings = settings.filter(k -> k.startsWith(ARCHIVED_SETTINGS_PREFIX));
        final Settings settingsExcludingExistingArchivedSettings =
                settings.filter(k -> k.startsWith(ARCHIVED_SETTINGS_PREFIX) == false);
        final Settings settingsWithUnknownOrInvalidArchived = clusterSettings.archiveUnknownOrInvalidSettings(
                settingsExcludingExistingArchivedSettings,
                e -> logUnknownSetting(settingsType, e, logger),
                (e, ex) -> logInvalidSetting(settingsType, e, ex, logger));
        return Tuple.tuple(
                Settings.builder()
                        .put(settingsWithUnknownOrInvalidArchived.filter(k -> k.startsWith(ARCHIVED_SETTINGS_PREFIX) == false))
                        .put(existingArchivedSettings)
                        .build(),
                settingsWithUnknownOrInvalidArchived.filter(k -> k.startsWith(ARCHIVED_SETTINGS_PREFIX)));
    }

    private void logUnknownSetting(final String settingType, final Map.Entry<String, String> e, final Logger logger) {
        logger.warn("ignoring existing unknown {} setting: [{}] with value [{}]; archiving", settingType, e.getKey(), e.getValue());
    }

    private void logInvalidSetting(
            final String settingType, final Map.Entry<String, String> e, final IllegalArgumentException ex, final Logger logger) {
        logger.warn(
                (Supplier<?>)
                        () -> new ParameterizedMessage("ignoring existing invalid {} setting: [{}] with value [{}]; archiving",
                                settingType,
                                e.getKey(),
                                e.getValue()),
                ex);
    }

}
