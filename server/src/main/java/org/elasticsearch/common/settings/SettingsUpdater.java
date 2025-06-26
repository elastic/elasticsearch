/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.settings;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.core.Tuple;

import static org.elasticsearch.cluster.ClusterState.builder;

/**
 * Updates transient and persistent cluster state settings if there are any changes
 * due to the update.
 */
public final class SettingsUpdater extends BaseSettingsUpdater {
    final Settings.Builder transientUpdates = Settings.builder();
    final Settings.Builder persistentUpdates = Settings.builder();

    public SettingsUpdater(ClusterSettings scopedSettings) {
        super(scopedSettings);
    }

    public synchronized Settings getTransientUpdates() {
        return transientUpdates.build();
    }

    public synchronized Settings getPersistentUpdate() {
        return persistentUpdates.build();
    }

    public synchronized ClusterState updateSettings(
        final ClusterState currentState,
        final Settings transientToApply,
        final Settings persistentToApply,
        final Logger logger
    ) {
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
        final Tuple<Settings, Settings> partitionedTransientSettings = partitionKnownAndValidSettings(
            currentState.metadata().transientSettings(),
            "transient",
            logger
        );
        final Settings knownAndValidTransientSettings = partitionedTransientSettings.v1();
        final Settings unknownOrInvalidTransientSettings = partitionedTransientSettings.v2();
        final Settings.Builder transientSettings = Settings.builder().put(knownAndValidTransientSettings);
        changed |= scopedSettings.updateDynamicSettings(transientToApply, transientSettings, transientUpdates, "transient");

        final Tuple<Settings, Settings> partitionedPersistentSettings = partitionKnownAndValidSettings(
            currentState.metadata().persistentSettings(),
            "persistent",
            logger
        );
        final Settings knownAndValidPersistentSettings = partitionedPersistentSettings.v1();
        final Settings unknownOrInvalidPersistentSettings = partitionedPersistentSettings.v2();
        final Settings.Builder persistentSettings = Settings.builder().put(knownAndValidPersistentSettings);
        changed |= scopedSettings.updateDynamicSettings(persistentToApply, persistentSettings, persistentUpdates, "persistent");

        final ClusterState clusterState;
        if (changed) {
            Settings transientFinalSettings = transientSettings.build();
            Settings persistentFinalSettings = persistentSettings.build();
            // both transient and persistent settings must be consistent by itself we can't allow dependencies to be
            // in either of them otherwise a full cluster restart will break the settings validation
            scopedSettings.validate(transientFinalSettings, true);
            scopedSettings.validate(persistentFinalSettings, true);

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
        scopedSettings.validateUpdate(settings);

        return clusterState;
    }
}
