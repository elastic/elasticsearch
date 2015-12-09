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

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.ClusterState.builder;

/**
 * Updates transient and persistent cluster state settings if there are any changes
 * due to the update.
 */
final class SettingsUpdater {
    final Settings.Builder transientUpdates = Settings.settingsBuilder();
    final Settings.Builder persistentUpdates = Settings.settingsBuilder();
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

    synchronized ClusterState updateSettings(final ClusterState currentState, Settings transientToApply, Settings persistentToApply) {
        boolean changed = false;
        Settings.Builder transientSettings = Settings.settingsBuilder();
        transientSettings.put(currentState.metaData().transientSettings());
        changed |= apply(transientToApply, transientSettings, transientUpdates, "transient");

        Settings.Builder persistentSettings = Settings.settingsBuilder();
        persistentSettings.put(currentState.metaData().persistentSettings());
        changed |= apply(persistentToApply, persistentSettings, persistentUpdates, "persistent");

        if (!changed) {
            return currentState;
        }

        MetaData.Builder metaData = MetaData.builder(currentState.metaData())
            .persistentSettings(persistentSettings.build())
            .transientSettings(transientSettings.build());

        ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
        boolean updatedReadOnly = MetaData.SETTING_READ_ONLY_SETTING.get(metaData.persistentSettings()) || MetaData.SETTING_READ_ONLY_SETTING.get(metaData.transientSettings());
        if (updatedReadOnly) {
            blocks.addGlobalBlock(MetaData.CLUSTER_READ_ONLY_BLOCK);
        } else {
            blocks.removeGlobalBlock(MetaData.CLUSTER_READ_ONLY_BLOCK);
        }
        ClusterState build = builder(currentState).metaData(metaData).blocks(blocks).build();
        Settings settings = build.metaData().settings();
        // now we try to apply things and if they are invalid we fail
        // this dryRun will validate & parse settings but won't actually apply them.
        clusterSettings.dryRun(settings);
        return build;
    }

    private boolean apply(Settings toApply, Settings.Builder target, Settings.Builder updates, String type) {
        boolean changed = false;
        final Set<String> toRemove = new HashSet<>();
        Settings.Builder settingsBuilder = Settings.settingsBuilder();
        for (Map.Entry<String, String> entry : toApply.getAsMap().entrySet()) {
            if (entry.getValue() == null) {
                toRemove.add(entry.getKey());
            } else if (clusterSettings.isLoggerSetting(entry.getKey()) || clusterSettings.hasDynamicSetting(entry.getKey())) {
                settingsBuilder.put(entry.getKey(), entry.getValue());
                updates.put(entry.getKey(), entry.getValue());
                changed = true;
            } else {
                throw new IllegalArgumentException(type + " setting [" + entry.getKey() + "], not dynamically updateable");
            }

        }
        changed |= applyDeletes(toRemove, target);
        target.put(settingsBuilder.build());
        return changed;
    }

    private final boolean applyDeletes(Set<String> deletes, Settings.Builder builder) {
        boolean changed = false;
        for (String entry : deletes) {
            Set<String> keysToRemove = new HashSet<>();
            Set<String> keySet = builder.internalMap().keySet();
            for (String key : keySet) {
                if (Regex.simpleMatch(entry, key)) {
                    keysToRemove.add(key);
                }
            }
            for (String key : keysToRemove) {
                builder.remove(key);
                changed = true;
            }
        }
        return changed;
    }
}
