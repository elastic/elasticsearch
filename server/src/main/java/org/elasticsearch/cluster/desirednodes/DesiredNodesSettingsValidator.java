/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.desirednodes;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.DesiredNode;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class DesiredNodesSettingsValidator {
    private final ClusterSettings clusterSettings;
    private final Map<String, Function<Long, Setting<?>>> memoryOverride;
    private final Map<String, Function<Integer, Setting<?>>> processorOverride;

    public DesiredNodesSettingsValidator(
        ClusterSettings clusterSettings,
        Map<String, Function<Long, Setting<?>>> memoryOverride,
        Map<String, Function<Integer, Setting<?>>> processorOverride
    ) {
        this.clusterSettings = clusterSettings;
        this.memoryOverride = memoryOverride;
        this.processorOverride = processorOverride;
    }

    public void validateSettings(DesiredNode node) {
        final Settings nodeSettings = node.settings();
        final Settings.Builder updatedSettingsBuilder = Settings.builder().put(nodeSettings);
        List<String> unknownSettings = null;
        final Set<Setting<?>> settingSet = new HashSet<>();
        for (String settingKey : nodeSettings.keySet()) {
            Setting<?> setting = clusterSettings.get(settingKey);
            if (setting == null) {
                if (unknownSettings == null) {
                    unknownSettings = new ArrayList<>();
                }
                updatedSettingsBuilder.remove(settingKey);
                unknownSettings.add(settingKey);
                continue;
            }
            settingSet.add(maybeOverride(setting, node));
        }

        if (unknownSettings != null && node.version().onOrBefore(Version.CURRENT)) {
            assert unknownSettings.isEmpty() == false;
            throw new IllegalArgumentException("Unknown settings " + unknownSettings);
        }

        final Settings updatedSettings = updatedSettingsBuilder.build();
        final ClusterSettings desiredNodeSettings = new ClusterSettings(updatedSettings, settingSet, Collections.emptySet());
        desiredNodeSettings.validate(updatedSettings, true);
    }

    private Setting<?> maybeOverride(Setting<?> setting, DesiredNode desiredNode) {
        Function<Long, Setting<?>> settingFactory = memoryOverride.get(setting.getKey());
        if (settingFactory != null) {
            assert processorOverride.containsKey(setting.getKey()) == false;
            return settingFactory.apply(desiredNode.memory().getBytes());
        }

        Function<Integer, Setting<?>> processorSetting = processorOverride.get(setting.getKey());
        if (processorSetting != null) {
            assert memoryOverride.containsKey(setting.getKey()) == false;
            return processorSetting.apply(desiredNode.processors());
        }

        return setting;
    }
}
