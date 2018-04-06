/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;

import java.util.Map;

public class UpdateAllocationSettingsStep extends ClusterStateActionStep {
    private final Map<String, String> include;
    private final Map<String, String> exclude;
    private final Map<String, String> require;

    public UpdateAllocationSettingsStep(StepKey key, StepKey nextStepKey, Map<String, String> include,
                                        Map<String, String> exclude, Map<String, String> require) {
        super(key, nextStepKey);
        this.include = include;
        this.exclude = exclude;
        this.require = require;
    }

    @Override
    public ClusterState performAction(Index index, ClusterState clusterState) {
        IndexMetaData idxMeta = clusterState.metaData().index(index);
        if (idxMeta == null) {
            return clusterState;
        }
        Settings existingSettings = idxMeta.getSettings();
        Settings.Builder newSettings = Settings.builder();
        addMissingAttrs(include, IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey(), existingSettings, newSettings);
        addMissingAttrs(exclude, IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey(), existingSettings, newSettings);
        addMissingAttrs(require, IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey(), existingSettings, newSettings);
        return ClusterState.builder(clusterState)
            .metaData(MetaData.builder(clusterState.metaData())
                .updateSettings(newSettings.build(), index.getName())).build();
    }

    /**
     * Inspects the <code>existingSettings</code> and adds any attributes that
     * are missing for the given <code>settingsPrefix</code> to the
     * <code>newSettingsBuilder</code>.
     */
    static void addMissingAttrs(Map<String, String> newAttrs, String settingPrefix, Settings existingSettings,
                                 Settings.Builder newSettingsBuilder) {
        newAttrs.entrySet().stream().filter(e -> {
            String existingValue = existingSettings.get(settingPrefix + e.getKey());
            return existingValue == null || (existingValue.equals(e.getValue()) == false);
        }).forEach(e -> newSettingsBuilder.put(settingPrefix + e.getKey(), e.getValue()));
    }
}
