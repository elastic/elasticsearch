/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

/**
 * This class provides the cluster wide default rollover conditions that are used by all data streams managed by DLM. The rollover
 * conditions are defined by a cluster setting.
 */
public class DefaultClusterRolloverProvider {
    private static final Logger logger = LogManager.getLogger(DefaultClusterRolloverProvider.class);

    public static final Setting<RolloverConditions> CLUSTER_DLM_DEFAULT_ROLLOVER_SETTING = new Setting<>(
        "cluster.dlm.default.rollover",
        "max_age=7d,max_primary_shard_size=50gb,min_docs=1,max_primary_shard_docs=200000000",
        (s) -> RolloverConditions.parseSetting(s, "cluster.dlm.default.rollover"),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile RolloverConditions conditions;

    private DefaultClusterRolloverProvider(Settings settings) {
        setConditions(CLUSTER_DLM_DEFAULT_ROLLOVER_SETTING.get(settings));
    }

    public static DefaultClusterRolloverProvider create(Settings settings, ClusterSettings clusterSettings) {
        DefaultClusterRolloverProvider provider = new DefaultClusterRolloverProvider(settings);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_DLM_DEFAULT_ROLLOVER_SETTING, provider::setConditions);
        return provider;
    }

    private void setConditions(RolloverConditions conditions) {
        this.conditions = conditions;
        logger.info("Set default rollover conditions for DLM: [{}]", conditions);
    }

    public RolloverConditions get() {
        return conditions;
    }
}
