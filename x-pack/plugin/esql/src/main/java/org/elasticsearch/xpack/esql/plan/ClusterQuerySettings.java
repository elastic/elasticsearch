/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.List;

/**
 * The operator-supplied defaults currently in force, read once per query by {@link QuerySettings#resolve}.
 *
 * <p>Holds raw {@link Settings} rather than parsed values because each derived setting declares the registry default
 * as its own, so a value-shaped view could not tell an unset key from one set to the default.
 *
 * <p>Updates arrive through the grouped settings-update consumer, which hands over node and cluster-state settings
 * merged and filtered to these keys — so one view covers both {@code elasticsearch.yml} and
 * {@code PUT _cluster/settings}.
 */
public final class ClusterQuerySettings {

    private static final Logger logger = LogManager.getLogger(ClusterQuerySettings.class);

    /** No operator defaults in play — the correct value for tests and for callers with no cluster context. */
    public static final ClusterQuerySettings EMPTY = new ClusterQuerySettings();

    private volatile Settings values;

    private ClusterQuerySettings() {
        this.values = Settings.EMPTY;
    }

    public ClusterQuerySettings(ClusterService clusterService) {
        List<Setting<?>> derived = QuerySettings.clusterSettings();
        // The update consumer does not fire on registration, so seed from node settings or an elasticsearch.yml
        // value would not apply until something unrelated changed.
        this.values = filterToDerived(clusterService.getSettings(), derived);
        reportUnusableValues(this.values);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(updated -> {
            this.values = updated;
            reportUnusableValues(updated);
        }, derived);
    }

    /**
     * Log any configured value this node cannot use. Resolution falls back to the built-in default for such a value
     * rather than failing queries, so without this the operator would have no signal at all. Fires once per
     * observation, not once per query.
     */
    private static void reportUnusableValues(Settings settings) {
        for (QuerySettingDef<?> def : QuerySettings.all()) {
            String error = def.clusterValueError(settings);
            if (error != null) {
                logger.warn(
                    "Cluster setting [{}{}] is configured but not usable on this cluster and is being ignored; "
                        + "queries fall back to the built-in default. Reason: {}",
                    QuerySettingDef.CLUSTER_SETTING_PREFIX,
                    def.name(),
                    error
                );
            }
        }
    }

    private static Settings filterToDerived(Settings settings, List<Setting<?>> derived) {
        return settings.filter(key -> {
            for (Setting<?> setting : derived) {
                if (setting.getKey().equals(key)) {
                    return true;
                }
            }
            return false;
        });
    }

    /** The settings as an operator left them. Only keys actually set are present. */
    public Settings values() {
        return values;
    }
}
