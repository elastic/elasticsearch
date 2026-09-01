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
 * The operator-supplied defaults currently in force, as a live view of the cluster settings derived from
 * {@link QuerySettings#clusterSettings()}. Read once per query by {@link QuerySettings#resolve} to contribute the
 * cluster layer of {@code default < cluster < body < SET}.
 *
 * <p>Holds a {@link Settings} rather than a map of parsed values because the fold needs to know whether an operator
 * <b>set</b> a key, not what it would read if they had not. Each derived setting declares the registry default as
 * its own default, so a value-shaped view could not tell an unset key from one an operator set to the default —
 * a distinction that matters to a merging reconciler and to every check that asks whether a setting is
 * operator-configured.
 *
 * <p>Updates arrive through the grouped settings-update consumer, which hands over the merged node and cluster-state
 * settings filtered to these keys. That single view therefore covers both write paths: a value in
 * {@code elasticsearch.yml} and one written with {@code PUT _cluster/settings}.
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
        // Seed from node settings so a value in elasticsearch.yml is in force before the first cluster state is
        // applied; the update consumer does not fire on registration, only on change.
        this.values = filterToDerived(clusterService.getSettings(), derived);
        reportUnusableValues(this.values);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(updated -> {
            this.values = updated;
            reportUnusableValues(updated);
        }, derived);
    }

    /**
     * Log any configured value this node cannot use. A value is refused when it is written, and an invalid one in
     * recovered cluster state is archived rather than applied, so this fires only for state that arrived over the
     * wire without either check — a node joining a running cluster, or a mixed-version cluster whose master accepted
     * a value this node cannot use. Resolution falls back to the built-in default for such a value rather than
     * failing queries, which would otherwise leave the operator with no signal at all; this is that signal, on the
     * operator's own channel and once per observation rather than once per query.
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
