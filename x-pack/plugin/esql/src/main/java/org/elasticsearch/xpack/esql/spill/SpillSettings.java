/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.spill;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.monitor.jvm.JvmInfo;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The node's spill settings and the one rule that makes the kill switch safe: a query reads a snapshot when its scope is
 * created and keeps it. Enabling or disabling applies to queries started afterwards; a query that started with spilling
 * on finishes its spills and reads them back, one that started with it off never spills. The budget is resolved to bytes
 * once per query, from the request breaker's limit at that moment, so the soft line sits below the hard line by
 * construction; a breaker without a limit falls back to the same share of the heap. The store is static.
 * <p>
 * Everything here is behind the {@code esql_spill} feature flag, which is on in snapshot builds only: without it no
 * setting is registered, no pool is created and no store is loaded.
 *
 * @param enabled the kill switch
 * @param queryBudget the share of the request breaker's limit a query may use on a node before its operators spill
 */
public record SpillSettings(boolean enabled, RatioValue queryBudget) {

    public static final FeatureFlag ESQL_SPILL_FEATURE_FLAG = new FeatureFlag("esql_spill");

    public static final Setting<Boolean> ENABLED = Setting.boolSetting(
        "esql.spill.enabled",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<RatioValue> QUERY_BUDGET = new Setting<>(
        "esql.spill.query_budget",
        "50%",
        RatioValue::parseRatioValue,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * The backend, by its provider's name. Set to a name no loaded provider answers to, the node fails at start; left at
     * the default with no provider present, spilling is off.
     */
    public static final Setting<String> STORE = Setting.simpleString("esql.spill.store", "disk", Setting.Property.NodeScope);

    /**
     * The own-state minimum: an operator holding less never spills, so that a query crossing its budget does not spill
     * every small operator at once. A constant rather than a setting.
     */
    public static final long MIN_SPILL_BYTES = ByteSizeValue.ofMb(16).getBytes();

    public static final SpillSettings DEFAULTS = new SpillSettings(ENABLED.getDefault(null), QUERY_BUDGET.getDefault(null));

    /**
     * The settings the plugin registers: these three when the flag is on, none otherwise.
     */
    public static List<Setting<?>> settings() {
        return ESQL_SPILL_FEATURE_FLAG.isEnabled() ? List.of(ENABLED, QUERY_BUDGET, STORE) : List.of();
    }

    /**
     * The budget in bytes for a query starting now: the share of the request breaker's current limit, or of the heap when
     * the breaker has no limit.
     */
    public long budgetBytes(CircuitBreaker requestBreaker) {
        long limit = requestBreaker.getLimit();
        long base = limit > 0 ? limit : JvmInfo.jvmInfo().getMem().getHeapMax().getBytes();
        return (long) (base * queryBudget.getAsRatio());
    }

    /**
     * The current settings, updated by the cluster settings and read as one snapshot.
     */
    public static final class Holder {
        private final AtomicReference<SpillSettings> current = new AtomicReference<>(DEFAULTS);

        public Holder(ClusterService clusterService) {
            this(clusterService.getClusterSettings());
        }

        public Holder(ClusterSettings clusterSettings) {
            current.set(new SpillSettings(clusterSettings.get(ENABLED), clusterSettings.get(QUERY_BUDGET)));
            // one consumer for both, so an update that changes both never yields a snapshot with one of each
            clusterSettings.addSettingsUpdateConsumer(
                ENABLED,
                QUERY_BUDGET,
                (enabled, budget) -> current.set(new SpillSettings(enabled, budget))
            );
        }

        public SpillSettings get() {
            return current.get();
        }
    }
}
