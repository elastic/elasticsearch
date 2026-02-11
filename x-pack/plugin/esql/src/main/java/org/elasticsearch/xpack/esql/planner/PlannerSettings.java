/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.MemorySizeValue;
import org.elasticsearch.compute.lucene.DataPartitioning;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.monitor.jvm.JvmInfo;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Values for cluster level settings used in physical planning.
 */
public class PlannerSettings {
    public static final Setting<DataPartitioning> DEFAULT_DATA_PARTITIONING = Setting.enumSetting(
        DataPartitioning.class,
        "esql.default_data_partitioning",
        DataPartitioning.AUTO,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<ByteSizeValue> VALUES_LOADING_JUMBO_SIZE = new Setting<>("esql.values_loading_jumbo_size", settings -> {
        long proportional = JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() / 1024;
        return ByteSizeValue.ofBytes(Math.max(proportional, ByteSizeValue.ofMb(1).getBytes())).getStringRep();
    },
        s -> MemorySizeValue.parseBytesSizeValueOrHeapRatio(s, "esql.values_loading_jumbo_size"),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Integer> LUCENE_TOPN_LIMIT = Setting.intSetting(
        "esql.lucene_topn_limit",
        IndexSettings.MAX_RESULT_WINDOW_SETTING.getDefault(Settings.EMPTY),
        -1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<ByteSizeValue> INTERMEDIATE_LOCAL_RELATION_MAX_SIZE = Setting.memorySizeSetting(
        "esql.intermediate_local_relation_max_size",
        "0.1%",
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Boolean> REDUCTION_LATE_MATERIALIZATION = Setting.boolSetting(
        "esql.reduction_late_materialization",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * The threshold number of grouping keys for a partial aggregation to start emitting intermediate results early.
     * While emitting partial results can reduce memory pressure and allow for incremental downstream processing,
     * it might emit the same keys multiple times, incurring serialization and network overhead. This setting,
     * in conjunction with {@link #PARTIAL_AGGREGATION_EMIT_UNIQUENESS_THRESHOLD}, helps mitigate these costs by
     * only triggering early emission when a significant number of keys have been collected and most are unique,
     * thus lowering the probability of re-emitting the same keys.
     * <p>
     * NOTE that the defaults are chosen somewhat arbitrarily but are partially based on other systems.
     * Other systems sometimes default to a lower threshold (e.g., 10,000) without a uniqueness threshold.
     * We may lower these defaults after benchmarking more use cases.
     */
    public static final Setting<Integer> PARTIAL_AGGREGATION_EMIT_KEYS_THRESHOLD = Setting.intSetting(
        "esql.partial_agg_emit_keys_threshold",
        100_000,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * The uniqueness threshold of grouping keys for partial aggregation to start emitting keys early.
     * This threshold controls the trade-off between the benefits of early emission and the costs of
     * repeated serialization and network transfer of the same keys. A higher uniqueness ratio ensures early emission
     * only if keys are not repeatedly seen in incoming data and are unlikely to appear again in future data.
     */
    public static final Setting<Double> PARTIAL_AGGREGATION_EMIT_UNIQUENESS_THRESHOLD = Setting.doubleSetting(
        "esql.partial_agg_emit_unique_threshold",
        0.1,
        0.0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * If we're loading more than this many fields at a time we discard column loaders after each
     * page regardless of whether we can reuse them. They have significant per-field memory overhead
     * so discarding them between pages allows some queries that would have OOMed to succeed. Usually
     * the paths that need very high performance don't load more than a handful of fields at a time,
     * so they <strong>do</strong> reuse fields.
     */
    public static final Setting<Integer> REUSE_COLUMN_LOADERS_THRESHOLD = Setting.intSetting(
        "esql.reuse_column_loaders_threshold",
        30,
        0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static List<Setting<?>> settings() {
        return List.of(
            DEFAULT_DATA_PARTITIONING,
            VALUES_LOADING_JUMBO_SIZE,
            LUCENE_TOPN_LIMIT,
            INTERMEDIATE_LOCAL_RELATION_MAX_SIZE,
            REDUCTION_LATE_MATERIALIZATION,
            PARTIAL_AGGREGATION_EMIT_KEYS_THRESHOLD,
            PARTIAL_AGGREGATION_EMIT_UNIQUENESS_THRESHOLD,
            REUSE_COLUMN_LOADERS_THRESHOLD
        );
    }

    public static class Holder {
        private final AtomicReference<PlannerSettings> settings = new AtomicReference<>(PlannerSettings.DEFAULTS);

        public Holder(ClusterService clusterService) {
            var clusterSettings = clusterService.getClusterSettings();
            clusterSettings.initializeAndWatch(DEFAULT_DATA_PARTITIONING, v -> settings.updateAndGet(s -> s.defaultDataPartitioning(v)));
            clusterSettings.initializeAndWatch(VALUES_LOADING_JUMBO_SIZE, v -> settings.updateAndGet(s -> s.valuesLoadingJumboSize(v)));
            clusterSettings.initializeAndWatch(LUCENE_TOPN_LIMIT, v -> settings.updateAndGet(s -> s.luceneTopNLimit(v)));
            clusterSettings.initializeAndWatch(
                INTERMEDIATE_LOCAL_RELATION_MAX_SIZE,
                v -> settings.updateAndGet(s -> s.intermediateLocalRelationMaxSize(v))
            );
            clusterSettings.initializeAndWatch(
                PARTIAL_AGGREGATION_EMIT_KEYS_THRESHOLD,
                v -> settings.updateAndGet(s -> s.partialEmitKeysThreshold(v))
            );
            clusterSettings.initializeAndWatch(
                PARTIAL_AGGREGATION_EMIT_UNIQUENESS_THRESHOLD,
                v -> settings.updateAndGet(s -> s.partialEmitUniquenessThreshold(v))
            );
            clusterSettings.initializeAndWatch(
                REUSE_COLUMN_LOADERS_THRESHOLD,
                v -> settings.updateAndGet(s -> s.reuseColumnLoadersThreshold(v))
            );
        }

        public PlannerSettings get() {
            return settings.get();
        }
    }

    private final DataPartitioning defaultDataPartitioning;
    private final ByteSizeValue valuesLoadingJumboSize;
    private final int luceneTopNLimit;
    private final ByteSizeValue intermediateLocalRelationMaxSize;
    private final int partialEmitKeysThreshold;
    private final double partialEmitUniquenessThreshold;
    private final int reuseColumnLoadersThreshold;

    /**
     * Defaults.
     */
    public static final PlannerSettings DEFAULTS = new PlannerSettings(
        DEFAULT_DATA_PARTITIONING.get(Settings.EMPTY),
        VALUES_LOADING_JUMBO_SIZE.get(Settings.EMPTY),
        LUCENE_TOPN_LIMIT.getDefault(Settings.EMPTY),
        INTERMEDIATE_LOCAL_RELATION_MAX_SIZE.getDefault(Settings.EMPTY),
        PARTIAL_AGGREGATION_EMIT_KEYS_THRESHOLD.getDefault(Settings.EMPTY),
        PARTIAL_AGGREGATION_EMIT_UNIQUENESS_THRESHOLD.getDefault(Settings.EMPTY),
        REUSE_COLUMN_LOADERS_THRESHOLD.getDefault(Settings.EMPTY)
    );

    /**
     * Create.
     */
    public PlannerSettings(
        DataPartitioning defaultDataPartitioning,
        ByteSizeValue valuesLoadingJumboSize,
        int luceneTopNLimit,
        ByteSizeValue intermediateLocalRelationMaxSize,
        int partialEmitKeysThreshold,
        double partialEmitUniquenessThreshold,
        int reuseColumnLoadersThreshold
    ) {
        this.defaultDataPartitioning = defaultDataPartitioning;
        this.valuesLoadingJumboSize = valuesLoadingJumboSize;
        this.luceneTopNLimit = luceneTopNLimit;
        this.intermediateLocalRelationMaxSize = intermediateLocalRelationMaxSize;
        this.partialEmitKeysThreshold = partialEmitKeysThreshold;
        this.partialEmitUniquenessThreshold = partialEmitUniquenessThreshold;
        this.reuseColumnLoadersThreshold = reuseColumnLoadersThreshold;
    }

    public PlannerSettings defaultDataPartitioning(DataPartitioning defaultDataPartitioning) {
        return new PlannerSettings(
            defaultDataPartitioning,
            valuesLoadingJumboSize,
            luceneTopNLimit,
            intermediateLocalRelationMaxSize,
            partialEmitKeysThreshold,
            partialEmitUniquenessThreshold,
            reuseColumnLoadersThreshold
        );
    }

    public DataPartitioning defaultDataPartitioning() {
        return defaultDataPartitioning;
    }

    public PlannerSettings valuesLoadingJumboSize(ByteSizeValue valuesLoadingJumboSize) {
        return new PlannerSettings(
            defaultDataPartitioning,
            valuesLoadingJumboSize,
            luceneTopNLimit,
            intermediateLocalRelationMaxSize,
            partialEmitKeysThreshold,
            partialEmitUniquenessThreshold,
            reuseColumnLoadersThreshold
        );
    }

    public ByteSizeValue valuesLoadingJumboSize() {
        return valuesLoadingJumboSize;
    }

    public PlannerSettings luceneTopNLimit(int luceneTopNLimit) {
        return new PlannerSettings(
            defaultDataPartitioning,
            valuesLoadingJumboSize,
            luceneTopNLimit,
            intermediateLocalRelationMaxSize,
            partialEmitKeysThreshold,
            partialEmitUniquenessThreshold,
            reuseColumnLoadersThreshold
        );
    }

    /**
     * Maximum {@code LIMIT} that we're willing to push to Lucene's topn.
     * <p>
     *     Lucene's topn code was designed for <strong>search</strong>
     *     which typically fetches 10 or 30 or 50 or 100 or 1000 documents.
     *     That's as many you want on a page, and that's what it's designed for.
     *     But if you go to, say, page 10, Lucene implements this as a search
     *     for {@code page_size * page_number} docs and then materializes only
     *     the last {@code page_size} documents. Traditionally, Elasticsearch
     *     limits that {@code page_size * page_number} which it calls the
     *     {@link IndexSettings#MAX_RESULT_WINDOW_SETTING "result window"}.
     *     So! ESQL defaults to the same default - {@code 10,000}.
     * </p>
     */
    public int luceneTopNLimit() {
        return luceneTopNLimit;
    }

    public PlannerSettings intermediateLocalRelationMaxSize(ByteSizeValue intermediateLocalRelationMaxSize) {
        return new PlannerSettings(
            defaultDataPartitioning,
            valuesLoadingJumboSize,
            luceneTopNLimit,
            intermediateLocalRelationMaxSize,
            partialEmitKeysThreshold,
            partialEmitUniquenessThreshold,
            reuseColumnLoadersThreshold
        );
    }

    public ByteSizeValue intermediateLocalRelationMaxSize() {
        return intermediateLocalRelationMaxSize;
    }

    public PlannerSettings partialEmitKeysThreshold(int partialEmitKeysThreshold) {
        return new PlannerSettings(
            defaultDataPartitioning,
            valuesLoadingJumboSize,
            luceneTopNLimit,
            intermediateLocalRelationMaxSize,
            partialEmitKeysThreshold,
            partialEmitUniquenessThreshold,
            reuseColumnLoadersThreshold
        );
    }

    public int partialEmitKeysThreshold() {
        return partialEmitKeysThreshold;
    }

    public PlannerSettings partialEmitUniquenessThreshold(double partialEmitUniquenessThreshold) {
        return new PlannerSettings(
            defaultDataPartitioning,
            valuesLoadingJumboSize,
            luceneTopNLimit,
            intermediateLocalRelationMaxSize,
            partialEmitKeysThreshold,
            partialEmitUniquenessThreshold,
            reuseColumnLoadersThreshold
        );
    }

    public double partialEmitUniquenessThreshold() {
        return partialEmitUniquenessThreshold;
    }

    public PlannerSettings reuseColumnLoadersThreshold(int reuseColumnLoadersThreshold) {
        return new PlannerSettings(
            defaultDataPartitioning,
            valuesLoadingJumboSize,
            luceneTopNLimit,
            intermediateLocalRelationMaxSize,
            partialEmitKeysThreshold,
            partialEmitUniquenessThreshold,
            reuseColumnLoadersThreshold
        );
    }

    /**
     * If we're loading more than this many fields at a time we discard column loaders after each
     * page regardless of whether we can reuse them. They have significant per-field memory overhead
     * so discarding them between pages allows some queries that would have OOMed to succeed. Usually
     * the paths that need very high performance don't load more than a handful of fields at a time,
     * so they <strong>do</strong> reuse fields.
     */
    public int reuseColumnLoadersThreshold() {
        return reuseColumnLoadersThreshold;
    }
}
