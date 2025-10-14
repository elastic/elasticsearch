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

    private volatile DataPartitioning defaultDataPartitioning;
    private volatile ByteSizeValue valuesLoadingJumboSize;
    private volatile int luceneTopNLimit;
    private volatile ByteSizeValue intermediateLocalRelationMaxSize;

    /**
     * Ctor for prod that listens for updates from the {@link ClusterService}.
     */
    public PlannerSettings(ClusterService clusterService) {
        var clusterSettings = clusterService.getClusterSettings();
        clusterSettings.initializeAndWatch(DEFAULT_DATA_PARTITIONING, v -> this.defaultDataPartitioning = v);
        clusterSettings.initializeAndWatch(VALUES_LOADING_JUMBO_SIZE, v -> this.valuesLoadingJumboSize = v);
        clusterSettings.initializeAndWatch(LUCENE_TOPN_LIMIT, v -> this.luceneTopNLimit = v);
        clusterSettings.initializeAndWatch(INTERMEDIATE_LOCAL_RELATION_MAX_SIZE, v -> this.intermediateLocalRelationMaxSize = v);
    }

    /**
     * Ctor for testing.
     */
    public PlannerSettings(
        DataPartitioning defaultDataPartitioning,
        ByteSizeValue valuesLoadingJumboSize,
        int luceneTopNLimit,
        ByteSizeValue intermediateLocalRelationMaxSize
    ) {
        this.defaultDataPartitioning = defaultDataPartitioning;
        this.valuesLoadingJumboSize = valuesLoadingJumboSize;
        this.luceneTopNLimit = luceneTopNLimit;
        this.intermediateLocalRelationMaxSize = intermediateLocalRelationMaxSize;
    }

    public DataPartitioning defaultDataPartitioning() {
        return defaultDataPartitioning;
    }

    public ByteSizeValue valuesLoadingJumboSize() {
        return valuesLoadingJumboSize;
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

    public ByteSizeValue intermediateLocalRelationMaxSize() {
        return intermediateLocalRelationMaxSize;
    }
}
