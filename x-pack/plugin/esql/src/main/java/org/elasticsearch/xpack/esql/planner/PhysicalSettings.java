/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.MemorySizeValue;
import org.elasticsearch.compute.lucene.DataPartitioning;
import org.elasticsearch.monitor.jvm.JvmInfo;

/**
 * Values for cluster level settings used in physical planning.
 */
public class PhysicalSettings {
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

    private volatile DataPartitioning defaultDataPartitioning;
    private volatile ByteSizeValue valuesLoadingJumboSize;

    /**
     * Ctor for prod that listens for updates from the {@link ClusterService}.
     */
    public PhysicalSettings(ClusterService clusterService) {
        clusterService.getClusterSettings().initializeAndWatch(DEFAULT_DATA_PARTITIONING, v -> this.defaultDataPartitioning = v);
        clusterService.getClusterSettings().initializeAndWatch(VALUES_LOADING_JUMBO_SIZE, v -> this.valuesLoadingJumboSize = v);
    }

    /**
     * Ctor for testing.
     */
    public PhysicalSettings(DataPartitioning defaultDataPartitioning, ByteSizeValue valuesLoadingJumboSize) {
        this.defaultDataPartitioning = defaultDataPartitioning;
        this.valuesLoadingJumboSize = valuesLoadingJumboSize;
    }

    public DataPartitioning defaultDataPartitioning() {
        return defaultDataPartitioning;
    }

    public ByteSizeValue valuesLoadingJumboSize() {
        return valuesLoadingJumboSize;
    }
}
