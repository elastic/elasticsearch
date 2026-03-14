/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.writeloadforecaster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.allocation.WriteLoadForecaster;
import org.elasticsearch.index.Index;
import org.elasticsearch.injection.guice.Inject;

import java.util.Map;
import java.util.OptionalDouble;
import java.util.stream.Collectors;

/**
 * A write-load forecaster that takes its forecasts from the most recent {@link ClusterInfo} object.
 */
public class ClusterInfoWriteLoadForecaster implements WriteLoadForecaster {

    private static final Logger logger = LogManager.getLogger(ClusterInfoWriteLoadForecaster.class);

    private volatile Map<Index, Double> indexWriteLoadForecasts = Map.of();

    @Inject
    public void setClusterInfoService(ClusterInfoService clusterInfoService) {
        logger.info("Registering cluster info listener");
        clusterInfoService.addListener(this::onNewClusterInfo);
    }

    /**
     * We take the largest write-load we see for each index as indicative of the write-load
     */
    private void onNewClusterInfo(ClusterInfo newClusterInfo) {
        this.indexWriteLoadForecasts = newClusterInfo.getShardWriteLoads()
            .entrySet()
            .stream()
            .collect(Collectors.toUnmodifiableMap(e -> e.getKey().getIndex(), Map.Entry::getValue, Double::max));
    }

    @Override
    public ProjectMetadata.Builder withWriteLoadForecastForWriteIndex(String dataStreamName, ProjectMetadata.Builder metadata) {
        // We don't persist these write-loads
        return metadata;
    }

    @Override
    public OptionalDouble getForecastedWriteLoad(IndexMetadata indexMetadata) {
        Double writeLoad = indexWriteLoadForecasts.get(indexMetadata.getIndex());
        return writeLoad == null ? OptionalDouble.empty() : OptionalDouble.of(writeLoad);
    }

    @Override
    public void refreshLicense() {
        // No-op, serverless only
    }
}
