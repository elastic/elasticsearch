/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.writeloadforecaster;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.index.Index;

import java.util.Map;
import java.util.OptionalDouble;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

/**
 * A write-load forecaster that takes its forecasts from the most recent {@link ClusterInfo} object.
 */
public class ClusterInfoWriteLoadForecaster extends AbstractLicenseCheckingWriteLoadForecaster {

    private volatile Map<Index, Double> indexWriteLoadForecasts = Map.of();

    public ClusterInfoWriteLoadForecaster(BooleanSupplier licenseSupplier) {
        super(licenseSupplier);
    }

    /**
     * We take the largest write-load we see for each index as indicative of the write-load
     */
    public void onNewClusterInfo(ClusterInfo newClusterInfo) {
        if (newClusterInfo == null) {
            return;
        }
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
        if (hasValidLicense == false) {
            return OptionalDouble.empty();
        }

        Double writeLoad = indexWriteLoadForecasts.get(indexMetadata.getIndex());
        return writeLoad == null ? OptionalDouble.empty() : OptionalDouble.of(writeLoad);
    }
}
