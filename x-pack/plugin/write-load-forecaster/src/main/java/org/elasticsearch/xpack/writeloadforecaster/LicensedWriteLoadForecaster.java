/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.writeloadforecaster;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.allocation.WriteLoadForecaster;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexWriteLoad;

import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.function.BooleanSupplier;

class LicensedWriteLoadForecaster implements WriteLoadForecaster {
    public static final Setting<TimeValue> MAX_INDEX_AGE_SETTING = Setting.timeSetting(
        "write_load_forecaster.max_index_age",
        TimeValue.timeValueDays(7),
        TimeValue.timeValueHours(1),
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> MIN_UPTIME_SETTING = Setting.timeSetting(
        "write_load_forecaster.min_uptime",
        TimeValue.timeValueHours(1),
        TimeValue.timeValueMinutes(1),
        Setting.Property.NodeScope
    );

    private final BooleanSupplier hasValidLicense;
    private volatile TimeValue maxIndexAge;
    private volatile TimeValue minShardUptime;

    LicensedWriteLoadForecaster(BooleanSupplier hasValidLicense, Settings settings, ClusterSettings clusterSettings) {
        this(hasValidLicense, MAX_INDEX_AGE_SETTING.get(settings), MIN_UPTIME_SETTING.get(settings));
        clusterSettings.addSettingsUpdateConsumer(MAX_INDEX_AGE_SETTING, this::setMaxIndexAgeSetting);
        clusterSettings.addSettingsUpdateConsumer(MIN_UPTIME_SETTING, this::setMinUptimeSetting);
    }

    LicensedWriteLoadForecaster(BooleanSupplier hasValidLicense, TimeValue maxIndexAge, TimeValue minShardUptime) {
        this.hasValidLicense = hasValidLicense;
        this.maxIndexAge = maxIndexAge;
        this.minShardUptime = minShardUptime;
    }

    private void setMaxIndexAgeSetting(TimeValue updatedMaxIndexAge) {
        this.maxIndexAge = updatedMaxIndexAge;
    }

    private void setMinUptimeSetting(TimeValue updatedMinShardUptime) {
        this.minShardUptime = updatedMinShardUptime;
    }

    @Override
    public ClusterState withWriteLoadForecastForWriteIndex(String dataStreamName, ClusterState clusterState) {
        if (hasValidLicense.getAsBoolean() == false) {
            return clusterState;
        }

        DataStream dataStream = clusterState.metadata().dataStreams().get(dataStreamName);

        if (dataStream == null) {
            return clusterState;
        }

        double maxWriteLoadAvg = Double.MIN_VALUE;
        double totalWriteLoadAvg = 0;
        double totalIndices = 0;

        for (Index index : dataStream.getIndices()) {
            final IndexMetadata indexMetadata = clusterState.metadata().getIndexSafe(index);
            final long indexAge = System.currentTimeMillis() - indexMetadata.getCreationDate();
            final IndexWriteLoad writeLoad = indexMetadata.getWriteLoad();

            if (index.equals(dataStream.getWriteIndex()) || indexAge > maxIndexAge.millis() || writeLoad == null) {
                continue;
            }

            double totalIndexWriteLoad = 0;
            int totalShards = 0;
            for (int shardId = 0; shardId < indexMetadata.getNumberOfShards(); shardId++) {
                final OptionalDouble writeLoadForShard = writeLoad.getWriteLoadForShard(shardId);
                final OptionalLong uptimeInMillisForShard = writeLoad.getUptimeInMillisForShard(shardId);
                if (writeLoadForShard.isPresent()) {
                    assert uptimeInMillisForShard.isPresent();
                    double shardWriteLoad = writeLoadForShard.getAsDouble();
                    long shardUptimeInMillis = uptimeInMillisForShard.getAsLong();
                    if (shardUptimeInMillis > minShardUptime.millis()) {
                        totalIndexWriteLoad += shardWriteLoad;
                        totalShards++;
                    }
                }

                if (totalShards > 0) {
                    final double indexWriteLoadAvg = totalIndexWriteLoad / totalShards;
                    totalWriteLoadAvg += indexWriteLoadAvg;
                    maxWriteLoadAvg = Math.max(maxWriteLoadAvg, indexWriteLoadAvg);
                    totalIndices++;
                }
            }

        }

        if (totalIndices == 0) {
            return clusterState;
        }

        final double normalizedIndexWriteLoad = (totalWriteLoadAvg / totalIndices) / maxWriteLoadAvg;
        final IndexMetadata writeIndex = clusterState.metadata().getIndexSafe(dataStream.getWriteIndex());

        return ClusterState.builder(clusterState)
            .metadata(
                Metadata.builder(clusterState.metadata())
                    .put(IndexMetadata.builder(writeIndex).indexWriteLoadForecast(normalizedIndexWriteLoad))
            )
            .build();
    }

    @Override
    public OptionalDouble getForecastedWriteLoad(IndexMetadata indexMetadata) {
        if (hasValidLicense.getAsBoolean() == false) {
            return OptionalDouble.empty();
        }

        if (WriteLoadForecasterPlugin.DEFAULT_WRITE_LOAD_FORECAST_SETTING.exists(indexMetadata.getSettings())) {
            Double defaultWriteLoad = WriteLoadForecasterPlugin.DEFAULT_WRITE_LOAD_FORECAST_SETTING.get(indexMetadata.getSettings());
            return OptionalDouble.of(defaultWriteLoad);
        }

        return indexMetadata.getForecastedWriteLoadForShard();
    }
}
