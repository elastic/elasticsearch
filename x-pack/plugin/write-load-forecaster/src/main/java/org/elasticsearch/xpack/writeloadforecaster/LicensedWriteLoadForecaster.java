/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.writeloadforecaster;

import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadataStats;
import org.elasticsearch.cluster.metadata.IndexWriteLoad;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.allocation.WriteLoadForecaster;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.function.BooleanSupplier;

import static org.elasticsearch.xpack.writeloadforecaster.WriteLoadForecasterPlugin.OVERRIDE_WRITE_LOAD_FORECAST_SETTING;

class LicensedWriteLoadForecaster implements WriteLoadForecaster {
    public static final Setting<TimeValue> MAX_INDEX_AGE_SETTING = Setting.timeSetting(
        "write_load_forecaster.max_index_age",
        TimeValue.timeValueDays(7),
        TimeValue.timeValueHours(1),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    private final BooleanSupplier hasValidLicense;
    private final ThreadPool threadPool;
    private volatile TimeValue maxIndexAge;

    LicensedWriteLoadForecaster(
        BooleanSupplier hasValidLicense,
        ThreadPool threadPool,
        Settings settings,
        ClusterSettings clusterSettings
    ) {
        this(hasValidLicense, threadPool, MAX_INDEX_AGE_SETTING.get(settings));
        clusterSettings.addSettingsUpdateConsumer(MAX_INDEX_AGE_SETTING, this::setMaxIndexAgeSetting);
    }

    // exposed for tests only
    LicensedWriteLoadForecaster(BooleanSupplier hasValidLicense, ThreadPool threadPool, TimeValue maxIndexAge) {
        this.hasValidLicense = hasValidLicense;
        this.threadPool = threadPool;
        this.maxIndexAge = maxIndexAge;
    }

    private void setMaxIndexAgeSetting(TimeValue updatedMaxIndexAge) {
        this.maxIndexAge = updatedMaxIndexAge;
    }

    @Override
    public Metadata.Builder withWriteLoadForecastForWriteIndex(String dataStreamName, Metadata.Builder metadata) {
        if (hasValidLicense.getAsBoolean() == false) {
            return metadata;
        }

        final DataStream dataStream = metadata.dataStream(dataStreamName);

        if (dataStream == null) {
            return metadata;
        }

        clearPreviousForecast(dataStream, metadata);

        final List<IndexWriteLoad> indicesWriteLoadWithinMaxAgeRange = getIndicesWithinMaxAgeRange(dataStream, metadata).stream()
            .filter(index -> index.equals(dataStream.getWriteIndex()) == false)
            .map(metadata::getSafe)
            .map(IndexMetadata::getStats)
            .filter(Objects::nonNull)
            .map(IndexMetadataStats::writeLoad)
            .filter(Objects::nonNull)
            .toList();

        OptionalDouble forecastIndexWriteLoad = forecastIndexWriteLoad(indicesWriteLoadWithinMaxAgeRange);

        if (forecastIndexWriteLoad.isEmpty()) {
            return metadata;
        }

        final IndexMetadata writeIndex = metadata.getSafe(dataStream.getWriteIndex());
        metadata.put(IndexMetadata.builder(writeIndex).indexWriteLoadForecast(forecastIndexWriteLoad.getAsDouble()).build(), false);

        return metadata;
    }

    private void clearPreviousForecast(DataStream dataStream, Metadata.Builder metadata) {
        if (dataStream.getIndices().size() > 1) {
            final Index previousWriteIndex = dataStream.getIndices().get(dataStream.getIndices().size() - 2);
            final IndexMetadata previousWriteIndexMetadata = metadata.getSafe(previousWriteIndex);
            final IndexMetadata.Builder previousWriteIndexMetadataBuilder = IndexMetadata.builder(previousWriteIndexMetadata)
                .indexWriteLoadForecast(null);
            if (previousWriteIndexMetadata.getSettings().hasValue(OVERRIDE_WRITE_LOAD_FORECAST_SETTING.getKey())) {
                Settings.Builder previousWriteIndexSettings = Settings.builder().put(previousWriteIndexMetadata.getSettings());
                previousWriteIndexSettings.remove(OVERRIDE_WRITE_LOAD_FORECAST_SETTING.getKey());
                previousWriteIndexMetadataBuilder.settings(previousWriteIndexSettings);
                previousWriteIndexMetadataBuilder.settingsVersion(previousWriteIndexMetadata.getSettingsVersion() + 1);
            }
            metadata.put(previousWriteIndexMetadataBuilder.build(), false);
        }
    }

    // Visible for testing
    static OptionalDouble forecastIndexWriteLoad(List<IndexWriteLoad> indicesWriteLoadWithinMaxAgeRange) {
        double totalWeightedWriteLoad = 0;
        long totalShardUptime = 0;
        for (IndexWriteLoad writeLoad : indicesWriteLoadWithinMaxAgeRange) {
            for (int shardId = 0; shardId < writeLoad.numberOfShards(); shardId++) {
                final OptionalDouble writeLoadForShard = writeLoad.getWriteLoadForShard(shardId);
                final OptionalLong uptimeInMillisForShard = writeLoad.getUptimeInMillisForShard(shardId);
                if (writeLoadForShard.isPresent()) {
                    assert uptimeInMillisForShard.isPresent();
                    double shardWriteLoad = writeLoadForShard.getAsDouble();
                    long shardUptimeInMillis = uptimeInMillisForShard.getAsLong();
                    totalWeightedWriteLoad += shardWriteLoad * shardUptimeInMillis;
                    totalShardUptime += shardUptimeInMillis;
                }
            }
        }

        return totalShardUptime == 0 ? OptionalDouble.empty() : OptionalDouble.of(totalWeightedWriteLoad / totalShardUptime);
    }

    // Visible for testing
    List<Index> getIndicesWithinMaxAgeRange(DataStream dataStream, Metadata.Builder metadata) {
        final List<Index> dataStreamIndices = dataStream.getIndices();
        final long currentTimeMillis = threadPool.absoluteTimeInMillis();
        // Consider at least 1 index (including the write index) for cases where rollovers happen less often than maxIndexAge
        int firstIndexWithinAgeRange = Math.max(dataStreamIndices.size() - 2, 0);
        for (int i = 0; i < dataStreamIndices.size(); i++) {
            Index index = dataStreamIndices.get(i);
            final IndexMetadata indexMetadata = metadata.getSafe(index);
            final long indexAge = currentTimeMillis - indexMetadata.getCreationDate();
            if (indexAge < maxIndexAge.getMillis()) {
                // We need to consider the previous index too in order to cover the entire max-index-age range.
                firstIndexWithinAgeRange = i == 0 ? 0 : i - 1;
                break;
            }
        }
        return dataStreamIndices.subList(firstIndexWithinAgeRange, dataStreamIndices.size());
    }

    @Override
    @SuppressForbidden(reason = "This is the only place where IndexMetadata#getForecastedWriteLoad is allowed to be used")
    public OptionalDouble getForecastedWriteLoad(IndexMetadata indexMetadata) {
        if (hasValidLicense.getAsBoolean() == false) {
            return OptionalDouble.empty();
        }

        if (OVERRIDE_WRITE_LOAD_FORECAST_SETTING.exists(indexMetadata.getSettings())) {
            Double overrideWriteLoadForecast = OVERRIDE_WRITE_LOAD_FORECAST_SETTING.get(indexMetadata.getSettings());
            return OptionalDouble.of(overrideWriteLoadForecast);
        }

        return indexMetadata.getForecastedWriteLoad();
    }
}
