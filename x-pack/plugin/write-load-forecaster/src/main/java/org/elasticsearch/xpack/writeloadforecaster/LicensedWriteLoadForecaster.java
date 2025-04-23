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
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.allocation.WriteLoadForecaster;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.List;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.function.BooleanSupplier;

import static org.elasticsearch.xpack.writeloadforecaster.WriteLoadForecasterPlugin.OVERRIDE_WRITE_LOAD_FORECAST_SETTING;

class LicensedWriteLoadForecaster implements WriteLoadForecaster {

    private static final Logger logger = LogManager.getLogger(LicensedWriteLoadForecaster.class);

    public static final Setting<TimeValue> MAX_INDEX_AGE_SETTING = Setting.timeSetting(
        "write_load_forecaster.max_index_age",
        TimeValue.timeValueDays(7),
        TimeValue.timeValueHours(1),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    private final BooleanSupplier hasValidLicenseSupplier;
    private final ThreadPool threadPool;
    private volatile TimeValue maxIndexAge;

    @SuppressWarnings("unused") // modified via VH_HAS_VALID_LICENSE_FIELD
    private volatile boolean hasValidLicense;

    LicensedWriteLoadForecaster(
        BooleanSupplier hasValidLicenseSupplier,
        ThreadPool threadPool,
        Settings settings,
        ClusterSettings clusterSettings
    ) {
        this(hasValidLicenseSupplier, threadPool, MAX_INDEX_AGE_SETTING.get(settings));
        clusterSettings.addSettingsUpdateConsumer(MAX_INDEX_AGE_SETTING, this::setMaxIndexAgeSetting);
    }

    // exposed for tests only
    LicensedWriteLoadForecaster(BooleanSupplier hasValidLicenseSupplier, ThreadPool threadPool, TimeValue maxIndexAge) {
        this.hasValidLicenseSupplier = hasValidLicenseSupplier;
        this.threadPool = threadPool;
        this.maxIndexAge = maxIndexAge;
    }

    private void setMaxIndexAgeSetting(TimeValue updatedMaxIndexAge) {
        this.maxIndexAge = updatedMaxIndexAge;
    }

    @Override
    public ProjectMetadata.Builder withWriteLoadForecastForWriteIndex(String dataStreamName, ProjectMetadata.Builder metadata) {
        if (hasValidLicense == false) {
            return metadata;
        }

        final DataStream dataStream = metadata.dataStream(dataStreamName);

        if (dataStream == null) {
            return metadata;
        }

        clearPreviousForecast(dataStream, metadata);

        final List<IndexWriteLoad> indicesWriteLoadWithinMaxAgeRange = DataStream.getIndicesWithinMaxAgeRange(
            dataStream,
            metadata::getSafe,
            maxIndexAge,
            threadPool::absoluteTimeInMillis
        )
            .stream()
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

    private static void clearPreviousForecast(DataStream dataStream, ProjectMetadata.Builder metadata) {
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

    @Override
    @SuppressForbidden(reason = "This is the only place where IndexMetadata#getForecastedWriteLoad is allowed to be used")
    public OptionalDouble getForecastedWriteLoad(IndexMetadata indexMetadata) {
        if (hasValidLicense == false) {
            return OptionalDouble.empty();
        }

        if (OVERRIDE_WRITE_LOAD_FORECAST_SETTING.exists(indexMetadata.getSettings())) {
            Double overrideWriteLoadForecast = OVERRIDE_WRITE_LOAD_FORECAST_SETTING.get(indexMetadata.getSettings());
            return OptionalDouble.of(overrideWriteLoadForecast);
        }

        return indexMetadata.getForecastedWriteLoad();
    }

    /**
     * Used to atomically {@code getAndSet()} the {@link #hasValidLicense} field. This is better than an
     * {@link java.util.concurrent.atomic.AtomicBoolean} because it takes one less pointer dereference on each read.
     */
    private static final VarHandle VH_HAS_VALID_LICENSE_FIELD;

    static {
        try {
            VH_HAS_VALID_LICENSE_FIELD = MethodHandles.lookup()
                .in(LicensedWriteLoadForecaster.class)
                .findVarHandle(LicensedWriteLoadForecaster.class, "hasValidLicense", boolean.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void refreshLicense() {
        final var newValue = hasValidLicenseSupplier.getAsBoolean();
        final var oldValue = (boolean) VH_HAS_VALID_LICENSE_FIELD.getAndSet(this, newValue);
        if (newValue != oldValue) {
            logger.info("license state changed, now [{}]", newValue ? "valid" : "not valid");
        }
    }
}
