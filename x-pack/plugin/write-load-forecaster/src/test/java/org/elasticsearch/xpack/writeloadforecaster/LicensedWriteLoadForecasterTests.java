/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.writeloadforecaster;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.allocation.WriteLoadForecaster;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.shard.IndexWriteLoad;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalDouble;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class LicensedWriteLoadForecasterTests extends ESTestCase {
    public void testForecastComputation() {
        final var metadataBuilder = Metadata.builder();
        final int numberOfBackingIndices = 10;
        final int numberOfShards = 1;
        final List<Index> backingIndices = new ArrayList<>();
        final String dataStreamName = "logs-es";
        for (int i = 0; i < numberOfBackingIndices; i++) {
            String dataStreamBackingIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, i);
            IndexMetadata indexMetadata = IndexMetadata.builder(dataStreamBackingIndexName)
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .build()
                )
                .indexWriteLoad(
                    IndexWriteLoad.builder(numberOfShards).withShardWriteLoad(0, 12, TimeValue.timeValueHours(24).millis()).build()
                )
                .creationDate(System.currentTimeMillis())
                .build();
            backingIndices.add(indexMetadata.getIndex());
            metadataBuilder.put(indexMetadata, false);
        }
        final DataStream dataStream = new DataStream(
            dataStreamName,
            backingIndices,
            numberOfBackingIndices,
            Collections.emptyMap(),
            false,
            false,
            false,
            false,
            IndexMode.STANDARD
        );
        metadataBuilder.put(dataStream);
        final var clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadataBuilder).build();

        WriteLoadForecaster writeLoadForecaster = new LicensedWriteLoadForecaster(
            () -> true,
            TimeValue.timeValueDays(7),
            TimeValue.timeValueHours(1)
        );

        ClusterState updatedClusterState = writeLoadForecaster.withWriteLoadForecastForWriteIndex(dataStream.getName(), clusterState);

        IndexMetadata writeIndex = updatedClusterState.metadata().getIndexSafe(dataStream.getWriteIndex());

        OptionalDouble forecastedWriteLoadForShard = writeLoadForecaster.getForecastedWriteLoad(writeIndex);

        assertThat(forecastedWriteLoadForShard.isPresent(), is(true));
        assertThat(forecastedWriteLoadForShard.getAsDouble(), is(equalTo(1.0)));
    }

    public void testForecastComputationFallbacksToSetting() {
        final var metadataBuilder = Metadata.builder();
        final int numberOfBackingIndices = 10;
        final int numberOfShards = 1;
        final List<Index> backingIndices = new ArrayList<>();
        final String dataStreamName = "logs-es";
        for (int i = 0; i < numberOfBackingIndices; i++) {
            String dataStreamBackingIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, i);
            IndexMetadata indexMetadata = IndexMetadata.builder(dataStreamBackingIndexName)
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(WriteLoadForecasterPlugin.DEFAULT_WRITE_LOAD_FORECAST_SETTING.getKey(), 0.5)
                        .build()
                )
                .indexWriteLoad(
                    IndexWriteLoad.builder(numberOfShards).withShardWriteLoad(0, 12, TimeValue.timeValueHours(24).millis()).build()
                )
                .creationDate(System.currentTimeMillis())
                .build();
            backingIndices.add(indexMetadata.getIndex());
            metadataBuilder.put(indexMetadata, false);
        }
        final DataStream dataStream = new DataStream(
            dataStreamName,
            backingIndices,
            numberOfBackingIndices,
            Collections.emptyMap(),
            false,
            false,
            false,
            false,
            IndexMode.STANDARD
        );
        metadataBuilder.put(dataStream);
        final var clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadataBuilder).build();

        WriteLoadForecaster writeLoadForecaster = new LicensedWriteLoadForecaster(
            () -> true,
            TimeValue.timeValueDays(7),
            TimeValue.timeValueHours(1)
        );

        ClusterState updatedClusterState = writeLoadForecaster.withWriteLoadForecastForWriteIndex(dataStream.getName(), clusterState);

        IndexMetadata writeIndex = updatedClusterState.metadata().getIndexSafe(dataStream.getWriteIndex());

        OptionalDouble forecastedWriteLoadForShard = writeLoadForecaster.getForecastedWriteLoad(writeIndex);

        assertThat(forecastedWriteLoadForShard.isPresent(), is(true));
        assertThat(forecastedWriteLoadForShard.getAsDouble(), is(equalTo(0.5)));
    }
}
