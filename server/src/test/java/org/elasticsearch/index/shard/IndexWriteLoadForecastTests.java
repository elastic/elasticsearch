/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalDouble;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class IndexWriteLoadForecastTests extends ESTestCase {
    public void testForecastComputation() {
        final var metadataBuilder = Metadata.builder();
        final int numberOfBackingIndices = 10;
        final int numberOfShards = 1;
        final List<Index> backingIndices = new ArrayList<>();
        final String dataStreamName = "my-ds";
        for (int i = 0; i < numberOfBackingIndices; i++) {
            String dataStreamBackingIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, i);
            IndexMetadata indexMetadata = IndexMetadata.builder(dataStreamBackingIndexName)
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexSettings.FORECAST_WRITE_LOAD_SETTING.getKey(), true)
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
        var ds = new DataStream(
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
        metadataBuilder.put(ds);
        final var clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadataBuilder).build();
        final IndexAbstraction.DataStream dataStream = (IndexAbstraction.DataStream) clusterState.metadata()
            .getIndicesLookup()
            .get(dataStreamName);

        final var updatedClusterState = IndexWriteLoadForecast.maybeIncludeWriteLoadForecast(
            dataStream,
            clusterState,
            TimeValue.timeValueDays(7),
            TimeValue.timeValueHours(8)
        );

        IndexMetadata writeIndex = updatedClusterState.metadata().getIndexSafe(dataStream.getWriteIndex());
        OptionalDouble forecastedWriteLoadForShard = writeIndex.getForecastedWriteLoadForShard(0);

        assertThat(forecastedWriteLoadForShard.isPresent(), is(true));
        assertThat(forecastedWriteLoadForShard.getAsDouble(), is(equalTo(1.0)));
    }

    public void testForecastComputationFallbacksToSetting() {
        final var metadataBuilder = Metadata.builder();
        final int numberOfBackingIndices = 10;
        final int numberOfShards = 1;
        final List<Index> backingIndices = new ArrayList<>();
        final String dataStreamName = "my-ds";
        for (int i = 0; i < numberOfBackingIndices; i++) {
            String dataStreamBackingIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, i);
            IndexMetadata indexMetadata = IndexMetadata.builder(dataStreamBackingIndexName)
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexSettings.FORECAST_WRITE_LOAD_SETTING.getKey(), true)
                        .put(IndexSettings.DEFAULT_INTERNAL_WRITE_LOAD_SETTING.getKey(), 0.5)
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
        var ds = new DataStream(
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
        metadataBuilder.put(ds);
        final var clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadataBuilder).build();
        final IndexAbstraction.DataStream dataStream = (IndexAbstraction.DataStream) clusterState.metadata()
            .getIndicesLookup()
            .get(dataStreamName);

        final var updatedClusterState = IndexWriteLoadForecast.maybeIncludeWriteLoadForecast(
            dataStream,
            clusterState,
            TimeValue.timeValueDays(7),
            TimeValue.timeValueHours(8)
        );

        IndexMetadata writeIndex = updatedClusterState.metadata().getIndexSafe(dataStream.getWriteIndex());
        OptionalDouble forecastedWriteLoadForShard = writeIndex.getForecastedWriteLoadForShard(0);

        assertThat(forecastedWriteLoadForShard.isPresent(), is(true));
        assertThat(forecastedWriteLoadForShard.getAsDouble(), is(equalTo(0.5)));
    }

}
