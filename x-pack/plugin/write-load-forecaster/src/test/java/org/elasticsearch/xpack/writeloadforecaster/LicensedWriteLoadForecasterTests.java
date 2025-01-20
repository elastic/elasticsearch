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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.writeloadforecaster.LicensedWriteLoadForecaster.forecastIndexWriteLoad;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class LicensedWriteLoadForecasterTests extends ESTestCase {
    ThreadPool threadPool;

    @Before
    public void setUpThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void tearDownThreadPool() {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testWriteLoadForecastIsAddedToWriteIndex() {
        final TimeValue maxIndexAge = TimeValue.timeValueDays(7);
        final AtomicBoolean hasValidLicense = new AtomicBoolean(true);
        final WriteLoadForecaster writeLoadForecaster = new LicensedWriteLoadForecaster(hasValidLicense::get, threadPool, maxIndexAge);

        final Metadata.Builder metadataBuilder = Metadata.builder();
        final String dataStreamName = "logs-es";
        final int numberOfBackingIndices = 10;
        final int numberOfShards = randomIntBetween(1, 5);
        final List<Index> backingIndices = new ArrayList<>();
        for (int i = 0; i < numberOfBackingIndices; i++) {
            final IndexMetadata indexMetadata = createIndexMetadata(
                DataStream.getDefaultBackingIndexName(dataStreamName, i),
                numberOfShards,
                randomIndexWriteLoad(numberOfShards),
                System.currentTimeMillis() - (maxIndexAge.millis() / 2)
            );
            backingIndices.add(indexMetadata.getIndex());
            metadataBuilder.put(indexMetadata, false);
        }

        final IndexMetadata writeIndexMetadata = createIndexMetadata(
            DataStream.getDefaultBackingIndexName(dataStreamName, numberOfBackingIndices),
            numberOfShards,
            null,
            System.currentTimeMillis()
        );
        backingIndices.add(writeIndexMetadata.getIndex());
        metadataBuilder.put(writeIndexMetadata, false);

        final DataStream dataStream = createDataStream(dataStreamName, backingIndices);
        metadataBuilder.put(dataStream);

        final Metadata.Builder updatedMetadataBuilder = writeLoadForecaster.withWriteLoadForecastForWriteIndex(
            dataStream.getName(),
            metadataBuilder
        );

        final IndexMetadata writeIndex = updatedMetadataBuilder.getSafe(dataStream.getWriteIndex());

        final OptionalDouble forecastedWriteLoad = writeLoadForecaster.getForecastedWriteLoad(writeIndex);

        assertThat(forecastedWriteLoad.isPresent(), is(true));
        assertThat(forecastedWriteLoad.getAsDouble(), is(greaterThan(0.0)));

        hasValidLicense.set(false);

        final OptionalDouble forecastedWriteLoadAfterLicenseChange = writeLoadForecaster.getForecastedWriteLoad(writeIndex);
        assertThat(forecastedWriteLoadAfterLicenseChange.isPresent(), is(false));
    }

    public void testUptimeIsUsedToWeightWriteLoad() {
        final TimeValue maxIndexAge = TimeValue.timeValueDays(7);
        final var metadataBuilder = Metadata.builder();
        final String dataStreamName = "logs-es";
        final int numberOfShards = 5;
        final List<Index> backingIndices = new ArrayList<>();
        // Weighted avg 14.4
        final IndexMetadata indexMetadata = createIndexMetadata(
            DataStream.getDefaultBackingIndexName(dataStreamName, 0),
            numberOfShards,
            IndexWriteLoad.builder(numberOfShards)
                .withShardWriteLoad(0, 12, 80)
                .withShardWriteLoad(1, 24, 5)
                .withShardWriteLoad(2, 24, 5)
                .withShardWriteLoad(3, 24, 5)
                .withShardWriteLoad(4, 24, 5)
                .build(),
            System.currentTimeMillis() - (maxIndexAge.millis() / 2)
        );
        backingIndices.add(indexMetadata.getIndex());
        metadataBuilder.put(indexMetadata, false);

        final IndexMetadata writeIndexMetadata = createIndexMetadata(
            DataStream.getDefaultBackingIndexName(dataStreamName, 1),
            numberOfShards,
            null,
            System.currentTimeMillis()
        );
        backingIndices.add(writeIndexMetadata.getIndex());
        metadataBuilder.put(writeIndexMetadata, false);

        final DataStream dataStream = createDataStream(dataStreamName, backingIndices);
        metadataBuilder.put(dataStream);

        final WriteLoadForecaster writeLoadForecaster = new LicensedWriteLoadForecaster(() -> true, threadPool, maxIndexAge);

        final Metadata.Builder updatedMetadataBuilder = writeLoadForecaster.withWriteLoadForecastForWriteIndex(
            dataStream.getName(),
            metadataBuilder
        );

        final IndexMetadata writeIndex = updatedMetadataBuilder.getSafe(dataStream.getWriteIndex());

        final OptionalDouble forecastedWriteLoad = writeLoadForecaster.getForecastedWriteLoad(writeIndex);

        assertThat(forecastedWriteLoad.isPresent(), is(true));
        assertThat(forecastedWriteLoad.getAsDouble(), is(equalTo(14.4)));
    }

    public void testForecastedWriteLoadIsOverriddenBySetting() {
        final TimeValue maxIndexAge = TimeValue.timeValueDays(7);
        final AtomicBoolean hasValidLicense = new AtomicBoolean(true);
        final WriteLoadForecaster writeLoadForecaster = new LicensedWriteLoadForecaster(hasValidLicense::get, threadPool, maxIndexAge);

        final Metadata.Builder metadataBuilder = Metadata.builder();
        final String dataStreamName = "logs-es";
        final int numberOfBackingIndices = 10;
        final int numberOfShards = randomIntBetween(1, 5);
        final List<Index> backingIndices = new ArrayList<>();
        for (int i = 0; i < numberOfBackingIndices; i++) {
            final IndexMetadata indexMetadata = createIndexMetadata(
                DataStream.getDefaultBackingIndexName(dataStreamName, i),
                numberOfShards,
                randomIndexWriteLoad(numberOfShards),
                System.currentTimeMillis() - (maxIndexAge.millis() / 2)
            );
            backingIndices.add(indexMetadata.getIndex());
            metadataBuilder.put(indexMetadata, false);
        }

        final IndexMetadata writeIndexMetadata = createIndexMetadata(
            DataStream.getDefaultBackingIndexName(dataStreamName, numberOfBackingIndices),
            numberOfShards,
            null,
            System.currentTimeMillis(),
            Settings.builder().put(WriteLoadForecasterPlugin.OVERRIDE_WRITE_LOAD_FORECAST_SETTING.getKey(), 0.6).build()
        );
        backingIndices.add(writeIndexMetadata.getIndex());
        metadataBuilder.put(writeIndexMetadata, false);

        final DataStream dataStream = createDataStream(dataStreamName, backingIndices);
        metadataBuilder.put(dataStream);

        final Metadata.Builder updatedMetadataBuilder = writeLoadForecaster.withWriteLoadForecastForWriteIndex(
            dataStream.getName(),
            metadataBuilder
        );

        final IndexMetadata writeIndex = updatedMetadataBuilder.getSafe(dataStream.getWriteIndex());

        final OptionalDouble forecastedWriteLoad = writeLoadForecaster.getForecastedWriteLoad(writeIndex);

        assertThat(forecastedWriteLoad.isPresent(), is(true));
        assertThat(forecastedWriteLoad.getAsDouble(), is(equalTo(0.6)));

        hasValidLicense.set(false);

        final OptionalDouble forecastedWriteLoadAfterLicenseChange = writeLoadForecaster.getForecastedWriteLoad(writeIndex);
        assertThat(forecastedWriteLoadAfterLicenseChange.isPresent(), is(false));
    }

    public void testWriteLoadForecast() {
        {
            OptionalDouble writeLoadForecast = forecastIndexWriteLoad(List.of());
            assertThat(writeLoadForecast.isEmpty(), is(true));
        }

        {
            OptionalDouble writeLoadForecast = forecastIndexWriteLoad(List.of(IndexWriteLoad.builder(5).build()));
            assertThat(writeLoadForecast.isEmpty(), is(true));
        }

        {
            OptionalDouble writeLoadForecast = forecastIndexWriteLoad(
                List.of(IndexWriteLoad.builder(1).withShardWriteLoad(0, 12, 100).build())
            );
            assertThat(writeLoadForecast.isPresent(), is(true));
            assertThat(writeLoadForecast.getAsDouble(), is(equalTo(12.0)));
        }

        {
            OptionalDouble writeLoadForecast = forecastIndexWriteLoad(
                List.of(
                    IndexWriteLoad.builder(5)
                        .withShardWriteLoad(0, 12, 80)
                        .withShardWriteLoad(1, 24, 5)
                        .withShardWriteLoad(2, 24, 5)
                        .withShardWriteLoad(3, 24, 5)
                        .withShardWriteLoad(4, 24, 5)
                        .build()
                )
            );
            assertThat(writeLoadForecast.isPresent(), is(true));
            assertThat(writeLoadForecast.getAsDouble(), is(equalTo(14.4)));
        }

        {
            OptionalDouble writeLoadForecast = forecastIndexWriteLoad(
                List.of(
                    IndexWriteLoad.builder(5)
                        .withShardWriteLoad(0, 12, 80)
                        .withShardWriteLoad(1, 24, 5)
                        .withShardWriteLoad(2, 24, 5)
                        .withShardWriteLoad(3, 24, 5)
                        .withShardWriteLoad(4, 24, 4)
                        .build(),
                    // Since this shard uptime is really low, it doesn't add much to the avg
                    IndexWriteLoad.builder(1).withShardWriteLoad(0, 120, 1).build()
                )
            );
            assertThat(writeLoadForecast.isPresent(), is(true));
            assertThat(writeLoadForecast.getAsDouble(), is(equalTo(15.36)));
        }

        {
            OptionalDouble writeLoadForecast = forecastIndexWriteLoad(
                List.of(
                    IndexWriteLoad.builder(2).withShardWriteLoad(0, 12, 25).withShardWriteLoad(1, 12, 25).build(),

                    IndexWriteLoad.builder(1).withShardWriteLoad(0, 12, 50).build()
                )
            );
            assertThat(writeLoadForecast.isPresent(), is(true));
            assertThat(writeLoadForecast.getAsDouble(), is(equalTo(12.0)));
        }

        {
            // All indices have the same uptime, therefore it's just a regular avg
            OptionalDouble writeLoadForecast = forecastIndexWriteLoad(
                List.of(
                    IndexWriteLoad.builder(3)
                        .withShardWriteLoad(0, 25, 1)
                        .withShardWriteLoad(1, 18, 1)
                        .withShardWriteLoad(2, 23, 1)
                        .build(),

                    IndexWriteLoad.builder(2).withShardWriteLoad(0, 6, 1).withShardWriteLoad(1, 8, 1).build(),

                    IndexWriteLoad.builder(1).withShardWriteLoad(0, 15, 1).build()
                )
            );
            assertThat(writeLoadForecast.isPresent(), is(true));
            assertThat(writeLoadForecast.getAsDouble(), is(closeTo(15.83, 0.01)));
        }
    }

    private IndexWriteLoad randomIndexWriteLoad(int numberOfShards) {
        IndexWriteLoad.Builder builder = IndexWriteLoad.builder(numberOfShards);
        for (int shardId = 0; shardId < numberOfShards; shardId++) {
            builder.withShardWriteLoad(shardId, randomDoubleBetween(0, 64, true), randomLongBetween(1, 10));
        }
        return builder.build();
    }

    private IndexMetadata createIndexMetadata(String indexName, int numberOfShards, IndexWriteLoad indexWriteLoad, long createdAt) {
        return createIndexMetadata(indexName, numberOfShards, indexWriteLoad, createdAt, Settings.EMPTY);
    }

    private IndexMetadata createIndexMetadata(
        String indexName,
        int numberOfShards,
        IndexWriteLoad indexWriteLoad,
        long createdAt,
        Settings extraSettings
    ) {
        return IndexMetadata.builder(indexName)
            .settings(
                Settings.builder()
                    .put(extraSettings)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .build()
            )
            .stats(indexWriteLoad == null ? null : new IndexMetadataStats(indexWriteLoad, 1, 1))
            .creationDate(createdAt)
            .build();
    }

    private DataStream createDataStream(String name, List<Index> backingIndices) {
        return DataStream.builder(name, backingIndices)
            .setGeneration(backingIndices.size())
            .setMetadata(Map.of())
            .setIndexMode(IndexMode.STANDARD)
            .build();
    }
}
