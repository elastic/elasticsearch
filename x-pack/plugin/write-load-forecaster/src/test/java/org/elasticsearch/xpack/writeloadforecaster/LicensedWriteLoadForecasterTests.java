/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.writeloadforecaster;

import org.elasticsearch.Version;
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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

    public void testGetIndicesWithinMaxAgeRange() {
        final TimeValue maxIndexAge = TimeValue.timeValueDays(7);
        final LicensedWriteLoadForecaster writeLoadForecaster = new LicensedWriteLoadForecaster(() -> true, threadPool, maxIndexAge);

        final Metadata.Builder metadataBuilder = Metadata.builder();
        final int numberOfBackingIndicesOlderThanMinAge = randomIntBetween(0, 10);
        final int numberOfBackingIndicesWithinMinAnge = randomIntBetween(0, 10);
        final int numberOfShards = 1;
        final List<Index> backingIndices = new ArrayList<>();
        final String dataStreamName = "logs-es";
        final List<Index> backingIndicesOlderThanMinAge = new ArrayList<>();
        for (int i = 0; i < numberOfBackingIndicesOlderThanMinAge; i++) {
            long creationDate = System.currentTimeMillis() - maxIndexAge.millis() * 2;
            final IndexMetadata indexMetadata = createIndexMetadata(
                DataStream.getDefaultBackingIndexName(dataStreamName, backingIndices.size(), creationDate),
                numberOfShards,
                randomIndexWriteLoad(numberOfShards),
                creationDate
            );
            backingIndices.add(indexMetadata.getIndex());
            backingIndicesOlderThanMinAge.add(indexMetadata.getIndex());
            metadataBuilder.put(indexMetadata, false);
        }

        final List<Index> backingIndicesWithinMinAge = new ArrayList<>();
        for (int i = 0; i < numberOfBackingIndicesWithinMinAnge; i++) {
            final long createdAt = System.currentTimeMillis() - (maxIndexAge.getMillis() / 2);
            final IndexMetadata indexMetadata = createIndexMetadata(
                DataStream.getDefaultBackingIndexName(dataStreamName, backingIndices.size(), createdAt),
                numberOfShards,
                randomIndexWriteLoad(numberOfShards),
                createdAt
            );
            backingIndices.add(indexMetadata.getIndex());
            backingIndicesWithinMinAge.add(indexMetadata.getIndex());
            metadataBuilder.put(indexMetadata, false);
        }

        final String writeIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, backingIndices.size());
        final IndexMetadata writeIndexMetadata = createIndexMetadata(writeIndexName, numberOfShards, null, System.currentTimeMillis());
        backingIndices.add(writeIndexMetadata.getIndex());
        metadataBuilder.put(writeIndexMetadata, false);

        final DataStream dataStream = createDataStream(dataStreamName, backingIndices);

        metadataBuilder.put(dataStream);

        final List<Index> indicesWithinMaxAgeRange = writeLoadForecaster.getIndicesWithinMaxAgeRange(dataStream, metadataBuilder);

        final List<Index> expectedIndicesWithinMaxAgeRange = new ArrayList<>();
        if (numberOfBackingIndicesOlderThanMinAge > 0) {
            expectedIndicesWithinMaxAgeRange.add(backingIndicesOlderThanMinAge.get(backingIndicesOlderThanMinAge.size() - 1));
        }
        expectedIndicesWithinMaxAgeRange.addAll(backingIndicesWithinMinAge);
        expectedIndicesWithinMaxAgeRange.add(writeIndexMetadata.getIndex());

        assertThat(indicesWithinMaxAgeRange, is(equalTo(expectedIndicesWithinMaxAgeRange)));
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
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .build()
            )
            .stats(indexWriteLoad == null ? null : new IndexMetadataStats(indexWriteLoad, 1, 1))
            .creationDate(createdAt)
            .build();
    }

    private DataStream createDataStream(String name, List<Index> backingIndices) {
        return new DataStream(
            name,
            backingIndices,
            backingIndices.size(),
            Collections.emptyMap(),
            false,
            false,
            false,
            false,
            IndexMode.STANDARD
        );
    }
}
