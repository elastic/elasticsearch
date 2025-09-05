/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.writeloadforecaster;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.lucene.util.hnsw.IntToIntFunction;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadataStats;
import org.elasticsearch.cluster.metadata.IndexWriteLoad;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.allocation.WriteLoadForecaster;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.writeloadforecaster.LicensedWriteLoadForecaster.forecastIndexWriteLoad;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

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
        final AtomicInteger licenseCheckCount = new AtomicInteger();
        final WriteLoadForecaster writeLoadForecaster = new LicensedWriteLoadForecaster(() -> {
            licenseCheckCount.incrementAndGet();
            return hasValidLicense.get();
        }, threadPool, maxIndexAge);

        writeLoadForecaster.refreshLicense();

        final String dataStreamName = "logs-es";
        final int numberOfBackingIndices = 10;
        final ProjectMetadata.Builder metadataBuilder = createMetadataBuilderWithDataStream(
            dataStreamName,
            numberOfBackingIndices,
            randomIntBetween(1, 5),
            maxIndexAge
        );
        final DataStream dataStream = metadataBuilder.dataStream(dataStreamName);

        final ProjectMetadata.Builder updatedMetadataBuilder = writeLoadForecaster.withWriteLoadForecastForWriteIndex(
            dataStream.getName(),
            metadataBuilder
        );

        final IndexMetadata writeIndex = updatedMetadataBuilder.getSafe(dataStream.getWriteIndex());

        final OptionalDouble forecastedWriteLoad = writeLoadForecaster.getForecastedWriteLoad(writeIndex);

        assertThat(forecastedWriteLoad.isPresent(), is(true));
        assertThat(forecastedWriteLoad.getAsDouble(), is(greaterThan(0.0)));

        assertThat(licenseCheckCount.get(), equalTo(1));
        hasValidLicense.set(false);

        writeLoadForecaster.refreshLicense();
        assertThat(licenseCheckCount.get(), equalTo(2));

        final OptionalDouble forecastedWriteLoadAfterLicenseChange = writeLoadForecaster.getForecastedWriteLoad(writeIndex);
        assertThat(forecastedWriteLoadAfterLicenseChange.isPresent(), is(false));
    }

    public void testUptimeIsUsedToWeightWriteLoad() {
        final TimeValue maxIndexAge = TimeValue.timeValueDays(7);
        final var metadataBuilder = ProjectMetadata.builder(randomProjectIdOrDefault());
        final String dataStreamName = "logs-es";
        final int numberOfShards = 5;
        final List<Index> backingIndices = new ArrayList<>();
        // Weighted avg 14.4
        final IndexMetadata indexMetadata = createIndexMetadata(
            DataStream.getDefaultBackingIndexName(dataStreamName, 0),
            numberOfShards,
            IndexWriteLoad.builder(numberOfShards)
                .withShardWriteLoad(0, 12, 999, 999, 80)
                .withShardWriteLoad(1, 24, 999, 999, 5)
                .withShardWriteLoad(2, 24, 999, 999, 5)
                .withShardWriteLoad(3, 24, 999, 999, 5)
                .withShardWriteLoad(4, 24, 999, 999, 5)
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
        writeLoadForecaster.refreshLicense();

        final ProjectMetadata.Builder updatedMetadataBuilder = writeLoadForecaster.withWriteLoadForecastForWriteIndex(
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
        writeLoadForecaster.refreshLicense();

        final ProjectMetadata.Builder metadataBuilder = ProjectMetadata.builder(randomProjectIdOrDefault());
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

        final ProjectMetadata.Builder updatedMetadataBuilder = writeLoadForecaster.withWriteLoadForecastForWriteIndex(
            dataStream.getName(),
            metadataBuilder
        );

        final IndexMetadata writeIndex = updatedMetadataBuilder.getSafe(dataStream.getWriteIndex());

        final OptionalDouble forecastedWriteLoad = writeLoadForecaster.getForecastedWriteLoad(writeIndex);

        assertThat(forecastedWriteLoad.isPresent(), is(true));
        assertThat(forecastedWriteLoad.getAsDouble(), is(equalTo(0.6)));

        hasValidLicense.set(false);
        writeLoadForecaster.refreshLicense();

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
                List.of(IndexWriteLoad.builder(1).withShardWriteLoad(0, 12, 999, 999, 100).build())
            );
            assertThat(writeLoadForecast.isPresent(), is(true));
            assertThat(writeLoadForecast.getAsDouble(), is(equalTo(12.0)));
        }

        {
            OptionalDouble writeLoadForecast = forecastIndexWriteLoad(
                List.of(
                    IndexWriteLoad.builder(5)
                        .withShardWriteLoad(0, 12, 999, 999, 80)
                        .withShardWriteLoad(1, 24, 999, 999, 5)
                        .withShardWriteLoad(2, 24, 999, 999, 5)
                        .withShardWriteLoad(3, 24, 999, 999, 5)
                        .withShardWriteLoad(4, 24, 999, 999, 5)
                        .build()
                )
            );
            assertThat(writeLoadForecast.isPresent(), is(true));
            assertThat(writeLoadForecast.getAsDouble(), is(equalTo(72.0)));
        }

        {
            OptionalDouble writeLoadForecast = forecastIndexWriteLoad(
                List.of(
                    IndexWriteLoad.builder(5)
                        .withShardWriteLoad(0, 12, 999, 999, 80)
                        .withShardWriteLoad(1, 24, 999, 999, 5)
                        .withShardWriteLoad(2, 24, 999, 999, 5)
                        .withShardWriteLoad(3, 24, 999, 999, 5)
                        .withShardWriteLoad(4, 24, 999, 999, 5)
                        .build(),
                    // Since this shard uptime is really low, it doesn't add much to the avg
                    IndexWriteLoad.builder(1).withShardWriteLoad(0, 120, 999, 999, 1).build()
                )
            );
            assertThat(writeLoadForecast.isPresent(), is(true));
            assertThat(writeLoadForecast.getAsDouble(), is(closeTo(72.59, 0.01)));
        }

        {
            OptionalDouble writeLoadForecast = forecastIndexWriteLoad(
                List.of(
                    IndexWriteLoad.builder(2).withShardWriteLoad(0, 12, 999, 999, 25).withShardWriteLoad(1, 12, 999, 999, 25).build(),

                    IndexWriteLoad.builder(1).withShardWriteLoad(0, 12, 999, 999, 50).build()
                )
            );
            assertThat(writeLoadForecast.isPresent(), is(true));
            assertThat(writeLoadForecast.getAsDouble(), is(equalTo(16.0)));
        }

        {
            // All indices have the same uptime, therefore it's just a regular avg
            OptionalDouble writeLoadForecast = forecastIndexWriteLoad(
                List.of(
                    IndexWriteLoad.builder(3)
                        .withShardWriteLoad(0, 25, 999, 999, 1)
                        .withShardWriteLoad(1, 18, 999, 999, 1)
                        .withShardWriteLoad(2, 23, 999, 999, 1)
                        .build(),

                    IndexWriteLoad.builder(2).withShardWriteLoad(0, 6, 999, 999, 1).withShardWriteLoad(1, 8, 999, 999, 1).build(),

                    IndexWriteLoad.builder(1).withShardWriteLoad(0, 15, 999, 999, 1).build()
                )
            );
            assertThat(writeLoadForecast.isPresent(), is(true));
            assertThat(writeLoadForecast.getAsDouble(), is(closeTo(31.66, 0.01)));
        }
    }

    private IndexWriteLoad randomIndexWriteLoad(int numberOfShards) {
        IndexWriteLoad.Builder builder = IndexWriteLoad.builder(numberOfShards);
        for (int shardId = 0; shardId < numberOfShards; shardId++) {
            builder.withShardWriteLoad(
                shardId,
                randomDoubleBetween(0, 64, true),
                randomDoubleBetween(0, 64, true),
                randomDoubleBetween(0, 64, true),
                randomLongBetween(1, 10)
            );
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

    public void testLicenseStateLogging() {

        final var seenMessages = new ArrayList<String>();

        final var collectingLoggingAssertion = new MockLog.SeenEventExpectation(
            "seen event",
            LicensedWriteLoadForecaster.class.getCanonicalName(),
            Level.INFO,
            "*"
        ) {
            @Override
            public boolean innerMatch(LogEvent event) {
                final var message = event.getMessage().getFormattedMessage();
                if (message.startsWith("license state changed, now [")) {
                    seenMessages.add(message);
                    return true;
                }

                return false;
            }
        };

        MockLog.assertThatLogger(() -> {
            final var hasValidLicense = new AtomicBoolean();
            final var writeLoadForecaster = new LicensedWriteLoadForecaster(hasValidLicense::get, threadPool, randomTimeValue());
            assertThat(seenMessages, empty());
            writeLoadForecaster.refreshLicense();
            assertThat(seenMessages, empty());

            hasValidLicense.set(true);
            writeLoadForecaster.refreshLicense();
            assertThat(seenMessages, contains("license state changed, now [valid]"));
            writeLoadForecaster.refreshLicense();
            assertThat(seenMessages, contains("license state changed, now [valid]"));

            hasValidLicense.set(false);
            writeLoadForecaster.refreshLicense();
            assertThat(seenMessages, contains("license state changed, now [valid]", "license state changed, now [not valid]"));

            hasValidLicense.set(true);
            ESTestCase.startInParallel(between(1, 10), ignored -> writeLoadForecaster.refreshLicense());
            assertThat(
                seenMessages,
                contains(
                    "license state changed, now [valid]",
                    "license state changed, now [not valid]",
                    "license state changed, now [valid]"
                )
            );
        }, LicensedWriteLoadForecaster.class, collectingLoggingAssertion);
    }

    public void testShardIncreaseDoesNotIncreaseTotalLoad() {
        testShardChangeDoesNotChangeTotalForecastLoad(ShardCountChange.INCREASE);
    }

    public void testShardDecreaseDoesNotDecreaseTotalLoad() {
        testShardChangeDoesNotChangeTotalForecastLoad(ShardCountChange.DECREASE);
    }

    private void testShardChangeDoesNotChangeTotalForecastLoad(ShardCountChange shardCountChange) {
        final TimeValue maxIndexAge = TimeValue.timeValueDays(7);
        final AtomicBoolean hasValidLicense = new AtomicBoolean(true);
        final AtomicInteger licenseCheckCount = new AtomicInteger();
        final WriteLoadForecaster writeLoadForecaster = new LicensedWriteLoadForecaster(() -> {
            licenseCheckCount.incrementAndGet();
            return hasValidLicense.get();
        }, threadPool, maxIndexAge);
        writeLoadForecaster.refreshLicense();

        final String dataStreamName = randomIdentifier();
        final ProjectMetadata.Builder originalMetadata = writeLoadForecaster.withWriteLoadForecastForWriteIndex(
            dataStreamName,
            createMetadataBuilderWithDataStream(dataStreamName, randomIntBetween(5, 15), shardCountChange.originalShardCount(), maxIndexAge)
        );

        // Generate the same data stream, but with a different number of shards in the write index
        final ProjectMetadata.Builder changedShardCountMetadata = writeLoadForecaster.withWriteLoadForecastForWriteIndex(
            dataStreamName,
            updateWriteIndexShardCount(dataStreamName, originalMetadata, shardCountChange)
        );

        IndexMetadata originalWriteIndexMetadata = originalMetadata.getSafe(originalMetadata.dataStream(dataStreamName).getWriteIndex());
        IndexMetadata changedShardCountWriteIndexMetadata = changedShardCountMetadata.getSafe(
            changedShardCountMetadata.dataStream(dataStreamName).getWriteIndex()
        );

        // The shard count changed
        assertThat(
            changedShardCountWriteIndexMetadata.getNumberOfShards(),
            shardCountChange.expectedChangeFromOriginal(originalWriteIndexMetadata.getNumberOfShards())
        );
        // But the total write-load did not
        assertThat(
            changedShardCountWriteIndexMetadata.getNumberOfShards() * writeLoadForecaster.getForecastedWriteLoad(
                changedShardCountWriteIndexMetadata
            ).getAsDouble(),
            closeTo(
                originalWriteIndexMetadata.getNumberOfShards() * writeLoadForecaster.getForecastedWriteLoad(originalWriteIndexMetadata)
                    .getAsDouble(),
                0.01
            )
        );
    }

    public enum ShardCountChange implements IntToIntFunction {
        INCREASE(1, 15) {
            @Override
            public int apply(int originalShardCount) {
                return randomIntBetween(originalShardCount + 1, originalShardCount * 3);
            }

            public Matcher<Integer> expectedChangeFromOriginal(int originalShardCount) {
                return greaterThan(originalShardCount);
            }
        },
        DECREASE(10, 30) {
            @Override
            public int apply(int originalShardCount) {
                return randomIntBetween(1, originalShardCount - 1);
            }

            public Matcher<Integer> expectedChangeFromOriginal(int originalShardCount) {
                return lessThan(originalShardCount);
            }
        };

        private final int originalMinimumShardCount;
        private final int originalMaximumShardCount;

        ShardCountChange(int originalMinimumShardCount, int originalMaximumShardCount) {
            this.originalMinimumShardCount = originalMinimumShardCount;
            this.originalMaximumShardCount = originalMaximumShardCount;
        }

        public int originalShardCount() {
            return randomIntBetween(originalMinimumShardCount, originalMaximumShardCount);
        }

        abstract Matcher<Integer> expectedChangeFromOriginal(int originalShardCount);
    }

    private ProjectMetadata.Builder updateWriteIndexShardCount(
        String dataStreamName,
        ProjectMetadata.Builder originalMetadata,
        ShardCountChange shardCountChange
    ) {
        final ProjectMetadata.Builder updatedShardCountMetadata = ProjectMetadata.builder(originalMetadata.getId());

        final DataStream originalDataStream = originalMetadata.dataStream(dataStreamName);
        final Index existingWriteIndex = Objects.requireNonNull(originalDataStream.getWriteIndex());
        final IndexMetadata originalWriteIndexMetadata = originalMetadata.getSafe(existingWriteIndex);

        // Copy all non-write indices over unchanged
        final List<IndexMetadata> backingIndexMetadatas = originalDataStream.getIndices()
            .stream()
            .filter(index -> index != existingWriteIndex)
            .map(originalMetadata::getSafe)
            .collect(Collectors.toList());

        // Create a new write index with an updated shard count
        final IndexMetadata writeIndexMetadata = createIndexMetadata(
            DataStream.getDefaultBackingIndexName(dataStreamName, backingIndexMetadatas.size()),
            shardCountChange.apply(originalWriteIndexMetadata.getNumberOfShards()),
            null,
            System.currentTimeMillis()
        );
        backingIndexMetadatas.add(writeIndexMetadata);
        backingIndexMetadatas.forEach(indexMetadata -> updatedShardCountMetadata.put(indexMetadata, false));

        final DataStream dataStream = createDataStream(
            dataStreamName,
            backingIndexMetadatas.stream().map(IndexMetadata::getIndex).toList()
        );
        updatedShardCountMetadata.put(dataStream);
        return updatedShardCountMetadata;
    }

    private ProjectMetadata.Builder createMetadataBuilderWithDataStream(
        String dataStreamName,
        int numberOfBackingIndices,
        int numberOfShards,
        TimeValue maxIndexAge
    ) {
        final ProjectMetadata.Builder metadataBuilder = ProjectMetadata.builder(randomProjectIdOrDefault());
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
        return metadataBuilder;
    }
}
