/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.datastreams.autosharding;

import org.elasticsearch.action.admin.indices.rollover.MaxAgeCondition;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamAutoShardingEvent;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadataStats;
import org.elasticsearch.cluster.metadata.IndexWriteLoad;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.action.datastreams.autosharding.AutoShardingResult.NOT_APPLICABLE_RESULT;
import static org.elasticsearch.action.datastreams.autosharding.AutoShardingType.COOLDOWN_PREVENTED_DECREASE;
import static org.elasticsearch.action.datastreams.autosharding.AutoShardingType.COOLDOWN_PREVENTED_INCREASE;
import static org.elasticsearch.action.datastreams.autosharding.AutoShardingType.DECREASE_SHARDS;
import static org.elasticsearch.action.datastreams.autosharding.AutoShardingType.INCREASE_SHARDS;
import static org.elasticsearch.action.datastreams.autosharding.AutoShardingType.NO_CHANGE_REQUIRED;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.hamcrest.Matchers.is;

public class DataStreamAutoShardingServiceTests extends ESTestCase {

    private ClusterService clusterService;
    private ThreadPool threadPool;
    private DataStreamAutoShardingService service;
    private long now;
    String dataStreamName;

    @Before
    public void setupService() {
        threadPool = new TestThreadPool(getTestName());
        Set<Setting<?>> builtInClusterSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        builtInClusterSettings.add(
            Setting.boolSetting(
                DataStreamAutoShardingService.DATA_STREAMS_AUTO_SHARDING_ENABLED,
                false,
                Setting.Property.Dynamic,
                Setting.Property.NodeScope
            )
        );
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, builtInClusterSettings);
        clusterService = createClusterService(threadPool, clusterSettings);
        now = System.currentTimeMillis();
        service = new DataStreamAutoShardingService(
            Settings.builder().put(DataStreamAutoShardingService.DATA_STREAMS_AUTO_SHARDING_ENABLED, true).build(),
            clusterService,
            () -> now
        );
        dataStreamName = randomAlphaOfLengthBetween(10, 100);
        logger.info("-> data stream name is [{}]", dataStreamName);
    }

    @After
    public void cleanup() {
        clusterService.close();
        threadPool.shutdownNow();
    }

    public void testCalculateValidations() {
        var projectId = randomProjectIdOrDefault();
        ProjectMetadata.Builder builder = ProjectMetadata.builder(projectId);
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            1,
            now,
            List.of(now - 3000, now - 2000, now - 1000),
            getWriteLoad(1, 2.0),
            null
        );
        builder.put(dataStream);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create("n1")).add(DiscoveryNodeUtils.create("n2")))
            .putProjectMetadata(builder.build())
            .build();

        {
            // autosharding disabled
            DataStreamAutoShardingService disabledAutoshardingService = new DataStreamAutoShardingService(
                Settings.EMPTY,
                clusterService,
                System::currentTimeMillis
            );

            AutoShardingResult autoShardingResult = disabledAutoshardingService.calculate(state.projectState(projectId), dataStream, 2.0);
            assertThat(autoShardingResult, is(NOT_APPLICABLE_RESULT));
        }

        {
            // null write load passed
            AutoShardingResult autoShardingResult = service.calculate(state.projectState(projectId), dataStream, null);
            assertThat(autoShardingResult, is(NOT_APPLICABLE_RESULT));
        }
    }

    public void testCalculateIncreaseShardingRecommendations() {
        // the input is a data stream with 5 backing indices with 1 shard each
        // all 4 backing indices have a write load of 2.0
        // we'll recreate it across the test and add an auto sharding event as we iterate
        {
            var projectId = randomProjectIdOrDefault();
            ProjectMetadata.Builder builder = ProjectMetadata.builder(projectId);
            Function<DataStreamAutoShardingEvent, DataStream> dataStreamSupplier = (autoShardingEvent) -> createDataStream(
                builder,
                dataStreamName,
                1,
                now,
                List.of(now - 10_000, now - 7000, now - 5000, now - 2000, now - 1000),
                getWriteLoad(1, 2.0),
                autoShardingEvent
            );

            DataStream dataStream = dataStreamSupplier.apply(null);
            builder.put(dataStream);
            ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
                .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create("n1")).add(DiscoveryNodeUtils.create("n2")))
                .putProjectMetadata(builder.build())
                .build();

            AutoShardingResult autoShardingResult = service.calculate(state.projectState(projectId), dataStream, 2.5);
            assertThat(autoShardingResult.type(), is(INCREASE_SHARDS));
            // no pre-existing scaling event so the cool down must be zero
            assertThat(autoShardingResult.coolDownRemaining(), is(TimeValue.ZERO));
            assertThat(autoShardingResult.targetNumberOfShards(), is(3));
        }

        {
            // let's add a pre-existing sharding event so that we'll return some cool down period that's preventing an INCREASE_SHARDS
            // event so the result type we're expecting is COOLDOWN_PREVENTED_INCREASE
            var projectId = randomProjectIdOrDefault();
            ProjectMetadata.Builder builder = ProjectMetadata.builder(projectId);
            Function<DataStreamAutoShardingEvent, DataStream> dataStreamSupplier = (autoShardingEvent) -> createDataStream(
                builder,
                dataStreamName,
                1,
                now,
                List.of(now - 10_000, now - 7000, now - 5000, now - 2000, now - 1000),
                getWriteLoad(1, 2.0),
                autoShardingEvent
            );

            // generation 4 triggered an auto sharding event to 2 shards
            DataStream dataStream = dataStreamSupplier.apply(
                new DataStreamAutoShardingEvent(DataStream.getDefaultBackingIndexName(dataStreamName, 4), 2, now - 1005)
            );
            builder.put(dataStream);
            ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
                .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create("n1")).add(DiscoveryNodeUtils.create("n2")))
                .putProjectMetadata(builder.build())
                .build();

            AutoShardingResult autoShardingResult = service.calculate(state.projectState(projectId), dataStream, 2.5);
            assertThat(autoShardingResult.type(), is(COOLDOWN_PREVENTED_INCREASE));
            // no pre-existing scaling event so the cool down must be zero
            assertThat(autoShardingResult.targetNumberOfShards(), is(3));
            // it's been 1005 millis since the last auto sharding event and the cool down is 270secoinds (270_000 millis)
            assertThat(autoShardingResult.coolDownRemaining(), is(TimeValue.timeValueMillis(268995)));
        }

        {
            // let's test a subsequent increase in the number of shards after a previos auto sharding event
            var projectId = randomProjectIdOrDefault();
            ProjectMetadata.Builder builder = ProjectMetadata.builder(projectId);
            Function<DataStreamAutoShardingEvent, DataStream> dataStreamSupplier = (autoShardingEvent) -> createDataStream(
                builder,
                dataStreamName,
                1,
                now,
                List.of(now - 10_000_000, now - 7_000_000, now - 2_000_000, now - 1_000_000, now - 1000),
                getWriteLoad(1, 2.0),
                autoShardingEvent
            );

            // generation 3 triggered an increase in shards event to 2 shards
            DataStream dataStream = dataStreamSupplier.apply(
                new DataStreamAutoShardingEvent(DataStream.getDefaultBackingIndexName(dataStreamName, 4), 2, now - 2_000_100)
            );
            builder.put(dataStream);
            ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
                .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create("n1")).add(DiscoveryNodeUtils.create("n2")))
                .putProjectMetadata(builder.build())
                .build();

            AutoShardingResult autoShardingResult = service.calculate(state.projectState(projectId), dataStream, 2.5);
            assertThat(autoShardingResult.type(), is(INCREASE_SHARDS));
            // no pre-existing scaling event so the cool down must be zero
            assertThat(autoShardingResult.targetNumberOfShards(), is(3));
            assertThat(autoShardingResult.coolDownRemaining(), is(TimeValue.ZERO));
        }
    }

    public void testCalculateDecreaseShardingRecommendations() {
        // the input is a data stream with 5 backing indices with 3 shards each
        {
            // testing a decrease shards events prevented by the cool down period not lapsing due to the oldest generation index being
            // "too new" (i.e. the cool down period hasn't lapsed since the oldest generation index)
            var projectId = randomProjectIdOrDefault();
            ProjectMetadata.Builder builder = ProjectMetadata.builder(projectId);
            Function<DataStreamAutoShardingEvent, DataStream> dataStreamSupplier = (autoShardingEvent) -> createDataStream(
                builder,
                dataStreamName,
                3,
                now,
                List.of(now - 10_000, now - 7000, now - 5000, now - 2000, now - 1000),
                getWriteLoad(3, 0.25),
                autoShardingEvent
            );

            DataStream dataStream = dataStreamSupplier.apply(null);
            builder.put(dataStream);
            ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
                .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create("n1")).add(DiscoveryNodeUtils.create("n2")))
                .putProjectMetadata(builder.build())
                .build();

            AutoShardingResult autoShardingResult = service.calculate(state.projectState(projectId), dataStream, 1.0);
            // the cooldown period for the decrease shards event hasn't lapsed since the data stream was created
            assertThat(autoShardingResult.type(), is(COOLDOWN_PREVENTED_DECREASE));
            assertThat(autoShardingResult.coolDownRemaining(), is(TimeValue.timeValueMillis(TimeValue.timeValueDays(3).millis() - 10_000)));
            // based on the write load of 0.75 we should be reducing the number of shards to 1
            assertThat(autoShardingResult.targetNumberOfShards(), is(1));
        }

        {
            var projectId = randomProjectIdOrDefault();
            ProjectMetadata.Builder builder = ProjectMetadata.builder(projectId);
            Function<DataStreamAutoShardingEvent, DataStream> dataStreamSupplier = (autoShardingEvent) -> createDataStream(
                builder,
                dataStreamName,
                3,
                now,
                List.of(
                    now - TimeValue.timeValueDays(21).getMillis(),
                    now - TimeValue.timeValueDays(15).getMillis(),
                    now - TimeValue.timeValueDays(4).getMillis(),
                    now - TimeValue.timeValueDays(2).getMillis(),
                    now - 1000
                ),
                getWriteLoad(3, 0.333),
                autoShardingEvent
            );

            DataStream dataStream = dataStreamSupplier.apply(null);
            builder.put(dataStream);
            ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
                .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create("n1")).add(DiscoveryNodeUtils.create("n2")))
                .putProjectMetadata(builder.build())
                .build();

            AutoShardingResult autoShardingResult = service.calculate(state.projectState(projectId), dataStream, 1.0);
            assertThat(autoShardingResult.type(), is(DECREASE_SHARDS));
            assertThat(autoShardingResult.targetNumberOfShards(), is(1));
            // no pre-existing auto sharding event however we have old enough backing indices (older than the cooldown period) so we can
            // make a decision to reduce the number of shards
            assertThat(autoShardingResult.coolDownRemaining(), is(TimeValue.ZERO));
        }

        {
            // let's test a decrease in number of shards after a previous decrease event
            var projectId = randomProjectIdOrDefault();
            ProjectMetadata.Builder builder = ProjectMetadata.builder(projectId);
            Function<DataStreamAutoShardingEvent, DataStream> dataStreamSupplier = (autoShardingEvent) -> createDataStream(
                builder,
                dataStreamName,
                3,
                now,
                List.of(
                    now - TimeValue.timeValueDays(21).getMillis(),
                    now - TimeValue.timeValueDays(15).getMillis(), // triggers auto sharding event
                    now - TimeValue.timeValueDays(4).getMillis(),
                    now - TimeValue.timeValueDays(2).getMillis(),
                    now - 1000
                ),
                getWriteLoad(3, 0.333),
                autoShardingEvent
            );

            // generation 2 triggered a decrease in shards event to 2 shards
            DataStream dataStream = dataStreamSupplier.apply(
                new DataStreamAutoShardingEvent(
                    DataStream.getDefaultBackingIndexName(dataStreamName, 2),
                    2,
                    now - TimeValue.timeValueDays(4).getMillis()
                )
            );
            builder.put(dataStream);
            ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
                .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create("n1")).add(DiscoveryNodeUtils.create("n2")))
                .putProjectMetadata(builder.build())
                .build();

            AutoShardingResult autoShardingResult = service.calculate(state.projectState(projectId), dataStream, 1.0);
            assertThat(autoShardingResult.type(), is(DECREASE_SHARDS));
            assertThat(autoShardingResult.targetNumberOfShards(), is(1));
            assertThat(autoShardingResult.coolDownRemaining(), is(TimeValue.ZERO));
        }

        {
            // let's test a decrease in number of shards that's prevented by the cool down period due to a previous sharding event
            // the expected result type here is COOLDOWN_PREVENTED_DECREASE
            var projectId = randomProjectIdOrDefault();
            ProjectMetadata.Builder builder = ProjectMetadata.builder(projectId);
            Function<DataStreamAutoShardingEvent, DataStream> dataStreamSupplier = (autoShardingEvent) -> createDataStream(
                builder,
                dataStreamName,
                3,
                now,
                List.of(
                    now - TimeValue.timeValueDays(21).getMillis(),
                    now - TimeValue.timeValueDays(2).getMillis(), // triggers auto sharding event
                    now - TimeValue.timeValueDays(1).getMillis(),
                    now - 1000
                ),
                getWriteLoad(3, 0.25),
                autoShardingEvent
            );

            // generation 2 triggered a decrease in shards event to 2 shards
            DataStream dataStream = dataStreamSupplier.apply(
                new DataStreamAutoShardingEvent(
                    DataStream.getDefaultBackingIndexName(dataStreamName, 2),
                    2,
                    now - TimeValue.timeValueDays(2).getMillis()
                )
            );
            builder.put(dataStream);
            ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
                .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create("n1")).add(DiscoveryNodeUtils.create("n2")))
                .putProjectMetadata(builder.build())
                .build();

            AutoShardingResult autoShardingResult = service.calculate(state.projectState(projectId), dataStream, 1.0);
            assertThat(autoShardingResult.type(), is(COOLDOWN_PREVENTED_DECREASE));
            assertThat(autoShardingResult.targetNumberOfShards(), is(1));
            assertThat(autoShardingResult.coolDownRemaining(), is(TimeValue.timeValueDays(1)));
        }

        {
            // no change required
            var projectId = randomProjectIdOrDefault();
            ProjectMetadata.Builder builder = ProjectMetadata.builder(projectId);
            Function<DataStreamAutoShardingEvent, DataStream> dataStreamSupplier = (autoShardingEvent) -> createDataStream(
                builder,
                dataStreamName,
                3,
                now,
                List.of(
                    now - TimeValue.timeValueDays(21).getMillis(),
                    now - TimeValue.timeValueDays(15).getMillis(),
                    now - TimeValue.timeValueDays(4).getMillis(),
                    now - TimeValue.timeValueDays(2).getMillis(),
                    now - 1000
                ),
                getWriteLoad(3, 1.333),
                autoShardingEvent
            );

            // generation 2 triggered a decrease in shards event to 2 shards
            DataStream dataStream = dataStreamSupplier.apply(null);
            builder.put(dataStream);
            ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
                .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create("n1")).add(DiscoveryNodeUtils.create("n2")))
                .putProjectMetadata(builder.build())
                .build();

            AutoShardingResult autoShardingResult = service.calculate(state.projectState(projectId), dataStream, 4.0);
            assertThat(autoShardingResult.type(), is(NO_CHANGE_REQUIRED));
            assertThat(autoShardingResult.targetNumberOfShards(), is(3));
            assertThat(autoShardingResult.coolDownRemaining(), is(TimeValue.ZERO));
        }
    }

    public void testComputeOptimalNumberOfShards() {
        int minWriteThreads = 2;
        int maxWriteThreads = 32;

        {
            // 0.0 indexing load recommends 1 shard
            logger.info("-> indexingLoad {}", 0.0);
            assertThat(DataStreamAutoShardingService.computeOptimalNumberOfShards(minWriteThreads, maxWriteThreads, 0.0), is(1L));
        }
        {
            // the small values will be very common so let's randomise to make sure we never go below 1L
            double indexingLoad = randomDoubleBetween(0.0001, 1.0, true);
            logger.info("-> indexingLoad {}", indexingLoad);
            assertThat(DataStreamAutoShardingService.computeOptimalNumberOfShards(minWriteThreads, maxWriteThreads, indexingLoad), is(1L));
        }

        {
            double indexingLoad = 2.0;
            logger.info("-> indexingLoad {}", indexingLoad);
            assertThat(DataStreamAutoShardingService.computeOptimalNumberOfShards(minWriteThreads, maxWriteThreads, indexingLoad), is(2L));
        }

        {
            // there's a broad range of popular values (a write index starting to be very busy, using between 3 and all of the 32 write
            // threads, so let's randomise this too to make sure we stay at 3 recommended shards)
            double indexingLoad = randomDoubleBetween(3.0002, 32.0, true);
            logger.info("-> indexingLoad {}", indexingLoad);

            assertThat(DataStreamAutoShardingService.computeOptimalNumberOfShards(minWriteThreads, maxWriteThreads, indexingLoad), is(3L));
        }

        {
            double indexingLoad = 49.0;
            logger.info("-> indexingLoad {}", indexingLoad);
            assertThat(DataStreamAutoShardingService.computeOptimalNumberOfShards(minWriteThreads, maxWriteThreads, indexingLoad), is(4L));
        }

        {
            double indexingLoad = 70.0;
            logger.info("-> indexingLoad {}", indexingLoad);
            assertThat(DataStreamAutoShardingService.computeOptimalNumberOfShards(minWriteThreads, maxWriteThreads, indexingLoad), is(5L));
        }

        {
            double indexingLoad = 100.0;
            logger.info("-> indexingLoad {}", indexingLoad);
            assertThat(DataStreamAutoShardingService.computeOptimalNumberOfShards(minWriteThreads, maxWriteThreads, indexingLoad), is(7L));
        }

        {
            double indexingLoad = 180.0;
            logger.info("-> indexingLoad {}", indexingLoad);
            assertThat(DataStreamAutoShardingService.computeOptimalNumberOfShards(minWriteThreads, maxWriteThreads, indexingLoad), is(12L));
        }
    }

    public void testGetMaxIndexLoadWithinCoolingPeriod() {
        final TimeValue coolingPeriod = TimeValue.timeValueDays(3);

        final Metadata.Builder metadataBuilder = Metadata.builder();
        final int numberOfBackingIndicesOutsideCoolingPeriod = randomIntBetween(3, 10);
        final int numberOfBackingIndicesWithinCoolingPeriod = randomIntBetween(3, 10);
        final List<Index> backingIndices = new ArrayList<>();
        final String dataStreamName = "logs";
        long now = System.currentTimeMillis();

        // to cover the entire cooling period we'll also include the backing index right before the index age calculation
        // this flag makes that index have a very low or very high write load
        boolean lastIndexBeforeCoolingPeriodHasLowWriteLoad = randomBoolean();
        for (int i = 0; i < numberOfBackingIndicesOutsideCoolingPeriod; i++) {
            long creationDate = now - (coolingPeriod.millis() * 2);
            IndexMetadata indexMetadata = createIndexMetadata(
                DataStream.getDefaultBackingIndexName(dataStreamName, backingIndices.size(), creationDate),
                1,
                getWriteLoad(1, 999.0),
                creationDate
            );

            if (lastIndexBeforeCoolingPeriodHasLowWriteLoad) {
                indexMetadata = createIndexMetadata(
                    DataStream.getDefaultBackingIndexName(dataStreamName, backingIndices.size(), creationDate),
                    1,
                    getWriteLoad(1, 1.0),
                    creationDate
                );
            }
            backingIndices.add(indexMetadata.getIndex());
            metadataBuilder.put(indexMetadata, false);
        }

        for (int i = 0; i < numberOfBackingIndicesWithinCoolingPeriod; i++) {
            final long createdAt = now - (coolingPeriod.getMillis() / 2);
            IndexMetadata indexMetadata;
            if (i == numberOfBackingIndicesWithinCoolingPeriod - 1) {
                indexMetadata = createIndexMetadata(
                    DataStream.getDefaultBackingIndexName(dataStreamName, backingIndices.size(), createdAt),
                    3,
                    getWriteLoad(3, 5.0), // max write index within cooling period
                    createdAt
                );
            } else {
                indexMetadata = createIndexMetadata(
                    DataStream.getDefaultBackingIndexName(dataStreamName, backingIndices.size(), createdAt),
                    3,
                    getWriteLoad(3, 3.0), // each backing index has a write load of 3.0
                    createdAt
                );
            }
            backingIndices.add(indexMetadata.getIndex());
            metadataBuilder.put(indexMetadata, false);
        }

        final String writeIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, backingIndices.size());
        final IndexMetadata writeIndexMetadata = createIndexMetadata(writeIndexName, 3, getWriteLoad(3, 1.0), System.currentTimeMillis());
        backingIndices.add(writeIndexMetadata.getIndex());
        metadataBuilder.put(writeIndexMetadata, false);

        final DataStream dataStream = DataStream.builder(dataStreamName, backingIndices)
            .setGeneration(backingIndices.size())
            .setMetadata(Map.of())
            .setIndexMode(IndexMode.STANDARD)
            .build();

        metadataBuilder.put(dataStream);

        double maxIndexLoadWithinCoolingPeriod = DataStreamAutoShardingService.getMaxIndexLoadWithinCoolingPeriod(
            metadataBuilder.build().getProject(),
            dataStream,
            3.0,
            coolingPeriod,
            () -> now
        );
        // to cover the entire cooldown period, the last index before the cooling period is taken into account
        assertThat(maxIndexLoadWithinCoolingPeriod, is(lastIndexBeforeCoolingPeriodHasLowWriteLoad ? 15.0 : 999.0));
    }

    public void testIndexLoadWithinCoolingPeriodIsSumOfShardsLoads() {
        final TimeValue coolingPeriod = TimeValue.timeValueDays(3);

        final Metadata.Builder metadataBuilder = Metadata.builder();
        final int numberOfBackingIndicesWithinCoolingPeriod = randomIntBetween(3, 10);
        final List<Index> backingIndices = new ArrayList<>();
        final String dataStreamName = "logs";
        long now = System.currentTimeMillis();

        double expectedIsSumOfShardLoads = 0.5 + 3.0 + 0.3333;

        for (int i = 0; i < numberOfBackingIndicesWithinCoolingPeriod; i++) {
            final long createdAt = now - (coolingPeriod.getMillis() / 2);
            IndexMetadata indexMetadata;
            IndexWriteLoad.Builder builder = IndexWriteLoad.builder(3);
            for (int shardId = 0; shardId < 3; shardId++) {
                switch (shardId) {
                    case 0 -> builder.withShardWriteLoad(shardId, 0.5, 40);
                    case 1 -> builder.withShardWriteLoad(shardId, 3.0, 10);
                    case 2 -> builder.withShardWriteLoad(shardId, 0.3333, 150);
                }
            }
            indexMetadata = createIndexMetadata(
                DataStream.getDefaultBackingIndexName(dataStreamName, backingIndices.size(), createdAt),
                3,
                builder.build(), // max write index within cooling period should be 0.5 (ish)
                createdAt
            );
            backingIndices.add(indexMetadata.getIndex());
            metadataBuilder.put(indexMetadata, false);
        }

        final String writeIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, backingIndices.size());
        final IndexMetadata writeIndexMetadata = createIndexMetadata(writeIndexName, 3, getWriteLoad(3, 0.1), System.currentTimeMillis());
        backingIndices.add(writeIndexMetadata.getIndex());
        metadataBuilder.put(writeIndexMetadata, false);

        final DataStream dataStream = DataStream.builder(dataStreamName, backingIndices)
            .setGeneration(backingIndices.size())
            .setMetadata(Map.of())
            .setIndexMode(IndexMode.STANDARD)
            .build();

        metadataBuilder.put(dataStream);

        double maxIndexLoadWithinCoolingPeriod = DataStreamAutoShardingService.getMaxIndexLoadWithinCoolingPeriod(
            metadataBuilder.build().getProject(),
            dataStream,
            0.1,
            coolingPeriod,
            () -> now
        );
        assertThat(maxIndexLoadWithinCoolingPeriod, is(expectedIsSumOfShardLoads));
    }

    public void testAutoShardingResultValidation() {
        {
            // throws exception when constructed using types that shouldn't report cooldowns
            expectThrows(
                IllegalArgumentException.class,
                () -> new AutoShardingResult(INCREASE_SHARDS, 1, 3, TimeValue.timeValueSeconds(3), 3.0)
            );

            expectThrows(
                IllegalArgumentException.class,
                () -> new AutoShardingResult(DECREASE_SHARDS, 3, 1, TimeValue.timeValueSeconds(3), 1.0)
            );

        }

        {
            // we can successfully create results with cooldown period for the designated types
            AutoShardingResult cooldownPreventedIncrease = new AutoShardingResult(
                COOLDOWN_PREVENTED_INCREASE,
                1,
                3,
                TimeValue.timeValueSeconds(3),
                3.0
            );
            assertThat(cooldownPreventedIncrease.coolDownRemaining(), is(TimeValue.timeValueSeconds(3)));

            AutoShardingResult cooldownPreventedDecrease = new AutoShardingResult(
                COOLDOWN_PREVENTED_DECREASE,
                3,
                1,
                TimeValue.timeValueSeconds(7),
                1.0
            );
            assertThat(cooldownPreventedDecrease.coolDownRemaining(), is(TimeValue.timeValueSeconds(7)));
        }
    }

    private DataStream createDataStream(
        ProjectMetadata.Builder builder,
        String dataStreamName,
        int numberOfShards,
        Long now,
        List<Long> indicesCreationDate,
        IndexWriteLoad backingIndicesWriteLoad,
        @Nullable DataStreamAutoShardingEvent autoShardingEvent
    ) {
        final List<Index> backingIndices = new ArrayList<>();
        int backingIndicesCount = indicesCreationDate.size();
        for (int k = 0; k < indicesCreationDate.size(); k++) {
            long createdAt = indicesCreationDate.get(k);
            IndexMetadata.Builder indexMetaBuilder;
            if (k < backingIndicesCount - 1) {
                indexMetaBuilder = IndexMetadata.builder(
                    createIndexMetadata(
                        DataStream.getDefaultBackingIndexName(dataStreamName, k + 1),
                        numberOfShards,
                        backingIndicesWriteLoad,
                        createdAt
                    )
                );
                // add rollover info only for non-write indices
                MaxAgeCondition rolloverCondition = new MaxAgeCondition(TimeValue.timeValueMillis(now - 2000L));
                indexMetaBuilder.putRolloverInfo(new RolloverInfo(dataStreamName, List.of(rolloverCondition), now - 2000L));
            } else {
                // write index
                indexMetaBuilder = IndexMetadata.builder(
                    createIndexMetadata(DataStream.getDefaultBackingIndexName(dataStreamName, k + 1), numberOfShards, null, createdAt)
                );
            }
            IndexMetadata indexMetadata = indexMetaBuilder.build();
            builder.put(indexMetadata, false);
            backingIndices.add(indexMetadata.getIndex());
        }
        return DataStream.builder(
            dataStreamName,
            DataStream.DataStreamIndices.backingIndicesBuilder(backingIndices).setAutoShardingEvent(autoShardingEvent).build()
        ).setGeneration(backingIndicesCount).build();
    }

    private IndexMetadata createIndexMetadata(
        String indexName,
        int numberOfShards,
        @Nullable IndexWriteLoad indexWriteLoad,
        long createdAt
    ) {
        return IndexMetadata.builder(indexName)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .build()
            )
            .stats(indexWriteLoad == null ? null : new IndexMetadataStats(indexWriteLoad, 1, 1))
            .creationDate(createdAt)
            .build();
    }

    private IndexWriteLoad getWriteLoad(int numberOfShards, double shardWriteLoad) {
        IndexWriteLoad.Builder builder = IndexWriteLoad.builder(numberOfShards);
        for (int shardId = 0; shardId < numberOfShards; shardId++) {
            builder.withShardWriteLoad(shardId, shardWriteLoad, 1);
        }
        return builder.build();
    }

}
