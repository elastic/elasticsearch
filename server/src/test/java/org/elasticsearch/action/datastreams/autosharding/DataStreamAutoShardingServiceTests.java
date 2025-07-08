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
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.datastreams.autosharding.DataStreamAutoShardingService.Decision;
import org.elasticsearch.action.datastreams.autosharding.DataStreamAutoShardingService.WriteLoadMetric;
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
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.IndexingStats;
import org.elasticsearch.index.shard.ShardId;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.action.datastreams.autosharding.AutoShardingResult.NOT_APPLICABLE_RESULT;
import static org.elasticsearch.action.datastreams.autosharding.AutoShardingType.COOLDOWN_PREVENTED_DECREASE;
import static org.elasticsearch.action.datastreams.autosharding.AutoShardingType.COOLDOWN_PREVENTED_INCREASE;
import static org.elasticsearch.action.datastreams.autosharding.AutoShardingType.DECREASE_SHARDS;
import static org.elasticsearch.action.datastreams.autosharding.AutoShardingType.INCREASE_SHARDS;
import static org.elasticsearch.action.datastreams.autosharding.AutoShardingType.NO_CHANGE_REQUIRED;
import static org.elasticsearch.action.datastreams.autosharding.DataStreamAutoShardingService.DATA_STREAMS_AUTO_SHARDING_DECREASE_SHARDS_LOAD_METRIC;
import static org.elasticsearch.action.datastreams.autosharding.DataStreamAutoShardingService.DATA_STREAMS_AUTO_SHARDING_INCREASE_SHARDS_LOAD_METRIC;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class DataStreamAutoShardingServiceTests extends ESTestCase {

    private static final int MIN_WRITE_THREADS = 2;
    private static final int MAX_WRITE_THREADS = 32;

    private ClusterSettings clusterSettings;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private List<Decision> decisionsLogged;
    private DataStreamAutoShardingService service;
    private long now;
    private String dataStreamName;

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
        clusterSettings = new ClusterSettings(Settings.EMPTY, builtInClusterSettings);
        clusterService = createClusterService(threadPool, clusterSettings);
        now = System.currentTimeMillis();
        decisionsLogged = new ArrayList<>();
        service = new DataStreamAutoShardingService(
            Settings.builder().put(DataStreamAutoShardingService.DATA_STREAMS_AUTO_SHARDING_ENABLED, true).build(),
            clusterService,
            () -> now,
            decisionsLogged::add
        );
        service.init();
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
            getWriteLoad(1, 2.0, 9999.0, 9999.0),
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

            AutoShardingResult autoShardingResult = disabledAutoshardingService.calculate(
                state.projectState(projectId),
                dataStream,
                createIndexStats(1, 9999.0, 9999.0, 2.0)
            );
            assertThat(autoShardingResult, is(NOT_APPLICABLE_RESULT));
        }

        {
            // null stats passed
            AutoShardingResult autoShardingResult = service.calculate(state.projectState(projectId), dataStream, null);
            assertThat(autoShardingResult, is(NOT_APPLICABLE_RESULT));
        }
    }

    public void testCalculateIncreaseShardingRecommendations_noPreviousShardingEvent() {
        // the input is a data stream with 5 backing indices with 1 shard each
        var projectId = randomProjectIdOrDefault();
        ProjectMetadata.Builder builder = ProjectMetadata.builder(projectId);
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            1,
            now,
            List.of(now - 10_000, now - 7000, now - 5000, now - 2000, now - 1000),
            getWriteLoad(1, 9999.0, 9999.0, 9999.0), // not used for increase calculation
            null
        );
        builder.put(dataStream);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create("n1")).add(DiscoveryNodeUtils.create("n2")))
            .putProjectMetadata(builder.build())
            .build();

        AutoShardingResult autoShardingResult = service.calculate(
            state.projectState(projectId),
            dataStream,
            createIndexStats(1, 9999.0, 9999.0, 2.5)
        );
        assertThat(autoShardingResult.type(), is(INCREASE_SHARDS));
        // no pre-existing scaling event so the cool down must be zero
        assertThat(autoShardingResult.coolDownRemaining(), is(TimeValue.ZERO));
        assertThat(autoShardingResult.targetNumberOfShards(), is(3));
    }

    public void testCalculateIncreaseShardingRecommendations_preventedByCooldown() {
        // the input is a data stream with 5 backing indices with 1 shard each
        // let's add a pre-existing sharding event so that we'll return some cool down period that's preventing an INCREASE_SHARDS
        // event so the result type we're expecting is COOLDOWN_PREVENTED_INCREASE
        var projectId = randomProjectIdOrDefault();
        ProjectMetadata.Builder builder = ProjectMetadata.builder(projectId);

        // generation 4 triggered an auto sharding event to 2 shards
        DataStreamAutoShardingEvent autoShardingEvent = new DataStreamAutoShardingEvent(
            DataStream.getDefaultBackingIndexName(dataStreamName, 4),
            2,
            now - 1005
        );
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            1,
            now,
            List.of(now - 10_000, now - 7000, now - 5000, now - 2000, now - 1000),
            getWriteLoad(1, 9999.0, 9999.0, 9999.0), // not used for increase calculation
            autoShardingEvent
        );
        builder.put(dataStream);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create("n1")).add(DiscoveryNodeUtils.create("n2")))
            .putProjectMetadata(builder.build())
            .build();

        AutoShardingResult autoShardingResult = service.calculate(
            state.projectState(projectId),
            dataStream,
            createIndexStats(1, 9999.0, 9999.0, 2.5)
        );
        assertThat(autoShardingResult.type(), is(COOLDOWN_PREVENTED_INCREASE));
        // no pre-existing scaling event so the cool down must be zero
        assertThat(autoShardingResult.targetNumberOfShards(), is(3));
        // it's been 1005 millis since the last auto sharding event and the cool down is 270 seconds (270_000 millis)
        assertThat(autoShardingResult.coolDownRemaining(), is(TimeValue.timeValueMillis(268995)));
    }

    public void testCalculateIncreaseShardingRecommendations_notPreventedByPreviousIncrease() {
        // the input is a data stream with 5 backing indices with 1 shard each
        // let's test a subsequent increase in the number of shards after a previous auto sharding event
        var projectId = randomProjectIdOrDefault();
        ProjectMetadata.Builder builder = ProjectMetadata.builder(projectId);
        // generation 3 triggered an increase in shards event to 2 shards
        DataStreamAutoShardingEvent autoShardingEvent = new DataStreamAutoShardingEvent(
            DataStream.getDefaultBackingIndexName(dataStreamName, 4),
            2,
            now - 2_000_100
        );
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            1,
            now,
            List.of(now - 10_000_000, now - 7_000_000, now - 2_000_000, now - 1_000_000, now - 1000),
            getWriteLoad(1, 9999.0, 9999.0, 9999.0), // not used for increase calculation
            autoShardingEvent
        );
        builder.put(dataStream);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create("n1")).add(DiscoveryNodeUtils.create("n2")))
            .putProjectMetadata(builder.build())
            .build();

        AutoShardingResult autoShardingResult = service.calculate(
            state.projectState(projectId),
            dataStream,
            createIndexStats(1, 9999.0, 9999.0, 2.5)
        );
        assertThat(autoShardingResult.type(), is(INCREASE_SHARDS));
        // no pre-existing scaling event so the cool down must be zero
        assertThat(autoShardingResult.targetNumberOfShards(), is(3));
        assertThat(autoShardingResult.coolDownRemaining(), is(TimeValue.ZERO));
    }

    public void testCalculateIncreaseShardingRecommendations_usingRecentWriteLoad() {
        // Repeated testCalculateIncreaseShardingRecommendations_noPreviousShardingEvent but with RECENT rather than PEAK write load
        var projectId = randomProjectIdOrDefault();
        ProjectMetadata.Builder builder = ProjectMetadata.builder(projectId);
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            1,
            now,
            List.of(now - 10_000, now - 7000, now - 5000, now - 2000, now - 1000),
            getWriteLoad(1, 9999.0, 9999.0, 9999.0), // not used for increase calculation
            null
        );
        builder.put(dataStream);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create("n1")).add(DiscoveryNodeUtils.create("n2")))
            .putProjectMetadata(builder.build())
            .build();

        doWithMetricSelection(DATA_STREAMS_AUTO_SHARDING_INCREASE_SHARDS_LOAD_METRIC, WriteLoadMetric.RECENT, () -> {
            AutoShardingResult autoShardingResult = service.calculate(
                state.projectState(projectId),
                dataStream,
                createIndexStats(1, 9999.0, 2.5, 9999.0)
            );
            assertThat(autoShardingResult.type(), is(INCREASE_SHARDS));
            // no pre-existing scaling event so the cool down must be zero
            assertThat(autoShardingResult.coolDownRemaining(), is(TimeValue.ZERO));
            assertThat(autoShardingResult.targetNumberOfShards(), is(3));
        });
    }

    public void testCalculateIncreaseShardingRecommendations_usingAllTimeWriteLoad() {
        // Repeated testCalculateIncreaseShardingRecommendations_noPreviousShardingEvent but with ALL_TIME rather than PEAK write load
        var projectId = randomProjectIdOrDefault();
        ProjectMetadata.Builder builder = ProjectMetadata.builder(projectId);
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            1,
            now,
            List.of(now - 10_000, now - 7000, now - 5000, now - 2000, now - 1000),
            getWriteLoad(1, 9999.0, 9999.0, 9999.0), // not used for increase calculation
            null
        );
        builder.put(dataStream);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create("n1")).add(DiscoveryNodeUtils.create("n2")))
            .putProjectMetadata(builder.build())
            .build();

        doWithMetricSelection(DATA_STREAMS_AUTO_SHARDING_INCREASE_SHARDS_LOAD_METRIC, WriteLoadMetric.ALL_TIME, () -> {
            AutoShardingResult autoShardingResult = service.calculate(
                state.projectState(projectId),
                dataStream,
                createIndexStats(1, 2.5, 9999.0, 9999.0)
            );
            assertThat(autoShardingResult.type(), is(INCREASE_SHARDS));
            // no pre-existing scaling event so the cool down must be zero
            assertThat(autoShardingResult.coolDownRemaining(), is(TimeValue.ZERO));
            assertThat(autoShardingResult.targetNumberOfShards(), is(3));
        });
    }

    public void testCalculateDecreaseShardingRecommendations_dataStreamNotOldEnough() {
        // the input is a data stream with 5 backing indices with 3 shards each
        // testing a decrease shards events prevented by the cool down period not lapsing due to the oldest generation index being
        // "too new" (i.e. the cool down period hasn't lapsed since the oldest generation index)
        var projectId = randomProjectIdOrDefault();
        ProjectMetadata.Builder builder = ProjectMetadata.builder(projectId);
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            3,
            now,
            List.of(now - 10_000, now - 7000, now - 5000, now - 2000, now - 1000),
            getWriteLoad(3, 9999.0, 9999.0, 0.25),
            null
        );
        builder.put(dataStream);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create("n1")).add(DiscoveryNodeUtils.create("n2")))
            .putProjectMetadata(builder.build())
            .build();

        AutoShardingResult autoShardingResult = service.calculate(
            state.projectState(projectId),
            dataStream,
            createIndexStats(3, 9999.0, 9999.0, 1.0 / 3)
        );
        // the cooldown period for the decrease shards event hasn't lapsed since the data stream was created
        assertThat(autoShardingResult.type(), is(COOLDOWN_PREVENTED_DECREASE));
        assertThat(autoShardingResult.coolDownRemaining(), is(TimeValue.timeValueMillis(TimeValue.timeValueDays(3).millis() - 10_000)));
        // based on the write load of 0.75 we should be reducing the number of shards to 1
        assertThat(autoShardingResult.targetNumberOfShards(), is(1));
    }

    public void testCalculateDecreaseShardingRecommendations_noPreviousShardingEvent() {
        // the input is a data stream with 5 backing indices with 3 shards each
        var projectId = randomProjectIdOrDefault();
        ProjectMetadata.Builder builder = ProjectMetadata.builder(projectId);
        DataStream dataStream = createDataStream(
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
            getWriteLoad(3, 9999.0, 9999.0, 0.333),
            null
        );
        builder.put(dataStream);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create("n1")).add(DiscoveryNodeUtils.create("n2")))
            .putProjectMetadata(builder.build())
            .build();

        AutoShardingResult autoShardingResult = service.calculate(
            state.projectState(projectId),
            dataStream,
            createIndexStats(3, 9999.0, 9999.0, 1.0 / 3)
        );
        assertThat(autoShardingResult.type(), is(DECREASE_SHARDS));
        assertThat(autoShardingResult.targetNumberOfShards(), is(1));
        // no pre-existing auto sharding event however we have old enough backing indices (older than the cooldown period) so we can
        // make a decision to reduce the number of shards
        assertThat(autoShardingResult.coolDownRemaining(), is(TimeValue.ZERO));
    }

    public void testCalculateDecreaseShardingRecommendations_notPreventedByPreviousDecrease() {
        // the input is a data stream with 5 backing indices with 3 shards each
        // let's test a decrease in number of shards after a previous decrease event
        var projectId = randomProjectIdOrDefault();
        ProjectMetadata.Builder builder = ProjectMetadata.builder(projectId);
        // generation 2 triggered a decrease in shards event to 2 shards
        DataStreamAutoShardingEvent autoShardingEvent = new DataStreamAutoShardingEvent(
            DataStream.getDefaultBackingIndexName(dataStreamName, 2),
            2,
            now - TimeValue.timeValueDays(4).getMillis()
        );
        DataStream dataStream = createDataStream(
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
            getWriteLoad(3, 9999.0, 9999.0, 0.333),
            autoShardingEvent
        );
        builder.put(dataStream);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create("n1")).add(DiscoveryNodeUtils.create("n2")))
            .putProjectMetadata(builder.build())
            .build();

        AutoShardingResult autoShardingResult = service.calculate(
            state.projectState(projectId),
            dataStream,
            createIndexStats(3, 9999.0, 9999.0, 1.0 / 3)
        );
        assertThat(autoShardingResult.type(), is(DECREASE_SHARDS));
        assertThat(autoShardingResult.targetNumberOfShards(), is(1));
        assertThat(autoShardingResult.coolDownRemaining(), is(TimeValue.ZERO));
    }

    public void testCalculateDecreaseShardingRecommendations_preventedByCooldown() {
        // the input is a data stream with 5 backing indices with 3 shards each
        // let's test a decrease in number of shards that's prevented by the cool down period due to a previous sharding event
        // the expected result type here is COOLDOWN_PREVENTED_DECREASE
        var projectId = randomProjectIdOrDefault();
        ProjectMetadata.Builder builder = ProjectMetadata.builder(projectId);
        // generation 2 triggered a decrease in shards event to 2 shards
        DataStreamAutoShardingEvent autoShardingEvent = new DataStreamAutoShardingEvent(
            DataStream.getDefaultBackingIndexName(dataStreamName, 2),
            2,
            now - TimeValue.timeValueDays(2).getMillis()
        );
        DataStream dataStream = createDataStream(
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
            getWriteLoad(3, 9999.0, 9999.0, 0.25),
            autoShardingEvent
        );
        builder.put(dataStream);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create("n1")).add(DiscoveryNodeUtils.create("n2")))
            .putProjectMetadata(builder.build())
            .build();

        AutoShardingResult autoShardingResult = service.calculate(
            state.projectState(projectId),
            dataStream,
            createIndexStats(3, 9999.0, 9999.0, 1.0 / 3)
        );
        assertThat(autoShardingResult.type(), is(COOLDOWN_PREVENTED_DECREASE));
        assertThat(autoShardingResult.targetNumberOfShards(), is(1));
        assertThat(autoShardingResult.coolDownRemaining(), is(TimeValue.timeValueDays(1)));
    }

    public void testCalculateDecreaseShardingRecommendations_noChangeRequired() {
        // the input is a data stream with 5 backing indices with 3 shards each
        // no change required
        var projectId = randomProjectIdOrDefault();
        ProjectMetadata.Builder builder = ProjectMetadata.builder(projectId);
        // generation 2 triggered a decrease in shards event to 2 shards
        DataStream dataStream = createDataStream(
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
            getWriteLoad(3, 1.333, 9999.0, 9999.0),
            null
        );
        builder.put(dataStream);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create("n1")).add(DiscoveryNodeUtils.create("n2")))
            .putProjectMetadata(builder.build())
            .build();

        AutoShardingResult autoShardingResult = service.calculate(
            state.projectState(projectId),
            dataStream,
            createIndexStats(3, 9999.0, 9999.0, 4.0 / 3)
        );
        assertThat(autoShardingResult.type(), is(NO_CHANGE_REQUIRED));
        assertThat(autoShardingResult.targetNumberOfShards(), is(3));
        assertThat(autoShardingResult.coolDownRemaining(), is(TimeValue.ZERO));
    }

    public void testCalculateDecreaseShardingRecommendations_usingRecentWriteLoad() {
        // Repeated testCalculateDecreaseShardingRecommendations_noPreviousShardingEvent but with RECENT rather than PEAK write load
        var projectId = randomProjectIdOrDefault();
        ProjectMetadata.Builder builder = ProjectMetadata.builder(projectId);
        DataStream dataStream = createDataStream(
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
            getWriteLoad(3, 9999.0, 0.333, 9999.0),
            null
        );
        builder.put(dataStream);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create("n1")).add(DiscoveryNodeUtils.create("n2")))
            .putProjectMetadata(builder.build())
            .build();

        doWithMetricSelection(DATA_STREAMS_AUTO_SHARDING_DECREASE_SHARDS_LOAD_METRIC, WriteLoadMetric.RECENT, () -> {
            AutoShardingResult autoShardingResult = service.calculate(
                state.projectState(projectId),
                dataStream,
                createIndexStats(3, 9999.0, 1.0 / 3, 0.5 / 3)
            );
            assertThat(autoShardingResult.type(), is(DECREASE_SHARDS));
            assertThat(autoShardingResult.targetNumberOfShards(), is(1));
            assertThat(autoShardingResult.coolDownRemaining(), is(TimeValue.ZERO));
        });
    }

    public void testCalculateDecreaseShardingRecommendations_usingAllTimeWriteLoad() {
        // Repeated testCalculateDecreaseShardingRecommendations_noPreviousShardingEvent but with ALL_TIME rather than PEAK write load
        var projectId = randomProjectIdOrDefault();
        ProjectMetadata.Builder builder = ProjectMetadata.builder(projectId);
        DataStream dataStream = createDataStream(
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
            getWriteLoad(3, 9999.0, 9999.0, 0.333),
            null
        );
        builder.put(dataStream);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create("n1")).add(DiscoveryNodeUtils.create("n2")))
            .putProjectMetadata(builder.build())
            .build();

        doWithMetricSelection(DATA_STREAMS_AUTO_SHARDING_DECREASE_SHARDS_LOAD_METRIC, WriteLoadMetric.PEAK, () -> {
            AutoShardingResult autoShardingResult = service.calculate(
                state.projectState(projectId),
                dataStream,
                createIndexStats(3, 1.0 / 3, 9999.0, 0.5 / 3)
            );
            assertThat(autoShardingResult.type(), is(DECREASE_SHARDS));
            assertThat(autoShardingResult.targetNumberOfShards(), is(1));
            assertThat(autoShardingResult.coolDownRemaining(), is(TimeValue.ZERO));
        });
    }

    public void testCalculateDecreaseShardingRecommendations_correctDecisionData() {
        var projectId = randomProjectIdOrDefault();
        ProjectMetadata.Builder builder = ProjectMetadata.builder(projectId);
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            3,
            now,
            List.of(
                now - TimeValue.timeValueDays(21).getMillis(), // outside cooling period
                now - TimeValue.timeValueDays(4).getMillis(), // within cooling period
                now - TimeValue.timeValueDays(2).getMillis(), // within cooling period
                now - TimeValue.timeValueDays(1).getMillis(), // within cooling period
                now - 1000
            ),
            List.of(
                getWriteLoad(3, 9999.0, 0.444 / 3, 9999.0),
                getWriteLoad(3, 9999.0, 0.222 / 3, 9999.0),
                getWriteLoad(3, 9999.0, 0.333 / 3, 9999.0),
                getWriteLoad(3, 9999.0, 0.111 / 3, 9999.0)
            ),
            null
        );
        builder.put(dataStream);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create("n1")).add(DiscoveryNodeUtils.create("n2")))
            .putProjectMetadata(builder.build())
            .build();

        doWithMetricSelection(DATA_STREAMS_AUTO_SHARDING_DECREASE_SHARDS_LOAD_METRIC, WriteLoadMetric.RECENT, () -> {
            AutoShardingResult autoShardingResult = service.calculate(
                state.projectState(projectId),
                dataStream,
                createIndexStats(3, 0.9 / 3, 0.3 / 3, 1.9 / 3)
            );
            assertThat(autoShardingResult.type(), is(DECREASE_SHARDS));
            assertThat(autoShardingResult.currentNumberOfShards(), is(3));
            assertThat(autoShardingResult.targetNumberOfShards(), is(1));
            assertThat(autoShardingResult.coolDownRemaining(), is(TimeValue.ZERO));
            assertThat(decisionsLogged, hasSize(1));
            Decision decision = decisionsLogged.get(0);
            assertThat(
                decision.inputs().increaseShardsCooldown(),
                equalTo(DataStreamAutoShardingService.DATA_STREAMS_AUTO_SHARDING_INCREASE_SHARDS_COOLDOWN.getDefault(Settings.EMPTY))
            );
            assertThat(
                decision.inputs().decreaseShardsCooldown(),
                equalTo(DataStreamAutoShardingService.DATA_STREAMS_AUTO_SHARDING_DECREASE_SHARDS_COOLDOWN.getDefault(Settings.EMPTY))
            );
            assertThat(
                decision.inputs().minWriteThreads(),
                equalTo(DataStreamAutoShardingService.CLUSTER_AUTO_SHARDING_MIN_WRITE_THREADS.getDefault(Settings.EMPTY))
            );
            assertThat(
                decision.inputs().maxWriteThreads(),
                equalTo(DataStreamAutoShardingService.CLUSTER_AUTO_SHARDING_MAX_WRITE_THREADS.getDefault(Settings.EMPTY))
            );
            assertThat(decision.inputs().increaseShardsMetric(), equalTo(WriteLoadMetric.PEAK));
            assertThat(decision.inputs().decreaseShardsMetric(), equalTo(WriteLoadMetric.RECENT));
            assertThat(decision.inputs().dataStream(), equalTo(dataStreamName));
            assertThat(decision.inputs().writeIndex(), equalTo(DataStream.getDefaultBackingIndexName(dataStreamName, 5, now - 1000)));
            assertThat(decision.inputs().writeIndexAllTimeLoad(), closeTo(0.9, 1.0e-8));
            assertThat(decision.inputs().writeIndexRecentLoad(), closeTo(0.3, 1.0e-8));
            assertThat(decision.inputs().writeIndexPeakLoad(), closeTo(1.9, 1.0e-8));
            assertThat(decision.inputs().currentNumberOfWriteIndexShards(), equalTo(3));
            assertThat(decision.increaseCalculation().writeIndexLoadForIncrease(), closeTo(1.9, 1.0e-8)); // all-time
            // Increase shard count based on all-time load of 1.9 for write index:
            assertThat(decision.increaseCalculation().optimalShardCountForIncrease(), equalTo(2));
            // The highest load for decrease (i.e. recent load) within the cooling period is the 0.333 from generation 3
            assertThat(decision.decreaseCalculation().maxLoadWithinCooldown().load(), closeTo(0.333, 1.0e-8));
            assertThat(
                decision.decreaseCalculation().maxLoadWithinCooldown().previousIndexWithMaxLoad(),
                equalTo(DataStream.getDefaultBackingIndexName(dataStreamName, 3, now - TimeValue.timeValueDays(2).getMillis()))
            );
            // Decrease shard count based on recent load of 0.333 for gen 3 index
            assertThat(decision.decreaseCalculation().optimalShardCountForDecrease(), equalTo(1));
            assertThat(decision.result(), equalTo(autoShardingResult));
        });
    }

    public void testComputeOptimalNumberOfShards_zeroLoad() {
        assertThat(DataStreamAutoShardingService.computeOptimalNumberOfShards(MIN_WRITE_THREADS, MAX_WRITE_THREADS, 0.0), is(1));
    }

    public void testComputeOptimalNumberOfShards_smallLoad() {
        // the small values will be very common so let's randomise to make sure we never go below 1L
        double indexingLoad = randomDoubleBetween(0.0001, 1.0, true);
        assertThat(DataStreamAutoShardingService.computeOptimalNumberOfShards(MIN_WRITE_THREADS, MAX_WRITE_THREADS, indexingLoad), is(1));
    }

    public void testComputeOptimalNumberOfShards_load2() {
        assertThat(DataStreamAutoShardingService.computeOptimalNumberOfShards(MIN_WRITE_THREADS, MAX_WRITE_THREADS, 2.0), is(2));
    }

    public void testComputeOptimalNumberOfShards_loadUpTo32() {
        // there's a broad range of popular values (a write index starting to be very busy, using between 3 and all of the 32 write
        // threads, so let's randomise this too to make sure we stay at 3 recommended shards)
        double indexingLoad = randomDoubleBetween(3.0002, 32.0, true);
        assertThat(DataStreamAutoShardingService.computeOptimalNumberOfShards(MIN_WRITE_THREADS, MAX_WRITE_THREADS, indexingLoad), is(3));
    }

    public void testComputeOptimalNumberOfShards_load49() {
        assertThat(DataStreamAutoShardingService.computeOptimalNumberOfShards(MIN_WRITE_THREADS, MAX_WRITE_THREADS, 49.0), is(4));
    }

    public void testComputeOptimalNumberOfShards_load70() {
        assertThat(DataStreamAutoShardingService.computeOptimalNumberOfShards(MIN_WRITE_THREADS, MAX_WRITE_THREADS, 70.0), is(5));
    }

    public void testComputeOptimalNumberOfShards_load100() {
        assertThat(DataStreamAutoShardingService.computeOptimalNumberOfShards(MIN_WRITE_THREADS, MAX_WRITE_THREADS, 100.0), is(7));
    }

    public void testComputeOptimalNumberOfShards_load180() {
        assertThat(DataStreamAutoShardingService.computeOptimalNumberOfShards(MIN_WRITE_THREADS, MAX_WRITE_THREADS, 180.0), is(12));
    }

    public void testGetMaxIndexLoadWithinCoolingPeriod_withLongHistory() {
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
                getWriteLoad(1, 999.0, 9999.0, 9999.0),
                creationDate
            );

            if (lastIndexBeforeCoolingPeriodHasLowWriteLoad) {
                indexMetadata = createIndexMetadata(
                    DataStream.getDefaultBackingIndexName(dataStreamName, backingIndices.size(), creationDate),
                    1,
                    getWriteLoad(1, 1.0, 9999.0, 9999.0),
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
                    getWriteLoad(3, 5.0, 9999.0, 9999.0), // max write index within cooling period
                    createdAt
                );
            } else {
                indexMetadata = createIndexMetadata(
                    DataStream.getDefaultBackingIndexName(dataStreamName, backingIndices.size(), createdAt),
                    3,
                    getWriteLoad(3, 3.0, 9999.0, 9999.0), // each backing index has a write load of 3.0
                    createdAt
                );
            }
            backingIndices.add(indexMetadata.getIndex());
            metadataBuilder.put(indexMetadata, false);
        }

        final String writeIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, backingIndices.size());
        final IndexMetadata writeIndexMetadata = createIndexMetadata(writeIndexName, 3, null, System.currentTimeMillis());
        backingIndices.add(writeIndexMetadata.getIndex());
        metadataBuilder.put(writeIndexMetadata, false);

        final DataStream dataStream = DataStream.builder(dataStreamName, backingIndices)
            .setGeneration(backingIndices.size())
            .setMetadata(Map.of())
            .setIndexMode(IndexMode.STANDARD)
            .build();

        metadataBuilder.put(dataStream);

        Decision.DecreaseCalculation.MaxLoadWithinCooldown maxIndexLoadWithinCoolingPeriod = DataStreamAutoShardingService
            .getMaxIndexLoadWithinCoolingPeriod(
                metadataBuilder.build().getProject(),
                dataStream,
                createDecisionInputsForMaxLoadInCooldownTests(3.0, 9999.0, 9999.0, WriteLoadMetric.RECENT, WriteLoadMetric.ALL_TIME),
                () -> now
            );
        // to cover the entire cooldown period, the last index before the cooling period is taken into account
        assertThat(maxIndexLoadWithinCoolingPeriod.load(), is(lastIndexBeforeCoolingPeriodHasLowWriteLoad ? 15.0 : 999.0));
    }

    public void testGetMaxIndexLoadWithinCoolingPeriod_sumsShardLoads() {
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
                    case 0 -> builder.withShardWriteLoad(shardId, 0.5, 9999.0, 9999.0, 40);
                    case 1 -> builder.withShardWriteLoad(shardId, 3.0, 9999.0, 9999.0, 10);
                    case 2 -> builder.withShardWriteLoad(shardId, 0.3333, 9999.0, 9999.0, 150);
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
        final IndexMetadata writeIndexMetadata = createIndexMetadata(writeIndexName, 3, null, System.currentTimeMillis());
        backingIndices.add(writeIndexMetadata.getIndex());
        metadataBuilder.put(writeIndexMetadata, false);

        final DataStream dataStream = DataStream.builder(dataStreamName, backingIndices)
            .setGeneration(backingIndices.size())
            .setMetadata(Map.of())
            .setIndexMode(IndexMode.STANDARD)
            .build();

        metadataBuilder.put(dataStream);

        Decision.DecreaseCalculation.MaxLoadWithinCooldown maxIndexLoadWithinCoolingPeriod = DataStreamAutoShardingService
            .getMaxIndexLoadWithinCoolingPeriod(
                metadataBuilder.build().getProject(),
                dataStream,
                createDecisionInputsForMaxLoadInCooldownTests(0.1, 9999.0, 9999.0, WriteLoadMetric.RECENT, WriteLoadMetric.ALL_TIME),
                () -> now
            );
        assertThat(maxIndexLoadWithinCoolingPeriod.load(), is(expectedIsSumOfShardLoads));
    }

    public void testGetMaxIndexLoadWithinCoolingPeriod_usingAllTimeWriteLoad() {
        TimeValue coolingPeriod = TimeValue.timeValueDays(3);
        Metadata.Builder metadataBuilder = Metadata.builder();
        List<Index> backingIndices = new ArrayList<>();
        String dataStreamName = "logs";
        long now = System.currentTimeMillis();
        long createdAt = now - (coolingPeriod.getMillis() / 2);

        IndexMetadata indexMetadata;
        indexMetadata = createIndexMetadata(
            DataStream.getDefaultBackingIndexName(dataStreamName, 0, createdAt),
            3,
            getWriteLoad(3, 5.0, 9999.0, 9999.0),
            createdAt
        );
        backingIndices.add(indexMetadata.getIndex());
        metadataBuilder.put(indexMetadata, false);

        String writeIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        IndexMetadata writeIndexMetadata = createIndexMetadata(writeIndexName, 3, null, System.currentTimeMillis());
        backingIndices.add(writeIndexMetadata.getIndex());
        metadataBuilder.put(writeIndexMetadata, false);

        DataStream dataStream = DataStream.builder(dataStreamName, backingIndices)
            .setGeneration(2)
            .setMetadata(Map.of())
            .setIndexMode(IndexMode.STANDARD)
            .build();

        metadataBuilder.put(dataStream);

        Decision.DecreaseCalculation.MaxLoadWithinCooldown maxIndexLoadWithinCoolingPeriod = DataStreamAutoShardingService
            .getMaxIndexLoadWithinCoolingPeriod(
                metadataBuilder.build().getProject(),
                dataStream,
                createDecisionInputsForMaxLoadInCooldownTests(3.0, 9999.0, 9999.0, WriteLoadMetric.RECENT, WriteLoadMetric.ALL_TIME),
                () -> now
            );
        assertThat(maxIndexLoadWithinCoolingPeriod.load(), equalTo(3 * 5.0));
    }

    public void testGetMaxIndexLoadWithinCoolingPeriod_usingRecentWriteLoad() {
        TimeValue coolingPeriod = TimeValue.timeValueDays(3);
        Metadata.Builder metadataBuilder = Metadata.builder();
        List<Index> backingIndices = new ArrayList<>();
        String dataStreamName = "logs";
        long now = System.currentTimeMillis();
        long createdAt = now - (coolingPeriod.getMillis() / 2);

        IndexMetadata indexMetadata;
        indexMetadata = createIndexMetadata(
            DataStream.getDefaultBackingIndexName(dataStreamName, 0, createdAt),
            3,
            getWriteLoad(3, 9999.0, 5.0, 9999.0),
            createdAt
        );
        backingIndices.add(indexMetadata.getIndex());
        metadataBuilder.put(indexMetadata, false);

        String writeIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        IndexMetadata writeIndexMetadata = createIndexMetadata(writeIndexName, 3, null, System.currentTimeMillis());
        backingIndices.add(writeIndexMetadata.getIndex());
        metadataBuilder.put(writeIndexMetadata, false);

        DataStream dataStream = DataStream.builder(dataStreamName, backingIndices)
            .setGeneration(2)
            .setMetadata(Map.of())
            .setIndexMode(IndexMode.STANDARD)
            .build();

        metadataBuilder.put(dataStream);

        Decision.DecreaseCalculation.MaxLoadWithinCooldown maxIndexLoadWithinCoolingPeriod = DataStreamAutoShardingService
            .getMaxIndexLoadWithinCoolingPeriod(
                metadataBuilder.build().getProject(),
                dataStream,
                createDecisionInputsForMaxLoadInCooldownTests(9999.0, 3.0, 9999.0, WriteLoadMetric.ALL_TIME, WriteLoadMetric.RECENT),
                () -> now
            );
        assertThat(maxIndexLoadWithinCoolingPeriod.load(), equalTo(3 * 5.0));
    }

    public void testGetMaxIndexLoadWithinCoolingPeriod_usingPeakWriteLoad() {
        TimeValue coolingPeriod = TimeValue.timeValueDays(3);
        Metadata.Builder metadataBuilder = Metadata.builder();
        List<Index> backingIndices = new ArrayList<>();
        String dataStreamName = "logs";
        long now = System.currentTimeMillis();
        long createdAt = now - (coolingPeriod.getMillis() / 2);

        IndexMetadata indexMetadata;
        indexMetadata = createIndexMetadata(
            DataStream.getDefaultBackingIndexName(dataStreamName, 0, createdAt),
            3,
            getWriteLoad(3, 9999.0, 9999.0, 5.0),
            createdAt
        );
        backingIndices.add(indexMetadata.getIndex());
        metadataBuilder.put(indexMetadata, false);

        String writeIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        IndexMetadata writeIndexMetadata = createIndexMetadata(writeIndexName, 3, null, System.currentTimeMillis());
        backingIndices.add(writeIndexMetadata.getIndex());
        metadataBuilder.put(writeIndexMetadata, false);

        DataStream dataStream = DataStream.builder(dataStreamName, backingIndices)
            .setGeneration(2)
            .setMetadata(Map.of())
            .setIndexMode(IndexMode.STANDARD)
            .build();

        metadataBuilder.put(dataStream);

        Decision.DecreaseCalculation.MaxLoadWithinCooldown maxIndexLoadWithinCoolingPeriod = DataStreamAutoShardingService
            .getMaxIndexLoadWithinCoolingPeriod(
                metadataBuilder.build().getProject(),
                dataStream,
                createDecisionInputsForMaxLoadInCooldownTests(9999.0, 9999.0, 3.0, WriteLoadMetric.RECENT, WriteLoadMetric.PEAK),
                () -> now
            );
        assertThat(maxIndexLoadWithinCoolingPeriod.load(), equalTo(3 * 5.0));
    }

    private Decision.Inputs createDecisionInputsForMaxLoadInCooldownTests(
        double writeIndexAllTimeLoad,
        double writeIndexRecentLoad,
        double writeIndexPeakLoad,
        WriteLoadMetric increaseShardsMetric,
        WriteLoadMetric decreaseShardsMetric
    ) {
        return new Decision.Inputs(
            TimeValue.timeValueSeconds(270),
            TimeValue.timeValueDays(3),
            2,
            32,
            increaseShardsMetric,
            decreaseShardsMetric,
            dataStreamName,
            "the-write-index",
            writeIndexAllTimeLoad,
            writeIndexRecentLoad,
            writeIndexPeakLoad,
            3
        );
    }

    public void testAutoShardingResultValidation_increaseShardsShouldNotReportCooldown() {
        expectThrows(IllegalArgumentException.class, () -> new AutoShardingResult(INCREASE_SHARDS, 1, 3, TimeValue.timeValueSeconds(3)));
    }

    public void testAutoShardingResultValidation_decreaseShardsShouldNotReportCooldown() {
        expectThrows(IllegalArgumentException.class, () -> new AutoShardingResult(DECREASE_SHARDS, 3, 1, TimeValue.timeValueSeconds(3)));
    }

    public void testAutoShardingResultValidation_validCooldownPreventedIncrease() {
        AutoShardingResult cooldownPreventedIncrease = new AutoShardingResult(
            COOLDOWN_PREVENTED_INCREASE,
            1,
            3,
            TimeValue.timeValueSeconds(3)
        );
        assertThat(cooldownPreventedIncrease.coolDownRemaining(), is(TimeValue.timeValueSeconds(3)));
    }

    public void testAutoShardingResultValidation_validCooldownPreventedDecrease() {
        AutoShardingResult cooldownPreventedDecrease = new AutoShardingResult(
            COOLDOWN_PREVENTED_DECREASE,
            3,
            1,
            TimeValue.timeValueSeconds(7)
        );
        assertThat(cooldownPreventedDecrease.coolDownRemaining(), is(TimeValue.timeValueSeconds(7)));
    }

    private IndexStats createIndexStats(int numberOfShards, double shardWriteLoad, double shardRecentWriteLoad, double shardPeakWriteLoad) {
        String indexName = DataStream.getDefaultBackingIndexName(dataStreamName, 99); // the generation number here is not used
        Index index = new Index(indexName, randomUUID());
        IndexStats.IndexStatsBuilder builder = new IndexStats.IndexStatsBuilder(indexName, randomUUID(), null, null);
        for (int shardNumber = 0; shardNumber < numberOfShards; shardNumber++) {
            // Add stats for the primary:
            builder.add(createShardStats(shardWriteLoad, shardRecentWriteLoad, shardPeakWriteLoad, new ShardId(index, shardNumber), true));
            // Add stats for a replica, which should be ignored:
            builder.add(createShardStats(9999.0, 9999.0, 9999.0, new ShardId(index, shardNumber), false));
        }
        return builder.build();
    }

    private static ShardStats createShardStats(
        double indexingLoad,
        double recentIndexingLoad,
        double peakIndexingLoad,
        ShardId shardId,
        boolean primary
    ) {
        ShardRouting shardRouting = TestShardRouting.newShardRouting(shardId, "unused-node-id", primary, ShardRoutingState.STARTED);
        CommonStats commonStats = new CommonStats();
        commonStats.indexing = createIndexingStats(indexingLoad, recentIndexingLoad, peakIndexingLoad);
        return new ShardStats(shardRouting, commonStats, null, null, null, null, null, false, false, 0);
    }

    private static IndexingStats createIndexingStats(double indexingLoad, double recentIndexingLoad, double peakIndexingLoad) {
        int totalActiveTimeInNanos = 1_000_000_000;
        // Use the correct indexing time to give the required all-time load value of indexingLoad (aside from the rounding errors):
        long totalIndexingTimeSinceShardStartedInNanos = (long) (indexingLoad * totalActiveTimeInNanos);
        return new IndexingStats(
            new IndexingStats.Stats(
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                false,
                0,
                totalIndexingTimeSinceShardStartedInNanos,
                totalIndexingTimeSinceShardStartedInNanos,
                totalActiveTimeInNanos,
                recentIndexingLoad,
                peakIndexingLoad
            )
        );
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
        return createDataStream(
            builder,
            dataStreamName,
            numberOfShards,
            now,
            indicesCreationDate,
            Stream.generate(() -> backingIndicesWriteLoad).limit(indicesCreationDate.size() - 1).toList(),
            autoShardingEvent
        );
    }

    private DataStream createDataStream(
        ProjectMetadata.Builder builder,
        String dataStreamName,
        int numberOfShards,
        Long now,
        List<Long> indicesCreationDate,
        List<IndexWriteLoad> backingIndicesWriteLoads,
        @Nullable DataStreamAutoShardingEvent autoShardingEvent
    ) {
        assert backingIndicesWriteLoads.size() == indicesCreationDate.size() - 1 : "Expected index load for all except write index";
        final List<Index> backingIndices = new ArrayList<>();
        int backingIndicesCount = indicesCreationDate.size();
        for (int k = 0; k < indicesCreationDate.size(); k++) {
            long createdAt = indicesCreationDate.get(k);
            IndexMetadata.Builder indexMetaBuilder;
            if (k < backingIndicesCount - 1) {
                IndexWriteLoad backingIndexWriteLoad = backingIndicesWriteLoads.get(k);
                indexMetaBuilder = IndexMetadata.builder(
                    createIndexMetadata(
                        DataStream.getDefaultBackingIndexName(dataStreamName, k + 1, createdAt),
                        numberOfShards,
                        backingIndexWriteLoad,
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

    private IndexWriteLoad getWriteLoad(int numberOfShards, double shardWriteLoad, double shardRecentWriteLoad, double shardPeakWriteLoad) {
        IndexWriteLoad.Builder builder = IndexWriteLoad.builder(numberOfShards);
        for (int shardId = 0; shardId < numberOfShards; shardId++) {
            builder.withShardWriteLoad(shardId, shardWriteLoad, shardRecentWriteLoad, shardPeakWriteLoad, 1);
        }
        return builder.build();
    }

    private void doWithMetricSelection(Setting<WriteLoadMetric> setting, WriteLoadMetric metric, Runnable action) {
        clusterSettings.applySettings(Settings.builder().put(setting.getKey(), metric).build());
        try {
            action.run();
        } finally {
            clusterSettings.applySettings(Settings.builder().put(setting.getKey(), setting.getDefault(Settings.EMPTY)).build());
        }
    }

    // Tests for PeriodicDecisionLogger

    private static class FlushedDecisionsRecorder {

        List<Decision> highestLoadIncreaseDecisions = new ArrayList<>();
        List<Decision> highestLoadNonIncreaseDecisions = new ArrayList<>();

        public void record(DataStreamAutoShardingService.PeriodicDecisionLogger.FlushedDecisions flushedDecisions) {
            highestLoadIncreaseDecisions.addAll(flushedDecisions.highestLoadIncreaseDecisions());
            highestLoadNonIncreaseDecisions.addAll(flushedDecisions.highestLoadNonIncreaseDecisions());
        }

        public void clear() {
            highestLoadIncreaseDecisions.clear();
            highestLoadNonIncreaseDecisions.clear();
        }
    }

    public void testPeriodResultLogger_logsPeriodically() {
        long start = System.currentTimeMillis();
        AtomicLong clock = new AtomicLong(start);
        FlushedDecisionsRecorder recorder = new FlushedDecisionsRecorder();
        DataStreamAutoShardingService.PeriodicDecisionLogger periodicDecisionLogger =
            new DataStreamAutoShardingService.PeriodicDecisionLogger(clock::get, recorder::record);

        // Should not flush when logging at the start time:
        periodicDecisionLogger.maybeLogDecision(createNoChangeDecision(1));
        assertThat(recorder.highestLoadNonIncreaseDecisions, empty());

        // Should not flush when logging in the past:
        clock.set(start - 1);
        periodicDecisionLogger.maybeLogDecision(createNoChangeDecision(2));
        assertThat(recorder.highestLoadNonIncreaseDecisions, empty());

        // Should not flush when logging just before the interval since the start has elapsed:
        clock.set(start + DataStreamAutoShardingService.PeriodicDecisionLogger.FLUSH_INTERVAL_MILLIS - 1);
        periodicDecisionLogger.maybeLogDecision(createNoChangeDecision(3));
        assertThat(recorder.highestLoadNonIncreaseDecisions, empty());

        // Should flush when logging at exactly the interval since the start has elapsed:
        long firstFlushTime = start + DataStreamAutoShardingService.PeriodicDecisionLogger.FLUSH_INTERVAL_MILLIS;
        clock.set(firstFlushTime);
        periodicDecisionLogger.maybeLogDecision(createNoChangeDecision(4));
        assertThat(
            recorder.highestLoadNonIncreaseDecisions.stream().map(d -> d.inputs().dataStream()).toList(),
            containsInAnyOrder("data-stream-1", "data-stream-2", "data-stream-3", "data-stream-4")
        );
        recorder.clear();

        // Should not flush a second time when logging again at the same time:
        periodicDecisionLogger.maybeLogDecision(createNoChangeDecision(5));
        assertThat(recorder.highestLoadNonIncreaseDecisions, empty());

        // Should not flush a second time when logging just before the interval since the first flush has elapsed:
        clock.set(firstFlushTime + DataStreamAutoShardingService.PeriodicDecisionLogger.FLUSH_INTERVAL_MILLIS - 1);
        periodicDecisionLogger.maybeLogDecision(createNoChangeDecision(6));
        assertThat(recorder.highestLoadNonIncreaseDecisions, empty());

        // Should flush a second time when logging some extra time after the interval since the first flush has elapsed:
        long secondFlushTime = firstFlushTime + DataStreamAutoShardingService.PeriodicDecisionLogger.FLUSH_INTERVAL_MILLIS + 123456L;
        clock.set(secondFlushTime);
        periodicDecisionLogger.maybeLogDecision(createNoChangeDecision(7));
        assertThat(
            recorder.highestLoadNonIncreaseDecisions.stream().map(d -> d.inputs().dataStream()).toList(),
            containsInAnyOrder("data-stream-5", "data-stream-6", "data-stream-7")
        );
        recorder.clear();

        // Should not flush a third time when logging just before the interval since the second flush has elapsed:
        // (N.B. This time is more than two intervals since the start, but we count time from the last flush.)
        clock.set(secondFlushTime + DataStreamAutoShardingService.PeriodicDecisionLogger.FLUSH_INTERVAL_MILLIS - 1);
        periodicDecisionLogger.maybeLogDecision(createNoChangeDecision(8));
        assertThat(recorder.highestLoadNonIncreaseDecisions, empty());

        // Should flush a third time when logging at exactly the interval since the second flush has elapsed:
        clock.set(secondFlushTime + DataStreamAutoShardingService.PeriodicDecisionLogger.FLUSH_INTERVAL_MILLIS);
        periodicDecisionLogger.maybeLogDecision(createNoChangeDecision(9));
        assertThat(
            recorder.highestLoadNonIncreaseDecisions.stream().map(d -> d.inputs().dataStream()).toList(),
            containsInAnyOrder("data-stream-8", "data-stream-9")
        );
    }

    public void testPeriodResultLogger_logsHighestLoadNonIncrementDecisions() {
        long start = System.nanoTime();
        AtomicLong clock = new AtomicLong(start);
        FlushedDecisionsRecorder recorder = new FlushedDecisionsRecorder();
        DataStreamAutoShardingService.PeriodicDecisionLogger periodicDecisionLogger =
            new DataStreamAutoShardingService.PeriodicDecisionLogger(clock::get, recorder::record);

        // Pass in 13 decisions, in the order 8, 7, 6, 5; 13, 12, 11, 10; 4, 3, 2, 1; 9. Updating the clock before the last decision.
        for (int i = 8; i >= 5; i--) {
            periodicDecisionLogger.maybeLogDecision(createNoChangeDecision(i));
        }
        for (int i = 13; i >= 10; i--) {
            periodicDecisionLogger.maybeLogDecision(createNoChangeDecision(i));
        }
        for (int i = 4; i >= 1; i--) {
            periodicDecisionLogger.maybeLogDecision(createNoChangeDecision(i));
        }
        clock.set(start + DataStreamAutoShardingService.PeriodicDecisionLogger.FLUSH_INTERVAL_MILLIS);
        periodicDecisionLogger.maybeLogDecision(createNoChangeDecision(9));

        // Should have logged the 10 decisions with the highest load, in decreasing order, i.e. number 13 down to number 4:
        assertThat(
            recorder.highestLoadNonIncreaseDecisions.stream().map(d -> d.inputs().dataStream()).toList(),
            contains(IntStream.rangeClosed(4, 13).mapToObj(i -> "data-stream-" + i).toList().reversed().toArray())
        );
    }

    public void testPeriodResultLogger_separatesIncreaseAndNonIncreaseDecisions() {
        long start = System.nanoTime();
        AtomicLong clock = new AtomicLong(start);
        FlushedDecisionsRecorder recorder = new FlushedDecisionsRecorder();
        DataStreamAutoShardingService.PeriodicDecisionLogger periodicDecisionLogger =
            new DataStreamAutoShardingService.PeriodicDecisionLogger(clock::get, recorder::record);

        // Pass in 3 decisions. Update the clock before the last decision. The highest load decision has an increment result.
        periodicDecisionLogger.maybeLogDecision(createNoChangeDecision(1));
        periodicDecisionLogger.maybeLogDecision(createIncreaseDecision(3));
        clock.set(start + DataStreamAutoShardingService.PeriodicDecisionLogger.FLUSH_INTERVAL_MILLIS);
        periodicDecisionLogger.maybeLogDecision(createNoChangeDecision(2));

        // Assert that we correctly separated the increase and the non-increase decisions.
        assertThat(
            recorder.highestLoadIncreaseDecisions.stream().map(d -> d.inputs().dataStream()).toList(),
            containsInAnyOrder("data-stream-3")
        );
        assertThat(
            recorder.highestLoadNonIncreaseDecisions.stream().map(d -> d.inputs().dataStream()).toList(),
            containsInAnyOrder("data-stream-1", "data-stream-2")
        );
    }

    private DataStreamAutoShardingService.Decision createNoChangeDecision(int writeIndexLoad) {
        return new Decision(
            createDecisionInputsForPeriodLoggerTests(writeIndexLoad),
            new Decision.IncreaseCalculation(1.0 * writeIndexLoad, 2, null),
            null,
            new AutoShardingResult(AutoShardingType.NO_CHANGE_REQUIRED, 3, 3, TimeValue.ZERO)
        );
    }

    private DataStreamAutoShardingService.Decision createIncreaseDecision(int writeIndexLoad) {
        AutoShardingResult result = new AutoShardingResult(INCREASE_SHARDS, 3, 4, TimeValue.ZERO);
        return new Decision(
            createDecisionInputsForPeriodLoggerTests(writeIndexLoad),
            new Decision.IncreaseCalculation(1.0 * writeIndexLoad, 4, result),
            null,
            result
        );
    }

    private static Decision.Inputs createDecisionInputsForPeriodLoggerTests(int writeIndexLoadForIncreaseAndDataStreamName) {
        return new Decision.Inputs(
            TimeValue.timeValueSeconds(270),
            TimeValue.timeValueDays(3),
            2,
            32,
            WriteLoadMetric.PEAK,
            WriteLoadMetric.ALL_TIME,
            "data-stream-" + writeIndexLoadForIncreaseAndDataStreamName,
            "the-write-index",
            0.1,
            0.2,
            1.0 * writeIndexLoadForIncreaseAndDataStreamName,
            3
        );
    }
}
