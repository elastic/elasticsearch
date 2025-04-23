/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.datastreams.autosharding.AutoShardingResult;
import org.elasticsearch.action.datastreams.autosharding.AutoShardingType;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamAutoShardingEvent;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.elasticsearch.action.datastreams.autosharding.AutoShardingType.COOLDOWN_PREVENTED_DECREASE;
import static org.elasticsearch.action.datastreams.autosharding.AutoShardingType.COOLDOWN_PREVENTED_INCREASE;
import static org.elasticsearch.action.datastreams.autosharding.AutoShardingType.DECREASE_SHARDS;
import static org.elasticsearch.action.datastreams.autosharding.AutoShardingType.INCREASE_SHARDS;
import static org.elasticsearch.action.datastreams.autosharding.AutoShardingType.NOT_APPLICABLE;
import static org.elasticsearch.action.datastreams.autosharding.AutoShardingType.NO_CHANGE_REQUIRED;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class MetadataRolloverServiceAutoShardingTests extends ESTestCase {

    private final ProjectId projectId = randomProjectIdOrDefault();

    public void testRolloverDataStreamWithoutExistingAutosharding() throws Exception {
        String dataStreamName = "no_preexising_autoshard_event_ds";
        DataStream dataStream = DataStreamTestHelper.newInstance(
            dataStreamName,
            List.of(
                new Index(DataStream.getDefaultBackingIndexName(dataStreamName, 1), UUID.randomUUID().toString()),
                new Index(DataStream.getDefaultBackingIndexName(dataStreamName, 2), UUID.randomUUID().toString()),
                new Index(DataStream.getDefaultBackingIndexName(dataStreamName, 3), UUID.randomUUID().toString()),
                new Index(DataStream.getDefaultBackingIndexName(dataStreamName, 4), UUID.randomUUID().toString()),
                new Index(DataStream.getDefaultBackingIndexName(dataStreamName, 5), UUID.randomUUID().toString())
            ),
            5,
            null,
            false,
            null,
            (DataStreamAutoShardingEvent) null
        );
        ComposableIndexTemplate template = ComposableIndexTemplate.builder()
            .indexPatterns(List.of(dataStreamName + "*"))
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
            // all indices have, by default 3 shards (using a value GT 1 so we can test decreasing the number of shards)
            .template(new Template(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3).build(), null, null))
            .build();
        ProjectMetadata.Builder builder = ProjectMetadata.builder(projectId);
        builder.put("template", template);
        for (Index index : dataStream.getIndices()) {
            // all indices have, by default 3 shards (using a value GT 1 so we can test decreasing the number of shards)
            builder.put(getIndexMetadataBuilderForIndex(index, 3));
        }
        builder.put(dataStream);
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(builder).build();

        final TestTelemetryPlugin telemetryPlugin = new TestTelemetryPlugin();
        ThreadPool testThreadPool = new TestThreadPool(getTestName());
        try {
            MetadataRolloverService rolloverService = DataStreamTestHelper.getMetadataRolloverService(
                dataStream,
                testThreadPool,
                Set.of(),
                xContentRegistry(),
                telemetryPlugin.getTelemetryProvider(Settings.EMPTY)
            );

            // let's rollover the data stream using all the possible autosharding recommendations
            for (AutoShardingType type : AutoShardingType.values()) {
                telemetryPlugin.resetMeter();
                long before = testThreadPool.absoluteTimeInMillis();
                switch (type) {
                    case INCREASE_SHARDS -> {
                        List<Condition<?>> metConditions = List.of(new OptimalShardCountCondition(5));
                        MetadataRolloverService.RolloverResult rolloverResult = rolloverService.rolloverClusterState(
                            clusterState.projectState(projectId),
                            dataStream.getName(),
                            null,
                            new CreateIndexRequest("_na_"),
                            metConditions,
                            Instant.now(),
                            randomBoolean(),
                            false,
                            null,
                            new AutoShardingResult(INCREASE_SHARDS, 3, 5, TimeValue.ZERO),
                            false
                        );
                        assertRolloverResult(dataStream, rolloverResult, before, testThreadPool.absoluteTimeInMillis(), metConditions, 5);
                        assertTelemetry(
                            telemetryPlugin,
                            "es.auto_sharding.increase_shards.total",
                            List.of(
                                "es.auto_sharding.decrease_shards.total",
                                "es.auto_sharding.cooldown_prevented_increase.total",
                                "es.auto_sharding.cooldown_prevented_decrease.total"
                            )
                        );
                    }
                    case DECREASE_SHARDS -> {
                        {
                            // we have another condition that matched, so the rollover will be executed and the new number of shards
                            // will be 1
                            List<Condition<?>> metConditions = List.of(new MaxDocsCondition(2L), new OptimalShardCountCondition(1));
                            MetadataRolloverService.RolloverResult rolloverResult = rolloverService.rolloverClusterState(
                                clusterState.projectState(projectId),
                                dataStream.getName(),
                                null,
                                new CreateIndexRequest("_na_"),
                                metConditions,
                                Instant.now(),
                                randomBoolean(),
                                false,
                                null,
                                new AutoShardingResult(DECREASE_SHARDS, 3, 1, TimeValue.ZERO),
                                false
                            );
                            assertRolloverResult(
                                dataStream,
                                rolloverResult,
                                before,
                                testThreadPool.absoluteTimeInMillis(),
                                metConditions,
                                1
                            );
                            assertTelemetry(
                                telemetryPlugin,
                                "es.auto_sharding.decrease_shards.total",
                                List.of(
                                    "es.auto_sharding.increase_shards.total",
                                    "es.auto_sharding.cooldown_prevented_increase.total",
                                    "es.auto_sharding.cooldown_prevented_decrease.total"
                                )
                            );
                        }

                        {
                            // even if the decrease shards recommendation is not a rollover condition, an empty POST _rollover request will
                            // configure the decrease shards recommendation
                            List<Condition<?>> metConditions = List.of(new OptimalShardCountCondition(1));
                            MetadataRolloverService.RolloverResult rolloverResult = rolloverService.rolloverClusterState(
                                clusterState.projectState(projectId),
                                dataStream.getName(),
                                null,
                                new CreateIndexRequest("_na_"),
                                metConditions,
                                Instant.now(),
                                randomBoolean(),
                                false,
                                null,
                                new AutoShardingResult(DECREASE_SHARDS, 3, 1, TimeValue.ZERO),
                                false
                            );
                            assertRolloverResult(
                                dataStream,
                                rolloverResult,
                                before,
                                testThreadPool.absoluteTimeInMillis(),
                                metConditions,
                                1
                            );
                        }
                    }
                    case COOLDOWN_PREVENTED_INCREASE -> {
                        AutoShardingResult autoShardingResult = new AutoShardingResult(
                            COOLDOWN_PREVENTED_INCREASE,
                            3,
                            5,
                            TimeValue.timeValueMinutes(10)
                        );
                        MetadataRolloverService.RolloverResult rolloverResult = rolloverService.rolloverClusterState(
                            clusterState.projectState(projectId),
                            dataStream.getName(),
                            null,
                            new CreateIndexRequest("_na_"),
                            List.of(),
                            Instant.now(),
                            randomBoolean(),
                            false,
                            null,
                            new AutoShardingResult(COOLDOWN_PREVENTED_INCREASE, 3, 5, TimeValue.timeValueMinutes(10)),
                            false
                        );
                        // the expected number of shards remains 3 for the data stream due to the remaining cooldown
                        assertRolloverResult(dataStream, rolloverResult, before, testThreadPool.absoluteTimeInMillis(), List.of(), 3);
                        assertTelemetry(
                            telemetryPlugin,
                            "es.auto_sharding.cooldown_prevented_increase.total",
                            List.of(
                                "es.auto_sharding.increase_shards.total",
                                "es.auto_sharding.decrease_shards.total",
                                "es.auto_sharding.cooldown_prevented_decrease.total"
                            )
                        );
                    }
                    case COOLDOWN_PREVENTED_DECREASE -> {
                        MetadataRolloverService.RolloverResult rolloverResult = rolloverService.rolloverClusterState(
                            clusterState.projectState(projectId),
                            dataStream.getName(),
                            null,
                            new CreateIndexRequest("_na_"),
                            List.of(),
                            Instant.now(),
                            randomBoolean(),
                            false,
                            null,
                            new AutoShardingResult(COOLDOWN_PREVENTED_DECREASE, 3, 1, TimeValue.timeValueMinutes(10)),
                            false
                        );
                        // the expected number of shards remains 3 for the data stream due to the remaining cooldown
                        assertRolloverResult(dataStream, rolloverResult, before, testThreadPool.absoluteTimeInMillis(), List.of(), 3);
                        assertTelemetry(
                            telemetryPlugin,
                            "es.auto_sharding.cooldown_prevented_decrease.total",
                            List.of(
                                "es.auto_sharding.increase_shards.total",
                                "es.auto_sharding.decrease_shards.total",
                                "es.auto_sharding.cooldown_prevented_increase.total"
                            )
                        );
                    }
                    case NO_CHANGE_REQUIRED -> {
                        List<Condition<?>> metConditions = List.of(new MaxDocsCondition(randomNonNegativeLong()));
                        MetadataRolloverService.RolloverResult rolloverResult = rolloverService.rolloverClusterState(
                            clusterState.projectState(projectId),
                            dataStream.getName(),
                            null,
                            new CreateIndexRequest("_na_"),
                            metConditions,
                            Instant.now(),
                            randomBoolean(),
                            false,
                            null,
                            new AutoShardingResult(NO_CHANGE_REQUIRED, 3, 3, TimeValue.ZERO),
                            false
                        );
                        assertRolloverResult(dataStream, rolloverResult, before, testThreadPool.absoluteTimeInMillis(), metConditions, 3);
                        assertTelemetry(
                            telemetryPlugin,
                            null,
                            List.of(
                                "es.auto_sharding.increase_shards.total",
                                "es.auto_sharding.decrease_shards.total",
                                "es.auto_sharding.cooldown_prevented_increase.total",
                                "es.auto_sharding.cooldown_prevented_decrease.total"
                            )
                        );
                    }
                    case NOT_APPLICABLE -> {
                        List<Condition<?>> metConditions = List.of(new MaxDocsCondition(randomNonNegativeLong()));
                        MetadataRolloverService.RolloverResult rolloverResult = rolloverService.rolloverClusterState(
                            clusterState.projectState(projectId),
                            dataStream.getName(),
                            null,
                            new CreateIndexRequest("_na_"),
                            metConditions,
                            Instant.now(),
                            randomBoolean(),
                            false,
                            null,
                            new AutoShardingResult(NOT_APPLICABLE, 1, 1, TimeValue.MAX_VALUE),
                            false
                        );
                        assertRolloverResult(dataStream, rolloverResult, before, testThreadPool.absoluteTimeInMillis(), metConditions, 3);
                        assertTelemetry(
                            telemetryPlugin,
                            null,
                            List.of(
                                "es.auto_sharding.increase_shards.total",
                                "es.auto_sharding.decrease_shards.total",
                                "es.auto_sharding.cooldown_prevented_increase.total",
                                "es.auto_sharding.cooldown_prevented_decrease.total"
                            )
                        );
                    }
                }
            }
        } finally {
            testThreadPool.shutdown();
        }
    }

    public void testRolloverDataStreamWithExistingAutoShardEvent() throws Exception {
        String dataStreamName = "ds_with_existing_autoshard_event";
        String autoShardEventTriggerIndex = DataStream.getDefaultBackingIndexName(dataStreamName, 3);
        DataStream dataStream = DataStreamTestHelper.newInstance(
            dataStreamName,
            List.of(
                new Index(DataStream.getDefaultBackingIndexName(dataStreamName, 1), UUID.randomUUID().toString()),
                new Index(DataStream.getDefaultBackingIndexName(dataStreamName, 2), UUID.randomUUID().toString()),
                new Index(autoShardEventTriggerIndex, UUID.randomUUID().toString()),
                new Index(DataStream.getDefaultBackingIndexName(dataStreamName, 4), UUID.randomUUID().toString()),
                new Index(DataStream.getDefaultBackingIndexName(dataStreamName, 5), UUID.randomUUID().toString())
            ),
            5,
            null,
            false,
            null,
            new DataStreamAutoShardingEvent(autoShardEventTriggerIndex, 3, System.currentTimeMillis())
        );
        ComposableIndexTemplate template = ComposableIndexTemplate.builder()
            .indexPatterns(List.of(dataStreamName + "*"))
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
            // the index template does not configure any number of shards so we'll default to 1
            .build();
        ProjectMetadata.Builder builder = ProjectMetadata.builder(projectId);
        builder.put("template", template);
        int numberOfShards = 1;
        for (Index index : dataStream.getIndices()) {
            if (index.getName().equals(autoShardEventTriggerIndex)) {
                // we configure the indices to have 1 shard until the auto shard trigger index, after which we go to 3 shards
                numberOfShards = 3;
            }
            builder.put(getIndexMetadataBuilderForIndex(index, numberOfShards));
        }
        builder.put(dataStream);
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(builder).build();

        final TestTelemetryPlugin telemetryPlugin = new TestTelemetryPlugin();
        ThreadPool testThreadPool = new TestThreadPool(getTestName());
        try {
            MetadataRolloverService rolloverService = DataStreamTestHelper.getMetadataRolloverService(
                dataStream,
                testThreadPool,
                Set.of(),
                xContentRegistry(),
                telemetryPlugin.getTelemetryProvider(Settings.EMPTY)
            );

            // let's rollover the data stream using all the possible autosharding recommendations
            for (AutoShardingType type : AutoShardingType.values()) {
                telemetryPlugin.resetMeter();
                long before = testThreadPool.absoluteTimeInMillis();
                switch (type) {
                    case INCREASE_SHARDS -> {
                        List<Condition<?>> metConditions = List.of(new OptimalShardCountCondition(3));
                        MetadataRolloverService.RolloverResult rolloverResult = rolloverService.rolloverClusterState(
                            clusterState.projectState(projectId),
                            dataStream.getName(),
                            null,
                            new CreateIndexRequest("_na_"),
                            metConditions,
                            Instant.now(),
                            randomBoolean(),
                            false,
                            null,
                            new AutoShardingResult(INCREASE_SHARDS, 3, 5, TimeValue.ZERO),
                            false
                        );
                        assertRolloverResult(dataStream, rolloverResult, before, testThreadPool.absoluteTimeInMillis(), metConditions, 5);
                        assertTelemetry(
                            telemetryPlugin,
                            "es.auto_sharding.increase_shards.total",
                            List.of(
                                "es.auto_sharding.decrease_shards.total",
                                "es.auto_sharding.cooldown_prevented_increase.total",
                                "es.auto_sharding.cooldown_prevented_decrease.total"
                            )
                        );
                    }
                    case DECREASE_SHARDS -> {
                        {
                            // we have another condition that matched, so the rollover will be executed and the new number of shards
                            // will be 1
                            List<Condition<?>> metConditions = List.of(new MaxDocsCondition(2L), new OptimalShardCountCondition(1));
                            MetadataRolloverService.RolloverResult rolloverResult = rolloverService.rolloverClusterState(
                                clusterState.projectState(projectId),
                                dataStream.getName(),
                                null,
                                new CreateIndexRequest("_na_"),
                                metConditions,
                                Instant.now(),
                                randomBoolean(),
                                false,
                                null,
                                new AutoShardingResult(DECREASE_SHARDS, 3, 1, TimeValue.ZERO),
                                false
                            );
                            assertRolloverResult(
                                dataStream,
                                rolloverResult,
                                before,
                                testThreadPool.absoluteTimeInMillis(),
                                metConditions,
                                1
                            );
                            assertTelemetry(
                                telemetryPlugin,
                                "es.auto_sharding.decrease_shards.total",
                                List.of(
                                    "es.auto_sharding.increase_shards.total",
                                    "es.auto_sharding.cooldown_prevented_increase.total",
                                    "es.auto_sharding.cooldown_prevented_decrease.total"
                                )
                            );
                        }

                        {
                            // even if the decrease shards recommendation is not a rollover condition, an empty POST _rollover request will
                            // configure the decrease shards recommendation
                            List<Condition<?>> metConditions = List.of(new OptimalShardCountCondition(1));
                            MetadataRolloverService.RolloverResult rolloverResult = rolloverService.rolloverClusterState(
                                clusterState.projectState(projectId),
                                dataStream.getName(),
                                null,
                                new CreateIndexRequest("_na_"),
                                metConditions,
                                Instant.now(),
                                randomBoolean(),
                                false,
                                null,
                                new AutoShardingResult(DECREASE_SHARDS, 3, 1, TimeValue.ZERO),
                                false
                            );
                            assertRolloverResult(
                                dataStream,
                                rolloverResult,
                                before,
                                testThreadPool.absoluteTimeInMillis(),
                                metConditions,
                                1
                            );
                        }
                    }
                    case COOLDOWN_PREVENTED_INCREASE -> {
                        MetadataRolloverService.RolloverResult rolloverResult = rolloverService.rolloverClusterState(
                            clusterState.projectState(projectId),
                            dataStream.getName(),
                            null,
                            new CreateIndexRequest("_na_"),
                            List.of(),
                            Instant.now(),
                            randomBoolean(),
                            false,
                            null,
                            new AutoShardingResult(COOLDOWN_PREVENTED_INCREASE, 3, 5, TimeValue.timeValueMinutes(10)),
                            false
                        );
                        // the expected number of shards remains 3 for the data stream due to the remaining cooldown
                        assertRolloverResult(dataStream, rolloverResult, before, testThreadPool.absoluteTimeInMillis(), List.of(), 3);
                        assertTelemetry(
                            telemetryPlugin,
                            "es.auto_sharding.cooldown_prevented_increase.total",
                            List.of(
                                "es.auto_sharding.decrease_shards.total",
                                "es.auto_sharding.increase_shards.total",
                                "es.auto_sharding.cooldown_prevented_decrease.total"
                            )
                        );
                    }
                    case COOLDOWN_PREVENTED_DECREASE -> {
                        MetadataRolloverService.RolloverResult rolloverResult = rolloverService.rolloverClusterState(
                            clusterState.projectState(projectId),
                            dataStream.getName(),
                            null,
                            new CreateIndexRequest("_na_"),
                            List.of(),
                            Instant.now(),
                            randomBoolean(),
                            false,
                            null,
                            new AutoShardingResult(COOLDOWN_PREVENTED_DECREASE, 3, 1, TimeValue.timeValueMinutes(10)),
                            false
                        );
                        // the expected number of shards remains 3 for the data stream due to the remaining cooldown
                        assertRolloverResult(dataStream, rolloverResult, before, testThreadPool.absoluteTimeInMillis(), List.of(), 3);
                        assertTelemetry(
                            telemetryPlugin,
                            "es.auto_sharding.cooldown_prevented_decrease.total",
                            List.of(
                                "es.auto_sharding.increase_shards.total",
                                "es.auto_sharding.decrease_shards.total",
                                "es.auto_sharding.cooldown_prevented_increase.total"
                            )
                        );
                    }
                    case NO_CHANGE_REQUIRED -> {
                        List<Condition<?>> metConditions = List.of(new MaxDocsCondition(randomNonNegativeLong()));
                        MetadataRolloverService.RolloverResult rolloverResult = rolloverService.rolloverClusterState(
                            clusterState.projectState(projectId),
                            dataStream.getName(),
                            null,
                            new CreateIndexRequest("_na_"),
                            metConditions,
                            Instant.now(),
                            randomBoolean(),
                            false,
                            null,
                            new AutoShardingResult(NO_CHANGE_REQUIRED, 3, 3, TimeValue.ZERO),
                            false
                        );
                        assertRolloverResult(dataStream, rolloverResult, before, testThreadPool.absoluteTimeInMillis(), metConditions, 3);
                        assertTelemetry(
                            telemetryPlugin,
                            null,
                            List.of(
                                "es.auto_sharding.decrease_shards.total",
                                "es.auto_sharding.increase_shards.total",
                                "es.auto_sharding.cooldown_prevented_increase.total",
                                "es.auto_sharding.cooldown_prevented_decrease.total"
                            )
                        );
                    }
                    case NOT_APPLICABLE -> {
                        List<Condition<?>> metConditions = List.of(new MaxDocsCondition(randomNonNegativeLong()));
                        MetadataRolloverService.RolloverResult rolloverResult = rolloverService.rolloverClusterState(
                            clusterState.projectState(projectId),
                            dataStream.getName(),
                            null,
                            new CreateIndexRequest("_na_"),
                            metConditions,
                            Instant.now(),
                            randomBoolean(),
                            false,
                            null,
                            new AutoShardingResult(NOT_APPLICABLE, 1, 1, TimeValue.MAX_VALUE),
                            false
                        );
                        // if the auto sharding is not applicable we just use whatever's in the index template (1 shard in this case)
                        assertRolloverResult(dataStream, rolloverResult, before, testThreadPool.absoluteTimeInMillis(), metConditions, 1);
                        assertTelemetry(
                            telemetryPlugin,
                            null,
                            List.of(
                                "es.auto_sharding.decrease_shards.total",
                                "es.auto_sharding.increase_shards.total",
                                "es.auto_sharding.cooldown_prevented_increase.total",
                                "es.auto_sharding.cooldown_prevented_decrease.total"
                            )
                        );
                    }
                }
            }
        } finally {
            testThreadPool.shutdown();
        }
    }

    private void assertRolloverResult(
        DataStream preRolloverDataStream,
        MetadataRolloverService.RolloverResult rolloverResult,
        long before,
        long after,
        List<Condition<?>> metConditions,
        int expectedNumberOfShards
    ) {
        String sourceIndexName = DataStream.getDefaultBackingIndexName(
            preRolloverDataStream.getName(),
            preRolloverDataStream.getGeneration()
        );
        String newIndexName = DataStream.getDefaultBackingIndexName(
            preRolloverDataStream.getName(),
            preRolloverDataStream.getGeneration() + 1
        );
        assertEquals(sourceIndexName, rolloverResult.sourceIndexName());
        assertEquals(newIndexName, rolloverResult.rolloverIndexName());
        ProjectMetadata rolloverMetadata = rolloverResult.clusterState().metadata().getProject(projectId);
        assertEquals(preRolloverDataStream.getIndices().size() + 1, rolloverMetadata.indices().size());
        IndexMetadata rolloverIndexMetadata = rolloverMetadata.index(newIndexName);
        // number of shards remained the same
        assertThat(rolloverIndexMetadata.getNumberOfShards(), is(expectedNumberOfShards));

        IndexAbstraction ds = rolloverMetadata.getIndicesLookup().get(preRolloverDataStream.getName());
        assertThat(ds.getType(), equalTo(IndexAbstraction.Type.DATA_STREAM));
        assertThat(ds.getIndices(), hasSize(preRolloverDataStream.getIndices().size() + 1));
        assertThat(ds.getIndices(), hasItem(rolloverMetadata.index(sourceIndexName).getIndex()));
        assertThat(ds.getIndices(), hasItem(rolloverIndexMetadata.getIndex()));
        assertThat(ds.getWriteIndex(), equalTo(rolloverIndexMetadata.getIndex()));

        RolloverInfo info = rolloverMetadata.index(sourceIndexName).getRolloverInfos().get(preRolloverDataStream.getName());
        assertThat(info.getTime(), lessThanOrEqualTo(after));
        assertThat(info.getTime(), greaterThanOrEqualTo(before));
        assertThat(info.getMetConditions(), hasSize(metConditions.size()));
        for (Condition<?> rolloverInfoCondition : info.getMetConditions()) {
            boolean foundMetCondition = false;
            for (Condition<?> metCondition : metConditions) {
                if (metCondition.name.equals(rolloverInfoCondition.name)) {
                    foundMetCondition = true;
                    assertThat(rolloverInfoCondition.value, is(metCondition.value));
                    break;
                }
            }
            assertThat(foundMetCondition, is(true));
        }
    }

    private static IndexMetadata.Builder getIndexMetadataBuilderForIndex(Index index, int numberOfShards) {
        return IndexMetadata.builder(index.getName())
            .settings(ESTestCase.settings(IndexVersion.current()).put("index.hidden", true).put(SETTING_INDEX_UUID, index.getUUID()))
            .numberOfShards(numberOfShards)
            .numberOfReplicas(1);
    }

    private static void assertTelemetry(TestTelemetryPlugin telemetryPlugin, String presentMetric, List<String> missingMetrics) {
        if (presentMetric != null) {
            final List<Measurement> measurements = telemetryPlugin.getLongCounterMeasurement(presentMetric);
            assertThat(measurements, hasSize(1));
            Measurement measurement = measurements.get(0);
            assertThat(measurement.getLong(), is(1L));
            assertFalse(measurement.isDouble());
        }

        for (String metric : missingMetrics) {
            final List<Measurement> measurements = telemetryPlugin.getLongCounterMeasurement(metric);
            assertThat(measurements, empty());
        }
    }
}
