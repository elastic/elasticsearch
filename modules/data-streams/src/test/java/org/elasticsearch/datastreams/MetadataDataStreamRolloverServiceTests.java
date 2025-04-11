/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.rollover.Condition;
import org.elasticsearch.action.admin.indices.rollover.MaxDocsCondition;
import org.elasticsearch.action.admin.indices.rollover.MetadataRolloverService;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadataStats;
import org.elasticsearch.cluster.metadata.IndexWriteLoad;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.MapperTestUtils;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID;
import static org.elasticsearch.datastreams.DataStreamIndexSettingsProvider.FORMATTER;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class MetadataDataStreamRolloverServiceTests extends ESTestCase {

    public void testRolloverClusterStateForDataStream() throws Exception {
        Instant now = Instant.now();
        String dataStreamName = "logs-my-app";
        final DataStream dataStream = DataStream.builder(
            dataStreamName,
            List.of(new Index(DataStream.getDefaultBackingIndexName(dataStreamName, 1, now.toEpochMilli()), "uuid"))
        ).setIndexMode(IndexMode.TIME_SERIES).build();
        ComposableIndexTemplate template = ComposableIndexTemplate.builder()
            .indexPatterns(List.of(dataStream.getName() + "*"))
            .template(
                new Template(Settings.builder().put("index.mode", "time_series").put("index.routing_path", "uid").build(), null, null)
            )
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
            .build();
        final var projectId = randomProjectIdOrDefault();
        ProjectMetadata.Builder builder = ProjectMetadata.builder(projectId);
        builder.put("template", template);
        builder.put(
            IndexMetadata.builder(dataStream.getWriteIndex().getName())
                .settings(
                    settings(IndexVersion.current()).put("index.hidden", true)
                        .put(SETTING_INDEX_UUID, dataStream.getWriteIndex().getUUID())
                        .put("index.mode", "time_series")
                        .put("index.routing_path", "uid")
                        .put("index.time_series.start_time", FORMATTER.format(now.minus(4, ChronoUnit.HOURS)))
                        .put("index.time_series.end_time", FORMATTER.format(now.minus(2, ChronoUnit.HOURS)))
                )
                .numberOfShards(1)
                .numberOfReplicas(0)
        );
        builder.put(dataStream);
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(builder).build();
        final TestTelemetryPlugin telemetryPlugin = new TestTelemetryPlugin();

        ThreadPool testThreadPool = new TestThreadPool(getTestName());
        try {
            MetadataRolloverService rolloverService = DataStreamTestHelper.getMetadataRolloverService(
                dataStream,
                testThreadPool,
                Set.of(createSettingsProvider(xContentRegistry())),
                xContentRegistry(),
                telemetryPlugin.getTelemetryProvider(Settings.EMPTY)
            );
            MaxDocsCondition condition = new MaxDocsCondition(randomNonNegativeLong());
            List<Condition<?>> metConditions = Collections.singletonList(condition);
            CreateIndexRequest createIndexRequest = new CreateIndexRequest("_na_");
            IndexMetadataStats indexStats = new IndexMetadataStats(IndexWriteLoad.builder(1).build(), 10, 10);

            long before = testThreadPool.absoluteTimeInMillis();
            MetadataRolloverService.RolloverResult rolloverResult = rolloverService.rolloverClusterState(
                clusterState.projectState(projectId),
                dataStream.getName(),
                null,
                createIndexRequest,
                metConditions,
                now,
                randomBoolean(),
                false,
                indexStats,
                null,
                false
            );
            long after = testThreadPool.absoluteTimeInMillis();

            String sourceIndexName = DataStream.getDefaultBackingIndexName(dataStream.getName(), dataStream.getGeneration());
            String newIndexName = DataStream.getDefaultBackingIndexName(dataStream.getName(), dataStream.getGeneration() + 1);
            assertEquals(sourceIndexName, rolloverResult.sourceIndexName());
            assertEquals(newIndexName, rolloverResult.rolloverIndexName());
            ProjectMetadata rolloverMetadata = rolloverResult.clusterState().metadata().getProject(projectId);
            assertEquals(dataStream.getIndices().size() + 1, rolloverMetadata.indices().size());
            IndexMetadata rolloverIndexMetadata = rolloverMetadata.index(newIndexName);

            IndexAbstraction ds = rolloverMetadata.getIndicesLookup().get(dataStream.getName());
            assertThat(ds.getType(), equalTo(IndexAbstraction.Type.DATA_STREAM));
            assertThat(ds.getIndices(), hasSize(dataStream.getIndices().size() + 1));
            assertThat(ds.getIndices(), hasItem(rolloverMetadata.index(sourceIndexName).getIndex()));
            assertThat(ds.getIndices(), hasItem(rolloverIndexMetadata.getIndex()));
            assertThat(ds.getWriteIndex(), equalTo(rolloverIndexMetadata.getIndex()));

            RolloverInfo info = rolloverMetadata.index(sourceIndexName).getRolloverInfos().get(dataStream.getName());
            assertThat(info.getTime(), lessThanOrEqualTo(after));
            assertThat(info.getTime(), greaterThanOrEqualTo(before));
            assertThat(info.getMetConditions(), hasSize(1));
            assertThat(info.getMetConditions().get(0).value(), equalTo(condition.value()));

            IndexMetadata im = rolloverMetadata.index(rolloverMetadata.dataStreams().get(dataStreamName).getIndices().get(0));
            Instant startTime1 = IndexSettings.TIME_SERIES_START_TIME.get(im.getSettings());
            Instant endTime1 = IndexSettings.TIME_SERIES_END_TIME.get(im.getSettings());
            IndexMetadataStats indexStats1 = im.getStats();
            im = rolloverMetadata.index(rolloverMetadata.dataStreams().get(dataStreamName).getIndices().get(1));
            Instant startTime2 = IndexSettings.TIME_SERIES_START_TIME.get(im.getSettings());
            Instant endTime2 = IndexSettings.TIME_SERIES_END_TIME.get(im.getSettings());
            IndexMetadataStats indexStats2 = im.getStats();
            assertThat(startTime1.isBefore(endTime1), is(true));
            assertThat(endTime1, equalTo(startTime2));
            assertThat(endTime2.isAfter(endTime1), is(true));
            assertThat(indexStats1, is(equalTo(indexStats)));
            assertThat(indexStats2, is(nullValue()));
        } finally {
            testThreadPool.shutdown();
        }
    }

    public void testRolloverAndMigrateDataStream() throws Exception {
        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        String dataStreamName = "logs-my-app";
        IndexMode dsIndexMode = randomBoolean() ? null : IndexMode.STANDARD;
        final DataStream dataStream = DataStream.builder(
            dataStreamName,
            List.of(new Index(DataStream.getDefaultBackingIndexName(dataStreamName, 1, now.toEpochMilli()), "uuid"))
        ).setIndexMode(dsIndexMode).build();
        ComposableIndexTemplate template = ComposableIndexTemplate.builder()
            .indexPatterns(List.of(dataStream.getName() + "*"))
            .template(
                new Template(Settings.builder().put("index.mode", "time_series").put("index.routing_path", "uid").build(), null, null)
            )
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
            .build();
        final var projectId = randomProjectIdOrDefault();
        ProjectMetadata.Builder builder = ProjectMetadata.builder(projectId);
        builder.put("template", template);
        Settings.Builder indexSettings = settings(IndexVersion.current()).put("index.hidden", true)
            .put(SETTING_INDEX_UUID, dataStream.getWriteIndex().getUUID());
        if (dsIndexMode != null) {
            indexSettings.put("index.mode", dsIndexMode.getName());
        }
        builder.put(
            IndexMetadata.builder(dataStream.getWriteIndex().getName()).settings(indexSettings).numberOfShards(1).numberOfReplicas(0)
        );
        builder.put(dataStream);
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(builder).build();
        final TestTelemetryPlugin telemetryPlugin = new TestTelemetryPlugin();

        ThreadPool testThreadPool = new TestThreadPool(getTestName());
        try {
            MetadataRolloverService rolloverService = DataStreamTestHelper.getMetadataRolloverService(
                dataStream,
                testThreadPool,
                Set.of(createSettingsProvider(xContentRegistry())),
                xContentRegistry(),
                telemetryPlugin.getTelemetryProvider(Settings.EMPTY)
            );
            MaxDocsCondition condition = new MaxDocsCondition(randomNonNegativeLong());
            List<Condition<?>> metConditions = Collections.singletonList(condition);
            CreateIndexRequest createIndexRequest = new CreateIndexRequest("_na_");

            MetadataRolloverService.RolloverResult rolloverResult = rolloverService.rolloverClusterState(
                clusterState.projectState(projectId),
                dataStream.getName(),
                null,
                createIndexRequest,
                metConditions,
                now,
                randomBoolean(),
                false,
                null,
                null,
                false
            );

            String sourceIndexName = DataStream.getDefaultBackingIndexName(dataStream.getName(), dataStream.getGeneration());
            String newIndexName = DataStream.getDefaultBackingIndexName(dataStream.getName(), dataStream.getGeneration() + 1);
            assertEquals(sourceIndexName, rolloverResult.sourceIndexName());
            assertEquals(newIndexName, rolloverResult.rolloverIndexName());
            ProjectMetadata rolloverMetadata = rolloverResult.clusterState().metadata().getProject(projectId);
            assertEquals(dataStream.getIndices().size() + 1, rolloverMetadata.indices().size());

            // Assert data stream's index_mode has been changed to time_series.
            assertThat(rolloverMetadata.dataStreams().get(dataStreamName), notNullValue());
            assertThat(rolloverMetadata.dataStreams().get(dataStreamName).getIndexMode(), equalTo(IndexMode.TIME_SERIES));

            // Nothing changed for the original backing index:
            IndexMetadata im = rolloverMetadata.index(rolloverMetadata.dataStreams().get(dataStreamName).getIndices().get(0));
            assertThat(IndexSettings.MODE.get(im.getSettings()), equalTo(IndexMode.STANDARD));
            assertThat(IndexSettings.TIME_SERIES_START_TIME.exists(im.getSettings()), is(false));
            assertThat(IndexSettings.TIME_SERIES_END_TIME.exists(im.getSettings()), is(false));
            // New backing index is a tsdb index:
            im = rolloverMetadata.index(rolloverMetadata.dataStreams().get(dataStreamName).getIndices().get(1));
            assertThat(IndexSettings.MODE.get(im.getSettings()), equalTo(IndexMode.TIME_SERIES));
            Instant startTime = IndexSettings.TIME_SERIES_START_TIME.get(im.getSettings());
            Instant endTime = IndexSettings.TIME_SERIES_END_TIME.get(im.getSettings());
            assertThat(startTime.isBefore(endTime), is(true));
            assertThat(startTime, equalTo(now.minus(2, ChronoUnit.HOURS)));
            assertThat(endTime, equalTo(now.plus(30, ChronoUnit.MINUTES)));
        } finally {
            testThreadPool.shutdown();
        }
    }

    public void testChangingIndexModeFromTimeSeriesToSomethingElseNoEffectOnExistingDataStreams() throws Exception {
        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        String dataStreamName = "logs-my-app";
        final DataStream dataStream = DataStream.builder(
            dataStreamName,
            List.of(new Index(DataStream.getDefaultBackingIndexName(dataStreamName, 1, now.toEpochMilli()), "uuid"))
        ).setIndexMode(IndexMode.TIME_SERIES).build();
        ComposableIndexTemplate template = ComposableIndexTemplate.builder()
            .indexPatterns(List.of(dataStream.getName() + "*"))
            .template(
                new Template(Settings.builder().put("index.mode", "time_series").put("index.routing_path", "uid").build(), null, null)
            )
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
            .build();
        final var projectId = randomProjectIdOrDefault();
        ProjectMetadata.Builder builder = ProjectMetadata.builder(projectId);
        builder.put("template", template);
        builder.put(
            IndexMetadata.builder(dataStream.getWriteIndex().getName())
                .settings(
                    settings(IndexVersion.current()).put("index.hidden", true)
                        .put(SETTING_INDEX_UUID, dataStream.getWriteIndex().getUUID())
                        .put("index.mode", "time_series")
                        .put("index.routing_path", "uid")
                        .put("index.time_series.start_time", FORMATTER.format(now.minus(4, ChronoUnit.HOURS)))
                        .put("index.time_series.end_time", FORMATTER.format(now.minus(2, ChronoUnit.HOURS)))
                )
                .numberOfShards(1)
                .numberOfReplicas(0)
        );
        builder.put(dataStream);
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(builder).build();
        final TestTelemetryPlugin telemetryPlugin = new TestTelemetryPlugin();
        ThreadPool testThreadPool = new TestThreadPool(getTestName());
        try {
            MetadataRolloverService rolloverService = DataStreamTestHelper.getMetadataRolloverService(
                dataStream,
                testThreadPool,
                Set.of(createSettingsProvider(xContentRegistry())),
                xContentRegistry(),
                telemetryPlugin.getTelemetryProvider(Settings.EMPTY)
            );
            MaxDocsCondition condition = new MaxDocsCondition(randomNonNegativeLong());
            List<Condition<?>> metConditions = Collections.singletonList(condition);
            CreateIndexRequest createIndexRequest = new CreateIndexRequest("_na_");

            MetadataRolloverService.RolloverResult rolloverResult = rolloverService.rolloverClusterState(
                clusterState.projectState(projectId),
                dataStream.getName(),
                null,
                createIndexRequest,
                metConditions,
                now,
                randomBoolean(),
                false,
                null,
                null,
                false
            );

            String sourceIndexName = DataStream.getDefaultBackingIndexName(dataStream.getName(), dataStream.getGeneration());
            String newIndexName = DataStream.getDefaultBackingIndexName(dataStream.getName(), dataStream.getGeneration() + 1);
            assertEquals(sourceIndexName, rolloverResult.sourceIndexName());
            assertEquals(newIndexName, rolloverResult.rolloverIndexName());
            ProjectMetadata rolloverMetadata = rolloverResult.clusterState().metadata().getProject(projectId);
            assertEquals(dataStream.getIndices().size() + 1, rolloverMetadata.indices().size());

            // Assert data stream's index_mode remains time_series.
            assertThat(rolloverMetadata.dataStreams().get(dataStreamName), notNullValue());
            assertThat(rolloverMetadata.dataStreams().get(dataStreamName).getIndexMode(), equalTo(IndexMode.TIME_SERIES));

            // Nothing changed for the original tsdb backing index:
            IndexMetadata im = rolloverMetadata.index(rolloverMetadata.dataStreams().get(dataStreamName).getIndices().get(0));
            assertThat(IndexSettings.MODE.exists(im.getSettings()), is(true));
            Instant startTime = IndexSettings.TIME_SERIES_START_TIME.get(im.getSettings());
            Instant endTime = IndexSettings.TIME_SERIES_END_TIME.get(im.getSettings());
            assertThat(startTime.isBefore(endTime), is(true));
            assertThat(startTime, equalTo(now.minus(4, ChronoUnit.HOURS)));
            assertThat(endTime, equalTo(now.minus(2, ChronoUnit.HOURS)));
            // New backing index is also a tsdb index:
            im = rolloverMetadata.index(rolloverMetadata.dataStreams().get(dataStreamName).getIndices().get(1));
            assertThat(IndexSettings.MODE.get(im.getSettings()), equalTo(IndexMode.TIME_SERIES));
            startTime = IndexSettings.TIME_SERIES_START_TIME.get(im.getSettings());
            endTime = IndexSettings.TIME_SERIES_END_TIME.get(im.getSettings());
            assertThat(startTime.isBefore(endTime), is(true));
            assertThat(startTime, equalTo(now.minus(2, ChronoUnit.HOURS)));
            assertThat(endTime, equalTo(now.plus(30, ChronoUnit.MINUTES)));
        } finally {
            testThreadPool.shutdown();
        }
    }

    public void testRolloverClusterStateWithBrokenOlderTsdbDataStream() throws Exception {
        Instant now = Instant.now();
        String dataStreamName = "metrics-my-app";
        int numberOfBackingIndices = randomIntBetween(1, 3);
        final var projectId = randomProjectIdOrDefault();
        ClusterState clusterState = createClusterState(projectId, dataStreamName, numberOfBackingIndices, now, true);
        DataStream dataStream = clusterState.metadata().getProject(projectId).dataStreams().get(dataStreamName);
        ThreadPool testThreadPool = new TestThreadPool(getTestName());
        final TestTelemetryPlugin telemetryPlugin = new TestTelemetryPlugin();

        try {
            MetadataRolloverService rolloverService = DataStreamTestHelper.getMetadataRolloverService(
                dataStream,
                testThreadPool,
                Set.of(createSettingsProvider(xContentRegistry())),
                xContentRegistry(),
                telemetryPlugin.getTelemetryProvider(Settings.EMPTY)
            );
            MaxDocsCondition condition = new MaxDocsCondition(randomNonNegativeLong());
            List<Condition<?>> metConditions = Collections.singletonList(condition);
            CreateIndexRequest createIndexRequest = new CreateIndexRequest("_na_");
            IndexMetadataStats indexStats = new IndexMetadataStats(IndexWriteLoad.builder(1).build(), 10, 10);

            long before = testThreadPool.absoluteTimeInMillis();
            MetadataRolloverService.RolloverResult rolloverResult = rolloverService.rolloverClusterState(
                clusterState.projectState(projectId),
                dataStream.getName(),
                null,
                createIndexRequest,
                metConditions,
                now,
                randomBoolean(),
                false,
                indexStats,
                null,
                false
            );
            long after = testThreadPool.absoluteTimeInMillis();

            String sourceIndexName = DataStream.getDefaultBackingIndexName(dataStream.getName(), dataStream.getGeneration());
            String newIndexName = DataStream.getDefaultBackingIndexName(dataStream.getName(), dataStream.getGeneration() + 1);
            assertEquals(sourceIndexName, rolloverResult.sourceIndexName());
            assertEquals(newIndexName, rolloverResult.rolloverIndexName());
            ProjectMetadata rolloverMetadata = rolloverResult.clusterState().metadata().getProject(projectId);
            assertEquals(dataStream.getIndices().size() + 1, rolloverMetadata.indices().size());
            IndexMetadata rolloverIndexMetadata = rolloverMetadata.index(newIndexName);

            IndexAbstraction ds = rolloverMetadata.getIndicesLookup().get(dataStream.getName());
            assertThat(ds.getType(), equalTo(IndexAbstraction.Type.DATA_STREAM));
            assertThat(ds.getIndices(), hasSize(dataStream.getIndices().size() + 1));
            assertThat(ds.getIndices(), hasItem(rolloverMetadata.index(sourceIndexName).getIndex()));
            assertThat(ds.getIndices(), hasItem(rolloverIndexMetadata.getIndex()));
            assertThat(ds.getWriteIndex(), equalTo(rolloverIndexMetadata.getIndex()));
            assertThat(((DataStream) ds).getIndexMode(), equalTo(IndexMode.TIME_SERIES));

            RolloverInfo info = rolloverMetadata.index(sourceIndexName).getRolloverInfos().get(dataStream.getName());
            assertThat(info.getTime(), lessThanOrEqualTo(after));
            assertThat(info.getTime(), greaterThanOrEqualTo(before));
            assertThat(info.getMetConditions(), hasSize(1));
            assertThat(info.getMetConditions().get(0).value(), equalTo(condition.value()));

            for (int i = 0; i < numberOfBackingIndices; i++) {
                var im = rolloverMetadata.index(rolloverMetadata.dataStreams().get(dataStreamName).getIndices().get(i));
                assertThat(im.getTimeSeriesStart(), nullValue());
                assertThat(im.getTimeSeriesEnd(), nullValue());
                assertThat(im.getIndexMode(), nullValue());
            }
            {
                var im = rolloverMetadata.index(
                    rolloverMetadata.dataStreams().get(dataStreamName).getIndices().get(numberOfBackingIndices)
                );
                var lastStartTime = IndexSettings.TIME_SERIES_START_TIME.get(im.getSettings());
                var kastEndTime = IndexSettings.TIME_SERIES_END_TIME.get(im.getSettings());
                assertThat(lastStartTime, equalTo(now.minus(2, ChronoUnit.HOURS).truncatedTo(ChronoUnit.SECONDS)));
                assertThat(kastEndTime, equalTo(now.plus(30, ChronoUnit.MINUTES).truncatedTo(ChronoUnit.SECONDS)));
                assertThat(im.getIndexMode(), equalTo(IndexMode.TIME_SERIES));
            }
        } finally {
            testThreadPool.shutdown();
        }
    }

    public void testRolloverClusterStateWithBrokenTsdbDataStream() throws Exception {
        Instant now = Instant.now();
        String dataStreamName = "metrics-my-app";
        int numberOfBackingIndices = randomIntBetween(1, 3);
        final var projectId = randomProjectIdOrDefault();
        ClusterState clusterState = createClusterState(projectId, dataStreamName, numberOfBackingIndices, now, false);
        DataStream dataStream = clusterState.metadata().getProject(projectId).dataStreams().get(dataStreamName);
        final TestTelemetryPlugin telemetryPlugin = new TestTelemetryPlugin();
        ThreadPool testThreadPool = new TestThreadPool(getTestName());
        try {
            MetadataRolloverService rolloverService = DataStreamTestHelper.getMetadataRolloverService(
                dataStream,
                testThreadPool,
                Set.of(createSettingsProvider(xContentRegistry())),
                xContentRegistry(),
                telemetryPlugin.getTelemetryProvider(Settings.EMPTY)
            );
            MaxDocsCondition condition = new MaxDocsCondition(randomNonNegativeLong());
            List<Condition<?>> metConditions = Collections.singletonList(condition);
            CreateIndexRequest createIndexRequest = new CreateIndexRequest("_na_");
            IndexMetadataStats indexStats = new IndexMetadataStats(IndexWriteLoad.builder(1).build(), 10, 10);

            Exception e = expectThrows(
                IllegalArgumentException.class,
                () -> rolloverService.rolloverClusterState(
                    clusterState.projectState(projectId),
                    dataStream.getName(),
                    null,
                    createIndexRequest,
                    metConditions,
                    now,
                    randomBoolean(),
                    false,
                    indexStats,
                    null,
                    false
                )
            );
            assertThat(e.getMessage(), containsString("is overlapping with backing index"));
        } finally {
            testThreadPool.shutdown();
        }
    }

    private static ClusterState createClusterState(
        ProjectId projectId,
        String dataStreamName,
        int numberOfBackingIndices,
        Instant now,
        boolean includeVersion
    ) {
        List<Index> backingIndices = new ArrayList<>(numberOfBackingIndices);
        for (int i = 1; i <= numberOfBackingIndices; i++) {
            backingIndices.add(new Index(DataStream.getDefaultBackingIndexName(dataStreamName, i, now.toEpochMilli()), "uuid" + i));
        }
        final DataStream dataStream = DataStream.builder(dataStreamName, backingIndices).setGeneration(numberOfBackingIndices).build();
        ComposableIndexTemplate template = ComposableIndexTemplate.builder()
            .indexPatterns(List.of(dataStream.getName() + "*"))
            .template(
                new Template(Settings.builder().put("index.mode", "time_series").put("index.routing_path", "uid").build(), null, null)
            )
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
            .build();
        ProjectMetadata.Builder builder = ProjectMetadata.builder(projectId);
        builder.put("template", template);

        for (Index backingIndex : backingIndices) {
            var settings = settings(IndexVersion.current()).put("index.hidden", true)
                .put(SETTING_INDEX_UUID, backingIndex.getUUID())
                .put("index.mode", "time_series")
                .put("index.routing_path", "uid");
            if (includeVersion) {
                settings.put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersions.V_8_9_0);
            }
            builder.put(IndexMetadata.builder(backingIndex.getName()).settings(settings).numberOfShards(1).numberOfReplicas(0));
        }
        builder.put(dataStream);
        return ClusterState.builder(new ClusterName("test")).putProjectMetadata(builder).build();
    }

    static DataStreamIndexSettingsProvider createSettingsProvider(NamedXContentRegistry xContentRegistry) {
        return new DataStreamIndexSettingsProvider(
            im -> MapperTestUtils.newMapperService(xContentRegistry, createTempDir(), im.getSettings(), im.getIndex().getName())
        );
    }

}
