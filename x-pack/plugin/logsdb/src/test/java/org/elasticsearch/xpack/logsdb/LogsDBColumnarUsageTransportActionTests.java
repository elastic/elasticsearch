/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class LogsDBColumnarUsageTransportActionTests extends ESTestCase {

    public void testEmptyProject() {
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).build();
        var counts = LogsDBColumnarUsageTransportAction.computeIndexModeStats(project, clusterSettings(), IndexMode.LOGSDB_COLUMNAR);
        assertThat(counts.enabled(), equalTo(true));
        assertThat(counts.numIndices(), equalTo(0));
        assertThat(counts.numIndicesWithSyntheticSources(), equalTo(0));
        assertThat(counts.dataStreamsCount(), equalTo(0));
        assertThat(counts.dataStreamsManagedByIlm(), equalTo(0));
        assertThat(counts.dataStreamsManagedByDlm(), equalTo(0));
    }

    public void testEnabledFlagResolvedFromClusterSettings() {
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).build();
        assertThat(
            LogsDBColumnarUsageTransportAction.computeIndexModeStats(project, clusterSettings(), IndexMode.LOGSDB_COLUMNAR).enabled(),
            equalTo(true)
        );
        assertThat(
            LogsDBColumnarUsageTransportAction.computeIndexModeStats(
                project,
                clusterSettings(Settings.builder().put(LogsDBPlugin.CLUSTER_COLUMNAR_ENABLED.getKey(), false).build()),
                IndexMode.LOGSDB_COLUMNAR
            ).enabled(),
            equalTo(false)
        );
    }

    private static ClusterSettings clusterSettings() {
        return clusterSettings(Settings.EMPTY);
    }

    private static ClusterSettings clusterSettings(Settings settings) {
        return new ClusterSettings(settings, Set.of(LogsDBPlugin.CLUSTER_COLUMNAR_ENABLED));
    }

    public void testLogsdbColumnarIndexWithDefaultSyntheticSource() {
        // LOGSDB_COLUMNAR defaults to synthetic source — counted in both indices_count and indices_with_synthetic_source
        IndexMetadata index = IndexMetadata.builder("test-index")
            .settings(indexSettings(IndexVersion.current(), 1, 0).put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB_COLUMNAR.getName()))
            .build();
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).put(index, false).build();
        var counts = LogsDBColumnarUsageTransportAction.computeIndexModeStats(project, clusterSettings(), IndexMode.LOGSDB_COLUMNAR);
        assertThat(counts.numIndices(), equalTo(1));
        assertThat(counts.numIndicesWithSyntheticSources(), equalTo(1));
    }

    public void testLogsdbColumnarIndexWithColumnarStoredSource() {
        // COLUMNAR_STORED overrides the default synthetic source — counted in indices_count but not indices_with_synthetic_source
        IndexMetadata index = IndexMetadata.builder("test-index")
            .settings(
                indexSettings(IndexVersion.current(), 1, 0).put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB_COLUMNAR.getName())
                    .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.COLUMNAR_STORED.name())
            )
            .build();
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).put(index, false).build();
        var counts = LogsDBColumnarUsageTransportAction.computeIndexModeStats(project, clusterSettings(), IndexMode.LOGSDB_COLUMNAR);
        assertThat(counts.numIndices(), equalTo(1));
        assertThat(counts.numIndicesWithSyntheticSources(), equalTo(0));
    }

    public void testStandardIndexNotCounted() {
        IndexMetadata standardIndex = IndexMetadata.builder("standard-index").settings(indexSettings(IndexVersion.current(), 1, 0)).build();
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).put(standardIndex, false).build();
        var counts = LogsDBColumnarUsageTransportAction.computeIndexModeStats(project, clusterSettings(), IndexMode.LOGSDB_COLUMNAR);
        assertThat(counts.numIndices(), equalTo(0));
        assertThat(counts.numIndicesWithSyntheticSources(), equalTo(0));
    }

    public void testLogsdbIndexNotCounted() {
        IndexMetadata logsdbIndex = IndexMetadata.builder("logsdb-index")
            .settings(indexSettings(IndexVersion.current(), 1, 0).put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB.getName()))
            .build();
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).put(logsdbIndex, false).build();
        var counts = LogsDBColumnarUsageTransportAction.computeIndexModeStats(project, clusterSettings(), IndexMode.LOGSDB_COLUMNAR);
        assertThat(counts.numIndices(), equalTo(0));
        assertThat(counts.numIndicesWithSyntheticSources(), equalTo(0));
    }

    public void testMultipleIndicesMixed() {
        IndexMetadata columnarIndex1 = IndexMetadata.builder("columnar-1")
            .settings(indexSettings(IndexVersion.current(), 1, 0).put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB_COLUMNAR.getName()))
            .build();
        IndexMetadata columnarIndex2 = IndexMetadata.builder("columnar-2")
            .settings(
                indexSettings(IndexVersion.current(), 1, 0).put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB_COLUMNAR.getName())
                    .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.COLUMNAR_STORED.name())
            )
            .build();
        IndexMetadata logsdbIndex = IndexMetadata.builder("logsdb-index")
            .settings(indexSettings(IndexVersion.current(), 1, 0).put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB.getName()))
            .build();
        IndexMetadata standardIndex = IndexMetadata.builder("standard-index").settings(indexSettings(IndexVersion.current(), 1, 0)).build();
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(columnarIndex1, false)
            .put(columnarIndex2, false)
            .put(logsdbIndex, false)
            .put(standardIndex, false)
            .build();
        var counts = LogsDBColumnarUsageTransportAction.computeIndexModeStats(project, clusterSettings(), IndexMode.LOGSDB_COLUMNAR);
        assertThat(counts.numIndices(), equalTo(2));
        assertThat(counts.numIndicesWithSyntheticSources(), equalTo(1));
    }

    public void testDlmManagedDataStream() {
        String streamName = "columnar-dlm-stream";
        IndexMetadata writeIndex = buildColumnarWriteIndex(streamName, 1);
        DataStream dataStream = DataStream.builder(streamName, List.of(writeIndex.getIndex()))
            .setLifecycle(DataStreamLifecycle.dataLifecycleBuilder().dataRetention(randomPositiveTimeValue()).build())
            .build();
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).put(writeIndex, false).put(dataStream).build();
        var counts = LogsDBColumnarUsageTransportAction.computeIndexModeStats(project, clusterSettings(), IndexMode.LOGSDB_COLUMNAR);
        assertThat(counts.dataStreamsCount(), equalTo(1));
        assertThat(counts.dataStreamsManagedByDlm(), equalTo(1));
        assertThat(counts.dataStreamsManagedByIlm(), equalTo(0));
    }

    public void testIlmManagedDataStream() {
        String streamName = "columnar-ilm-stream";
        IndexMetadata writeIndex = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(streamName, 1))
            .settings(
                indexSettings(IndexVersion.current(), 1, 0).put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB_COLUMNAR.getName())
                    .put(IndexMetadata.LIFECYCLE_NAME, "my-ilm-policy")
            )
            .build();
        DataStream dataStream = DataStream.builder(streamName, List.of(writeIndex.getIndex())).build();
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).put(writeIndex, false).put(dataStream).build();
        var counts = LogsDBColumnarUsageTransportAction.computeIndexModeStats(project, clusterSettings(), IndexMode.LOGSDB_COLUMNAR);
        assertThat(counts.dataStreamsCount(), equalTo(1));
        assertThat(counts.dataStreamsManagedByIlm(), equalTo(1));
        assertThat(counts.dataStreamsManagedByDlm(), equalTo(0));
    }

    public void testUnmanagedDataStream() {
        String streamName = "columnar-unmanaged-stream";
        IndexMetadata writeIndex = buildColumnarWriteIndex(streamName, 1);
        DataStream dataStream = DataStream.builder(streamName, List.of(writeIndex.getIndex())).build();
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).put(writeIndex, false).put(dataStream).build();
        var counts = LogsDBColumnarUsageTransportAction.computeIndexModeStats(project, clusterSettings(), IndexMode.LOGSDB_COLUMNAR);
        assertThat(counts.dataStreamsCount(), equalTo(1));
        assertThat(counts.dataStreamsManagedByIlm(), equalTo(0));
        assertThat(counts.dataStreamsManagedByDlm(), equalTo(0));
    }

    public void testNonColumnarDataStreamNotCounted() {
        String streamName = "logsdb-stream";
        IndexMetadata writeIndex = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(streamName, 1))
            .settings(indexSettings(IndexVersion.current(), 1, 0).put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB.getName()))
            .build();
        DataStream dataStream = DataStream.builder(streamName, List.of(writeIndex.getIndex())).build();
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).put(writeIndex, false).put(dataStream).build();
        var counts = LogsDBColumnarUsageTransportAction.computeIndexModeStats(project, clusterSettings(), IndexMode.LOGSDB_COLUMNAR);
        assertThat(counts.dataStreamsCount(), equalTo(0));
        assertThat(counts.dataStreamsManagedByIlm(), equalTo(0));
        assertThat(counts.dataStreamsManagedByDlm(), equalTo(0));
    }

    public void testMixedDataStreams() {
        String dlmStream = "columnar-dlm";
        IndexMetadata dlmWriteIndex = buildColumnarWriteIndex(dlmStream, 1);

        String ilmStream = "columnar-ilm";
        IndexMetadata ilmWriteIndex = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(ilmStream, 1))
            .settings(
                indexSettings(IndexVersion.current(), 1, 0).put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB_COLUMNAR.getName())
                    .put(IndexMetadata.LIFECYCLE_NAME, "my-ilm-policy")
            )
            .build();

        String unmanagedStream = "columnar-unmanaged";
        IndexMetadata unmanagedWriteIndex = buildColumnarWriteIndex(unmanagedStream, 1);

        String logsdbStream = "logsdb-stream";
        IndexMetadata logsdbWriteIndex = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(logsdbStream, 1))
            .settings(indexSettings(IndexVersion.current(), 1, 0).put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB.getName()))
            .build();

        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(dlmWriteIndex, false)
            .put(
                DataStream.builder(dlmStream, List.of(dlmWriteIndex.getIndex()))
                    .setLifecycle(DataStreamLifecycle.dataLifecycleBuilder().build())
                    .build()
            )
            .put(ilmWriteIndex, false)
            .put(DataStream.builder(ilmStream, List.of(ilmWriteIndex.getIndex())).build())
            .put(unmanagedWriteIndex, false)
            .put(DataStream.builder(unmanagedStream, List.of(unmanagedWriteIndex.getIndex())).build())
            .put(logsdbWriteIndex, false)
            .put(DataStream.builder(logsdbStream, List.of(logsdbWriteIndex.getIndex())).build())
            .build();

        var counts = LogsDBColumnarUsageTransportAction.computeIndexModeStats(project, clusterSettings(), IndexMode.LOGSDB_COLUMNAR);
        assertThat(counts.dataStreamsCount(), equalTo(3));
        assertThat(counts.dataStreamsManagedByDlm(), equalTo(1));
        assertThat(counts.dataStreamsManagedByIlm(), equalTo(1));
        // standalone indices from the 3 columnar data stream backing indices
        assertThat(counts.numIndices(), equalTo(3));
    }

    private static IndexMetadata buildColumnarWriteIndex(String dataStreamName, int generation) {
        return IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, generation))
            .settings(indexSettings(IndexVersion.current(), 1, 0).put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB_COLUMNAR.getName()))
            .build();
    }
}
