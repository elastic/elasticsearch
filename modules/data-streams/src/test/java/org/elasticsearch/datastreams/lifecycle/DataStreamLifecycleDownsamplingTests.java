/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle;

import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockRequest;
import org.elasticsearch.action.datastreams.lifecycle.ErrorEntry;
import org.elasticsearch.action.downsample.DownsampleAction;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle.DownsamplingRound;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MetadataIndexStateService;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock.WRITE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.DownsampleTaskStatus.STARTED;
import static org.elasticsearch.cluster.metadata.IndexMetadata.DownsampleTaskStatus.SUCCESS;
import static org.elasticsearch.datastreams.DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleFixtures.createDataStream;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService.DEFAULT_MAX_DOWNSAMPLING_INDICES_IN_PROGRESS_PER_DATA_STREAM;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService.DOWNSAMPLED_INDEX_PREFIX;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService.FORCE_MERGE_COMPLETED_TIMESTAMP_METADATA_KEY;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService.ONE_HUNDRED_MB;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService.TARGET_MERGE_FACTOR_VALUE;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Testing the downsampling part of the data stream lifecycle, more specifically code relating to
 * {@link DataStreamLifecycleService#maybeExecuteDownsampling(ProjectState, DataStream, List, int)}.
 */
public class DataStreamLifecycleDownsamplingTests extends DataStreamLifecycleServiceTestCase {
    private String dataStreamName;
    private ProjectId projectId;
    private ProjectMetadata.Builder projectBuilder;

    @Before
    public void setup() {
        dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        projectId = randomProjectIdOrDefault();
        projectBuilder = ProjectMetadata.builder(projectId);
    }

    public void testDownsampling() throws Exception {
        int numBackingIndices = 2;
        DataStream dataStream = createDataStream(
            projectBuilder,
            dataStreamName,
            numBackingIndices,
            settings(IndexVersion.current()).put(MergePolicyConfig.INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING.getKey(), ONE_HUNDRED_MB)
                .put(MergePolicyConfig.INDEX_MERGE_POLICY_MERGE_FACTOR_SETTING.getKey(), TARGET_MERGE_FACTOR_VALUE)
                .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                .put("index.routing_path", "@timestamp"),
            DataStreamLifecycle.dataLifecycleBuilder()
                .downsamplingRounds(List.of(new DownsamplingRound(TimeValue.timeValueMillis(0), new DateHistogramInterval("5m"))))
                .dataRetention(TimeValue.MAX_VALUE)
                .build(),
            now
        );
        projectBuilder.put(dataStream);

        String nodeId = "localNode";
        DiscoveryNodes.Builder nodesBuilder = buildNodes(nodeId);
        // we are the master node
        nodesBuilder.masterNodeId(nodeId);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(projectBuilder).nodes(nodesBuilder).build();
        setState(clusterService, state);
        Index firstGenIndex = dataStream.getIndices().getFirst();
        String firstGenIndexName = firstGenIndex.getName();
        Set<Index> affectedIndices = dataStreamLifecycleService.maybeExecuteDownsampling(
            clusterService.state().projectState(projectId),
            dataStream,
            List.of(firstGenIndex),
            0
        );

        assertThat(affectedIndices, is(Set.of(firstGenIndex)));
        // we first mark the index as read-only
        assertThat(clientSeenRequests.size(), is(1));
        assertThat(clientSeenRequests.get(0), instanceOf(AddIndexBlockRequest.class));

        {
            // we do the read-only bit ourselves as it's unit-testing
            ProjectMetadata.Builder newProjectBuilder = ProjectMetadata.builder(state.metadata().getProject(projectId));
            IndexMetadata indexMetadata = newProjectBuilder.getSafe(firstGenIndex);
            Settings updatedSettings = Settings.builder().put(indexMetadata.getSettings()).put(WRITE.settingName(), true).build();
            newProjectBuilder.put(
                IndexMetadata.builder(indexMetadata).settings(updatedSettings).settingsVersion(indexMetadata.getSettingsVersion() + 1)
            );

            ClusterBlock indexBlock = MetadataIndexStateService.createUUIDBasedBlock(WRITE.getBlock());
            ClusterBlocks.Builder blocks = ClusterBlocks.builder(state.blocks());
            blocks.addIndexBlock(projectId, firstGenIndexName, indexBlock);

            state = ClusterState.builder(state).blocks(blocks).putProjectMetadata(newProjectBuilder).build();
            setState(clusterService, state);
        }

        // on the next run downsampling should be triggered
        affectedIndices = dataStreamLifecycleService.maybeExecuteDownsampling(
            clusterService.state().projectState(projectId),
            dataStream,
            List.of(firstGenIndex),
            0
        );
        assertThat(affectedIndices, is(Set.of(firstGenIndex)));
        assertThat(clientSeenRequests.size(), is(2));
        assertThat(clientSeenRequests.get(1), instanceOf(DownsampleAction.Request.class));

        String downsampleIndexName = DownsampleConfig.generateDownsampleIndexName(
            DOWNSAMPLED_INDEX_PREFIX,
            state.metadata().getProject(projectId).index(firstGenIndex),
            new DateHistogramInterval("5m")
        );
        {
            // let's simulate the in-progress downsampling
            IndexMetadata firstGenMetadata = state.metadata().getProject(projectId).index(firstGenIndexName);
            ProjectMetadata.Builder newProjectBuilder = ProjectMetadata.builder(state.metadata().getProject(projectId));

            newProjectBuilder.put(
                IndexMetadata.builder(downsampleIndexName)
                    .settings(
                        Settings.builder()
                            .put(firstGenMetadata.getSettings())
                            .put(IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_UUID.getKey(), firstGenIndex.getUUID())
                            .put(IndexMetadata.INDEX_DOWNSAMPLE_STATUS.getKey(), STARTED)
                    )
                    .numberOfReplicas(0)
                    .numberOfShards(1)
            );
            state = ClusterState.builder(state).putProjectMetadata(newProjectBuilder).build();
            setState(clusterService, state);
        }

        // on the next run downsampling nothing should be triggered as downsampling is in progress (i.e. the STATUS is STARTED)
        affectedIndices = dataStreamLifecycleService.maybeExecuteDownsampling(
            clusterService.state().projectState(projectId),
            dataStream,
            List.of(firstGenIndex),
            0
        );
        assertThat(affectedIndices, is(Set.of(firstGenIndex)));
        // still only 2 witnessed requests, nothing extra
        assertThat(clientSeenRequests.size(), is(2));

        {
            // mark the downsample operation as complete
            IndexMetadata firstGenMetadata = state.metadata().getProject(projectId).index(firstGenIndexName);
            ProjectMetadata.Builder newProjectBuilder = ProjectMetadata.builder(state.metadata().getProject(projectId));

            newProjectBuilder.put(
                IndexMetadata.builder(downsampleIndexName)
                    .settings(
                        Settings.builder()
                            .put(firstGenMetadata.getSettings())
                            .put(IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_NAME_KEY, firstGenIndexName)
                            .put(IndexMetadata.INDEX_DOWNSAMPLE_ORIGIN_NAME_KEY, firstGenIndexName)
                            .put(IndexMetadata.INDEX_DOWNSAMPLE_STATUS.getKey(), SUCCESS)
                    )
                    .numberOfReplicas(0)
                    .numberOfShards(1)
            );
            state = ClusterState.builder(state).putProjectMetadata(newProjectBuilder).build();
            setState(clusterService, state);
        }

        // on this run, as downsampling is complete we expect to trigger the {@link
        // org.elasticsearch.datastreams.lifecycle.downsampling.DeleteSourceAndAddDownsampleToDS}
        // cluster service task and delete the source index whilst adding the downsample index in the data stream
        affectedIndices = dataStreamLifecycleService.maybeExecuteDownsampling(
            clusterService.state().projectState(projectId),
            dataStream,
            List.of(firstGenIndex),
            randomIntBetween(0, DEFAULT_MAX_DOWNSAMPLING_INDICES_IN_PROGRESS_PER_DATA_STREAM)
        );
        assertThat(affectedIndices, is(Set.of(firstGenIndex)));
        assertBusy(() -> {
            ClusterState newState = clusterService.state();
            IndexAbstraction downsample = newState.metadata().getProject(projectId).getIndicesLookup().get(downsampleIndexName);
            // the downsample index must be part of the data stream
            assertThat(downsample.getParentDataStream(), is(notNullValue()));
            assertThat(downsample.getParentDataStream().getName(), is(dataStreamName));
            // the source index was deleted
            IndexAbstraction sourceIndexAbstraction = newState.metadata().getProject(projectId).getIndicesLookup().get(firstGenIndexName);
            assertThat(sourceIndexAbstraction, is(nullValue()));

            // no further requests should be triggered
            assertThat(clientSeenRequests.size(), is(2));
        }, 30, TimeUnit.SECONDS);
    }

    public void testDownsamplingWhenTargetIndexNameClashYieldsException() throws Exception {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 2;
        final var projectId = randomProjectIdOrDefault();
        ProjectMetadata.Builder builder = ProjectMetadata.builder(projectId);
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            settings(IndexVersion.current()).put(MergePolicyConfig.INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING.getKey(), ONE_HUNDRED_MB)
                .put(MergePolicyConfig.INDEX_MERGE_POLICY_MERGE_FACTOR_SETTING.getKey(), TARGET_MERGE_FACTOR_VALUE)
                .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                .put("index.routing_path", "@timestamp"),
            DataStreamLifecycle.dataLifecycleBuilder()
                .downsamplingRounds(List.of(new DownsamplingRound(TimeValue.timeValueMillis(0), new DateHistogramInterval("5m"))))
                .dataRetention(TimeValue.MAX_VALUE)
                .build(),
            now
        );
        builder.put(dataStream);

        String nodeId = "localNode";
        DiscoveryNodes.Builder nodesBuilder = buildNodes(nodeId);
        // we are the master node
        nodesBuilder.masterNodeId(nodeId);
        String firstGenIndexName = dataStream.getIndices().getFirst().getName();

        // mark the first generation as read-only already
        IndexMetadata indexMetadata = builder.get(firstGenIndexName);
        Settings updatedSettings = Settings.builder().put(indexMetadata.getSettings()).put(WRITE.settingName(), true).build();
        builder.put(IndexMetadata.builder(indexMetadata).settings(updatedSettings).settingsVersion(indexMetadata.getSettingsVersion() + 1));

        ClusterBlock indexBlock = MetadataIndexStateService.createUUIDBasedBlock(WRITE.getBlock());
        ClusterBlocks.Builder blocks = ClusterBlocks.builder();
        blocks.addIndexBlock(projectId, firstGenIndexName, indexBlock);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .blocks(blocks)
            .putProjectMetadata(builder)
            .nodes(nodesBuilder)
            .build();

        // add another index to the cluster state that clashes with the expected downsample index name for the configured round
        String downsampleIndexName = DownsampleConfig.generateDownsampleIndexName(
            DOWNSAMPLED_INDEX_PREFIX,
            state.metadata().getProject(projectId).index(firstGenIndexName),
            new DateHistogramInterval("5m")
        );
        ProjectMetadata.Builder newProject = ProjectMetadata.builder(state.metadata().getProject(projectId))
            .put(
                IndexMetadata.builder(downsampleIndexName).settings(settings(IndexVersion.current())).numberOfReplicas(0).numberOfShards(1)
            );
        state = ClusterState.builder(state).putProjectMetadata(newProject).nodes(nodesBuilder).build();
        setState(clusterService, state);

        final var projectState = clusterService.state().projectState(projectId);
        Index firstGenIndex = projectState.metadata().index(firstGenIndexName).getIndex();
        dataStreamLifecycleService.maybeExecuteDownsampling(projectState, dataStream, List.of(firstGenIndex), 0);

        assertThat(clientSeenRequests.size(), is(0));
        ErrorEntry error = dataStreamLifecycleService.getErrorStore().getError(projectId, firstGenIndexName);
        assertThat(error, notNullValue());
        assertThat(error.error(), containsString("resource_already_exists_exception"));
    }

    public void testFloodGateThrottlesAllWhenAtCapacity() {
        var dataStream = createDataStream(
            projectBuilder,
            dataStreamName,
            randomIntBetween(1, 10),
            settings(IndexVersion.current()).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                .put("index.routing_path", "@timestamp")
                .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), now - 20000L)
                .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), now - 10000L),
            DataStreamLifecycle.dataLifecycleBuilder()
                .downsamplingRounds(List.of(new DownsamplingRound(TimeValue.timeValueMillis(0), new DateHistogramInterval("5m"))))
                .build(),
            now
        );
        projectBuilder.put(dataStream);

        ProjectMetadata initialProject = projectBuilder.build();
        ProjectMetadata.Builder updatedProjectBuilder = ProjectMetadata.builder(initialProject);
        ClusterBlocks.Builder blocks = ClusterBlocks.builder();
        List<Index> targetIndices = new ArrayList<>();

        for (int i = 0; i < dataStream.getIndices().size() - 1; i++) {
            Index index = dataStream.getIndices().get(i);
            markReadOnly(updatedProjectBuilder, blocks, projectId, initialProject.index(index));
            targetIndices.add(index);
        }

        String nodeId = "node-1";
        DiscoveryNodes.Builder nodesBuilder = buildNodes(nodeId);
        nodesBuilder.masterNodeId(nodeId);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(updatedProjectBuilder)
            .blocks(blocks)
            .nodes(nodesBuilder)
            .build();
        setState(clusterService, state);

        Set<Index> affectedIndices = dataStreamLifecycleService.maybeExecuteDownsampling(
            clusterService.state().projectState(projectId),
            dataStream,
            targetIndices,
            DEFAULT_MAX_DOWNSAMPLING_INDICES_IN_PROGRESS_PER_DATA_STREAM
        );
        assertThat(affectedIndices.size(), is(targetIndices.size()));
        assertThat(affectedIndices, containsInAnyOrder(targetIndices.toArray(new Index[0])));
        assertThat(countDownsampleRequests(), is(0L));
    }

    public void testFloodGateUsesRemainingSlots() {
        int eligibleDownsampleIndices = DEFAULT_MAX_DOWNSAMPLING_INDICES_IN_PROGRESS_PER_DATA_STREAM + randomIntBetween(1, 10);
        var dataStream = createDataStream(
            projectBuilder,
            dataStreamName,
            eligibleDownsampleIndices + 1, // + 1 for the write index
            settings(IndexVersion.current()).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                .put("index.routing_path", "@timestamp")
                .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), now - 20000L)
                .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), now - 10000L),
            DataStreamLifecycle.dataLifecycleBuilder()
                .downsamplingRounds(List.of(new DownsamplingRound(TimeValue.timeValueMillis(0), new DateHistogramInterval("5m"))))
                .build(),
            now
        );
        projectBuilder.put(dataStream);

        ProjectMetadata initialProject = projectBuilder.build();
        ProjectMetadata.Builder updatedProjectBuilder = ProjectMetadata.builder(initialProject);
        ClusterBlocks.Builder blocks = ClusterBlocks.builder();
        List<Index> targetIndices = new ArrayList<>();

        for (int i = 0; i < eligibleDownsampleIndices; i++) {
            Index index = dataStream.getIndices().get(i);
            markReadOnly(updatedProjectBuilder, blocks, projectId, initialProject.index(index));
            targetIndices.add(index);
        }

        String nodeId = "node-1";
        DiscoveryNodes.Builder nodesBuilder = buildNodes(nodeId);
        nodesBuilder.masterNodeId(nodeId);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(updatedProjectBuilder)
            .blocks(blocks)
            .nodes(nodesBuilder)
            .build();
        setState(clusterService, state);

        int availableSlots = randomIntBetween(1, DEFAULT_MAX_DOWNSAMPLING_INDICES_IN_PROGRESS_PER_DATA_STREAM - 1);
        Set<Index> affectedIndices = dataStreamLifecycleService.maybeExecuteDownsampling(
            clusterService.state().projectState(projectId),
            dataStream,
            targetIndices,
            DEFAULT_MAX_DOWNSAMPLING_INDICES_IN_PROGRESS_PER_DATA_STREAM - availableSlots
        );
        assertThat(affectedIndices.size(), is(targetIndices.size()));
        assertThat(affectedIndices, containsInAnyOrder(targetIndices.toArray(Index.EMPTY_ARRAY)));
        assertThat(countDownsampleRequests(), is((long) Math.min(availableSlots, eligibleDownsampleIndices)));
    }

    public void testActivelyDownsampledIndicesCountedAndExcluded() {
        var dataStream = createDataStream(
            projectBuilder,
            dataStreamName,
            DEFAULT_MAX_DOWNSAMPLING_INDICES_IN_PROGRESS_PER_DATA_STREAM + 1,
            settings(IndexVersion.current()).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                .put("index.routing_path", "@timestamp")
                .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), now - 20000L)
                .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), now - 10000L),
            DataStreamLifecycle.dataLifecycleBuilder()
                .downsamplingRounds(List.of(new DownsamplingRound(TimeValue.timeValueMillis(0), new DateHistogramInterval("5m"))))
                .build(),
            now
        );
        projectBuilder.put(dataStream);

        ProjectMetadata initialProject = projectBuilder.build();
        ProjectMetadata.Builder updatedProjectBuilder = ProjectMetadata.builder(initialProject);
        ClusterBlocks.Builder blocks = ClusterBlocks.builder();

        // Mark all non-write indices as read-only and force-merged
        for (int i = 0; i < dataStream.getIndices().size() - 1; i++) {
            markReadOnlyAndForceMerged(updatedProjectBuilder, blocks, projectId, initialProject.index(dataStream.getIndices().get(i)));
        }

        // Give the first MAX backing indices active persistent downsampling tasks so the active count equals the threshold
        for (int i = 0; i < DEFAULT_MAX_DOWNSAMPLING_INDICES_IN_PROGRESS_PER_DATA_STREAM; i++) {
            downsamplingIndices.add(dataStream.getIndices().get(i));
        }

        String nodeId = "node-1";
        DiscoveryNodes.Builder nodesBuilder = buildNodes(nodeId);
        nodesBuilder.masterNodeId(nodeId);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(updatedProjectBuilder)
            .blocks(blocks)
            .nodes(nodesBuilder)
            .build();
        setState(clusterService, state);

        dataStreamLifecycleService.run(clusterService.state());

        // The MAX actively downsampled indices are counted and consume the full threshold,
        // so the remaining eligible index (index at position MAX) must not be triggered.
        assertThat(countDownsampleRequests(), is(0L));
    }

    public void testFloodGateIsIndependentPerDataStream() {
        int backingIndicesCount = DEFAULT_MAX_DOWNSAMPLING_INDICES_IN_PROGRESS_PER_DATA_STREAM + randomIntBetween(1, 10);
        var ds1 = createDataStream(
            projectBuilder,
            "ds1-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT),
            backingIndicesCount,
            settings(IndexVersion.current()).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                .put("index.routing_path", "@timestamp")
                .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), now - 20000L)
                .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), now - 10000L),
            DataStreamLifecycle.dataLifecycleBuilder()
                .downsamplingRounds(List.of(new DownsamplingRound(TimeValue.timeValueMillis(0), new DateHistogramInterval("5m"))))
                .build(),
            now
        );
        projectBuilder.put(ds1);

        var ds2 = createDataStream(
            projectBuilder,
            "ds2-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT),
            backingIndicesCount,
            settings(IndexVersion.current()).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                .put("index.routing_path", "@timestamp")
                .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), now - 20000L)
                .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), now - 10000L),
            DataStreamLifecycle.dataLifecycleBuilder()
                .downsamplingRounds(List.of(new DownsamplingRound(TimeValue.timeValueMillis(0), new DateHistogramInterval("5m"))))
                .build(),
            now
        );
        projectBuilder.put(ds2);

        ProjectMetadata initialProject = projectBuilder.build();
        ProjectMetadata.Builder updatedProjectBuilder = ProjectMetadata.builder(initialProject);
        ClusterBlocks.Builder blocks = ClusterBlocks.builder();

        // Mark all non-write backing indices of both streams as read-only and force-merged
        for (int i = 0; i < backingIndicesCount - 1; i++) {
            markReadOnlyAndForceMerged(updatedProjectBuilder, blocks, projectId, initialProject.index(ds1.getIndices().get(i)));
            markReadOnlyAndForceMerged(updatedProjectBuilder, blocks, projectId, initialProject.index(ds2.getIndices().get(i)));
        }

        String nodeId = "node-1";
        DiscoveryNodes.Builder nodesBuilder = buildNodes(nodeId);
        nodesBuilder.masterNodeId(nodeId);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(updatedProjectBuilder)
            .blocks(blocks)
            .nodes(nodesBuilder)
            .build();
        setState(clusterService, state);

        dataStreamLifecycleService.run(clusterService.state());

        // Each data stream independently triggers up to the threshold, for a total of 2 * MAX
        assertThat(countDownsampleRequests(), is(2L * DEFAULT_MAX_DOWNSAMPLING_INDICES_IN_PROGRESS_PER_DATA_STREAM));
    }

    public void testDetectingAFailedRequestUnaffectedByFloodGate() {
        var dataStream = createDataStream(
            projectBuilder,
            dataStreamName,
            2,
            settings(IndexVersion.current()).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                .put("index.routing_path", "@timestamp")
                .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), now - 20000L)
                .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), now - 10000L),
            DataStreamLifecycle.dataLifecycleBuilder()
                .downsamplingRounds(List.of(new DownsamplingRound(TimeValue.timeValueMillis(0), new DateHistogramInterval("5m"))))
                .build(),
            now
        );
        projectBuilder.put(dataStream);

        Index backingIndex = dataStream.getIndices().getFirst();
        ProjectMetadata initialProject = projectBuilder.build();
        ProjectMetadata.Builder updatedProjectBuilder = ProjectMetadata.builder(initialProject);
        ClusterBlocks.Builder blocks = ClusterBlocks.builder();
        markReadOnlyAndForceMerged(updatedProjectBuilder, blocks, projectId, initialProject.index(backingIndex));

        // Add downsample index with STARTED status (simulating an in-progress task with no persistent task entry,
        // e.g. after a master failover)
        String downsampleIndexName = DownsampleConfig.generateDownsampleIndexName(
            DOWNSAMPLED_INDEX_PREFIX,
            initialProject.index(backingIndex),
            new DateHistogramInterval("5m")
        );
        updatedProjectBuilder.put(
            IndexMetadata.builder(downsampleIndexName)
                .settings(
                    Settings.builder()
                        .put(initialProject.index(backingIndex).getSettings())
                        .put(IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_UUID.getKey(), backingIndex.getUUID())
                        .put(IndexMetadata.INDEX_DOWNSAMPLE_STATUS.getKey(), STARTED)
                )
                .numberOfShards(1)
                .numberOfReplicas(0)
        );

        String nodeId = "node-1";
        DiscoveryNodes.Builder nodesBuilder = buildNodes(nodeId);
        nodesBuilder.masterNodeId(nodeId);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(updatedProjectBuilder)
            .blocks(blocks)
            .nodes(nodesBuilder)
            .build();
        setState(clusterService, state);

        // Pass activeDownsamplingCount = MAX so the flood gate would block new triggers
        dataStreamLifecycleService.maybeExecuteDownsampling(
            clusterService.state().projectState(projectId),
            dataStream,
            List.of(backingIndex),
            DEFAULT_MAX_DOWNSAMPLING_INDICES_IN_PROGRESS_PER_DATA_STREAM
        );

        // The STARTED re-issue is not gated by canTriggerNewDownsampling — it must still fire
        assertThat(countDownsampleRequests(), is(1L));
    }

    public void testSuccessSwapUnaffectedByFloodGate() throws Exception {
        var dataStream = createDataStream(
            projectBuilder,
            dataStreamName,
            2,
            settings(IndexVersion.current()).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                .put("index.routing_path", "@timestamp")
                .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), now - 20000L)
                .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), now - 10000L),
            DataStreamLifecycle.dataLifecycleBuilder()
                .downsamplingRounds(List.of(new DownsamplingRound(TimeValue.timeValueMillis(0), new DateHistogramInterval("5m"))))
                .build(),
            now
        );
        projectBuilder.put(dataStream);

        Index backingIndex = dataStream.getIndices().getFirst();
        String backingIndexName = backingIndex.getName();
        ProjectMetadata initialProject = projectBuilder.build();
        ProjectMetadata.Builder updatedProjectBuilder = ProjectMetadata.builder(initialProject);
        ClusterBlocks.Builder blocks = ClusterBlocks.builder();
        markReadOnlyAndForceMerged(updatedProjectBuilder, blocks, projectId, initialProject.index(backingIndex));

        // Add downsample index with SUCCESS status (complete, not yet in the data stream)
        String downsampleIndexName = DownsampleConfig.generateDownsampleIndexName(
            DOWNSAMPLED_INDEX_PREFIX,
            initialProject.index(backingIndex),
            new DateHistogramInterval("5m")
        );
        updatedProjectBuilder.put(
            IndexMetadata.builder(downsampleIndexName)
                .settings(
                    Settings.builder()
                        .put(initialProject.index(backingIndex).getSettings())
                        .put(IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_NAME_KEY, backingIndexName)
                        .put(IndexMetadata.INDEX_DOWNSAMPLE_ORIGIN_NAME_KEY, backingIndexName)
                        .put(IndexMetadata.INDEX_DOWNSAMPLE_STATUS.getKey(), SUCCESS)
                )
                .numberOfShards(1)
                .numberOfReplicas(0)
        );

        String nodeId = "node-1";
        DiscoveryNodes.Builder nodesBuilder = buildNodes(nodeId);
        nodesBuilder.masterNodeId(nodeId);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(updatedProjectBuilder)
            .blocks(blocks)
            .nodes(nodesBuilder)
            .build();
        setState(clusterService, state);

        // Pass activeDownsamplingCount = MAX so the flood gate would block new triggers
        dataStreamLifecycleService.maybeExecuteDownsampling(
            clusterService.state().projectState(projectId),
            dataStream,
            List.of(backingIndex),
            DEFAULT_MAX_DOWNSAMPLING_INDICES_IN_PROGRESS_PER_DATA_STREAM
        );

        // The SUCCESS swap is not gated by canTriggerNewDownsampling — it must still execute
        assertBusy(() -> {
            ClusterState newState = clusterService.state();
            IndexAbstraction downsample = newState.metadata().getProject(projectId).getIndicesLookup().get(downsampleIndexName);
            assertThat(downsample, notNullValue());
            assertThat(downsample.getParentDataStream(), notNullValue());
            assertThat(downsample.getParentDataStream().getName(), is(dataStreamName));
            // source index was deleted
            assertThat(newState.metadata().getProject(projectId).getIndicesLookup().get(backingIndexName), nullValue());
        }, 30, TimeUnit.SECONDS);
    }

    private static void markReadOnly(
        ProjectMetadata.Builder projectBuilder,
        ClusterBlocks.Builder blocks,
        ProjectId projectId,
        IndexMetadata indexMetadata
    ) {
        Settings updatedSettings = Settings.builder().put(indexMetadata.getSettings()).put(WRITE.settingName(), true).build();
        projectBuilder.put(
            IndexMetadata.builder(indexMetadata).settings(updatedSettings).settingsVersion(indexMetadata.getSettingsVersion() + 1)
        );
        blocks.addIndexBlock(
            projectId,
            indexMetadata.getIndex().getName(),
            MetadataIndexStateService.createUUIDBasedBlock(WRITE.getBlock())
        );
    }

    private static void markReadOnlyAndForceMerged(
        ProjectMetadata.Builder projectBuilder,
        ClusterBlocks.Builder blocks,
        ProjectId projectId,
        IndexMetadata indexMetadata
    ) {
        Settings updatedSettings = Settings.builder().put(indexMetadata.getSettings()).put(WRITE.settingName(), true).build();
        projectBuilder.put(
            IndexMetadata.builder(indexMetadata)
                .settings(updatedSettings)
                .settingsVersion(indexMetadata.getSettingsVersion() + 1)
                .putCustom(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY, Map.of(FORCE_MERGE_COMPLETED_TIMESTAMP_METADATA_KEY, "1"))
        );
        blocks.addIndexBlock(
            projectId,
            indexMetadata.getIndex().getName(),
            MetadataIndexStateService.createUUIDBasedBlock(WRITE.getBlock())
        );
    }

    private long countDownsampleRequests() {
        return clientSeenRequests.stream().filter(r -> r instanceof DownsampleAction.Request).count();
    }
}
