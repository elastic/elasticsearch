/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.datastreams.PastTimeSeriesIndexCreationAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MetadataCreateDataStreamService;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RerouteBehavior;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.allocator.AllocationActionMultiListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.action.admin.indices.create.AutoCreateAction.AUTO_CREATE_INDEX_PRIORITY_SETTING;
import static org.elasticsearch.cluster.routing.allocation.allocator.AllocationActionListener.rerouteCompletionIsNotRequired;

/**
 * Internal action that creates one or more historical TSDB backing indices to cover past timestamps.
 * It submits a cluster update per data stream and creates backing indices anchored to the start time of
 * the next existing backing index, tiling backward in {@link #PAST_TSDB_INDEX_DURATION}-sized slots.
 * When the gap between two neighbouring indices is small enough (≤ {@link #GAP_FILL_THRESHOLD} × duration),
 * a single index is created that fills the entire gap.
 */
public class TransportPastTimeSeriesIndexCreationAction extends TransportMasterNodeAction<
    PastTimeSeriesIndexCreationAction.Request,
    PastTimeSeriesIndexCreationAction.Response> {

    /**
     * Controls the size of each historical TSDB backing index created by this action.
     * Defaults to 1 day; valid range is [1h, 7d].
     */
    public static final Setting<TimeValue> PAST_TSDB_INDEX_DURATION = Setting.timeSetting(
        "data_streams.past_tsdb_index_duration",
        TimeValue.timeValueDays(1),
        TimeValue.timeValueHours(1),
        TimeValue.timeValueDays(7),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Maximum gap between two existing backing indices, expressed as a multiple of {@link #PAST_TSDB_INDEX_DURATION},
     * that is filled by a single new index instead of tiling backward from the next index.
     */
    static final double GAP_FILL_THRESHOLD = 1.3;
    private static final Logger logger = LogManager.getLogger(TransportPastTimeSeriesIndexCreationAction.class);
    private final MasterServiceTaskQueue<PastTsdbIndexCreationTask> taskQueue;
    private final ProjectResolver projectResolver;

    @Inject
    public TransportPastTimeSeriesIndexCreationAction(
        TransportService transportService,
        ClusterService clusterService,
        SystemIndices systemIndices,
        Settings settings,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        AllocationService allocationService,
        MetadataCreateDataStreamService createDataStreamService,
        ProjectResolver projectResolver
    ) {
        super(
            PastTimeSeriesIndexCreationAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PastTimeSeriesIndexCreationAction.Request::new,
            PastTimeSeriesIndexCreationAction.Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.projectResolver = projectResolver;
        PastTimeSeriesIndexCreationExecutor pastTimeSeriesIndexCreationExecutor = new PastTimeSeriesIndexCreationExecutor(
            settings,
            clusterService,
            allocationService,
            createDataStreamService,
            systemIndices,
            projectResolver
        );
        pastTimeSeriesIndexCreationExecutor.init();
        this.taskQueue = clusterService.createTaskQueue(
            "past-tsdb-index-creation",
            AUTO_CREATE_INDEX_PRIORITY_SETTING.get(settings),
            pastTimeSeriesIndexCreationExecutor
        );
    }

    @Override
    protected void masterOperation(
        Task task,
        PastTimeSeriesIndexCreationAction.Request request,
        ClusterState state,
        ActionListener<PastTimeSeriesIndexCreationAction.Response> listener
    ) {
        ProjectMetadata project = projectResolver.getProjectMetadata(state);
        String dataStreamName = request.dataStreamName();
        DataStream dataStream = project.dataStreams().get(dataStreamName);

        if (dataStream == null) {
            // No data stream — nothing can be covered.
            listener.onFailure(new ResourceNotFoundException("Data stream [" + dataStreamName + "] not found"));
            return;
        }

        // Checking here is sufficient because a data stream cannot be converted from leader to follower, only the other way around.
        if (dataStream.isReplicated()) {
            listener.onFailure(
                new ElasticsearchException("Cannot create past TSDB backing index for replicated data stream [" + dataStreamName + "]")
            );
            return;
        }

        // Short-circuit when every requested timestamp is already covered by an existing backing index.
        Set<Instant> alreadyCovered = new HashSet<>();
        boolean hasUncovered = false;
        for (Instant ts : request.timestamps()) {
            if (dataStream.selectTimeSeriesWriteIndex(ts, project) != null) {
                alreadyCovered.add(ts);
            } else {
                hasUncovered = true;
            }
        }
        if (hasUncovered == false) {
            listener.onResponse(new PastTimeSeriesIndexCreationAction.Response(true, alreadyCovered));
            return;
        }

        taskQueue.submitTask(
            "past-tsdb-index-creation [" + dataStreamName + "]",
            new PastTsdbIndexCreationTask(
                request.dataStreamName(),
                request.timestamps().stream().mapToLong(Instant::toEpochMilli).sorted().toArray(),
                request.ackTimeout(),
                listener
            ),
            request.masterNodeTimeout()
        );
    }

    @Override
    protected ClusterBlockException checkBlock(PastTimeSeriesIndexCreationAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    record PastTsdbIndexCreationTask(
        String dataStreamName,
        long[] sortedTimestamps,
        TimeValue ackTimeout,
        ActionListener<PastTimeSeriesIndexCreationAction.Response> listener
    ) implements ClusterStateTaskListener {

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    static class PastTimeSeriesIndexCreationExecutor implements ClusterStateTaskExecutor<PastTsdbIndexCreationTask> {

        private final ClusterService clusterService;
        private final AllocationService allocationService;
        private final MetadataCreateDataStreamService createDataStreamService;
        private final SystemIndices systemIndices;
        private final ProjectResolver projectResolver;
        private long indexDurationMillis;

        PastTimeSeriesIndexCreationExecutor(
            Settings settings,
            ClusterService clusterService,
            AllocationService allocationService,
            MetadataCreateDataStreamService createDataStreamService,
            SystemIndices systemIndices,
            ProjectResolver projectResolver
        ) {
            this.clusterService = clusterService;
            this.allocationService = allocationService;
            this.createDataStreamService = createDataStreamService;
            this.systemIndices = systemIndices;
            this.projectResolver = projectResolver;
            this.indexDurationMillis = PAST_TSDB_INDEX_DURATION.get(settings).millis();
        }

        @Override
        public ClusterState execute(BatchExecutionContext<PastTsdbIndexCreationTask> batchExecutionContext) throws Exception {
            final var multiListener = new AllocationActionMultiListener<PastTimeSeriesIndexCreationAction.Response>(
                clusterService.threadPool().getThreadContext()
            );
            var state = batchExecutionContext.initialState();
            boolean stateChanged = false;

            for (var taskContext : batchExecutionContext.taskContexts()) {
                final var task = taskContext.getTask();
                try (var ignored = taskContext.captureResponseHeaders()) {
                    List<String> createdIndexNames = new ArrayList<>();
                    Set<Instant> coveredTimestamps = new HashSet<>();
                    state = executeTask(
                        state,
                        projectResolver,
                        createDataStreamService,
                        systemIndices,
                        task,
                        createdIndexNames,
                        coveredTimestamps,
                        indexDurationMillis
                    );
                    stateChanged |= createdIndexNames.isEmpty() == false;
                    taskContext.success(new ClusterStateAckListener() {
                        @Override
                        public boolean mustAck(DiscoveryNode discoveryNode) {
                            return true;
                        }

                        @Override
                        public void onAllNodesAcked() {
                            if (createdIndexNames.isEmpty()) {
                                task.listener()
                                    .onResponse(new PastTimeSeriesIndexCreationAction.Response(true, Set.copyOf(coveredTimestamps)));
                                return;
                            }
                            ActiveShardsObserver.waitForActiveShards(
                                clusterService,
                                projectResolver.getProjectId(),
                                createdIndexNames.toArray(String[]::new),
                                ActiveShardCount.DEFAULT,
                                task.ackTimeout(),
                                multiListener.delay(task.listener())
                                    .map(ok -> new PastTimeSeriesIndexCreationAction.Response(true, Set.copyOf(coveredTimestamps)))
                            );
                        }

                        @Override
                        public void onAckFailure(Exception e) {
                            multiListener.delay(task.listener())
                                .onResponse(new PastTimeSeriesIndexCreationAction.Response(false, Set.copyOf(coveredTimestamps)));
                        }

                        @Override
                        public void onAckTimeout() {
                            multiListener.delay(task.listener())
                                .onResponse(new PastTimeSeriesIndexCreationAction.Response(false, Set.copyOf(coveredTimestamps)));
                        }

                        @Override
                        public TimeValue ackTimeout() {
                            return task.ackTimeout();
                        }
                    });
                } catch (Exception e) {
                    taskContext.onFailure(e);
                }
            }

            if (stateChanged) {
                state = allocationService.reroute(state, "past-tsdb-index-creation", multiListener.reroute());
            } else {
                multiListener.noRerouteNeeded();
            }
            return state;
        }

        // Visible for testing
        static ClusterState executeTask(
            ClusterState clusterState,
            ProjectResolver projectResolver,
            MetadataCreateDataStreamService createDataStreamService,
            SystemIndices systemIndices,
            PastTsdbIndexCreationTask task,
            List<String> createdIndexNames,
            Set<Instant> coveredTimestamps,
            long indexDurationMillis
        ) throws Exception {
            String dataStreamName = task.dataStreamName();
            long[] timestamps = task.sortedTimestamps();

            ProjectState projectState = clusterState.projectState(projectResolver.getProjectId());
            ProjectMetadata currentProject = projectState.metadata();
            DataStream dataStream = currentProject.dataStreams().get(dataStreamName);
            if (dataStream == null) {
                throw new ResourceNotFoundException("Data stream [" + dataStreamName + "] not found");
            }
            assert dataStream.isReplicated() == false
                : "cannot create past TSDB backing index for replicated data stream [" + dataStreamName + "]";
            ClusterState updatedClusterState = clusterState;

            // From the oldest to the newest
            Deque<IndexBoundaries> stack = sortAndRetrieveExistingBackingIndices(dataStream, currentProject);
            IndexBoundaries previousIndex = null;

            for (long ts : timestamps) {
                assert stack.isEmpty() == false : "the data stream must have at least one backing index and it should be the latest";
                // Advance past indices whose range ends at or before ts.
                while (stack.isEmpty() == false && stack.peek().end() <= ts) {
                    previousIndex = stack.pop();
                }
                IndexBoundaries nextIndex = stack.isEmpty() ? null : stack.peek();
                assert nextIndex != null : "there should always be a next index, ultimately the write index";
                if (nextIndex.start() <= ts) {
                    coveredTimestamps.add(Instant.ofEpochMilli(ts));
                    continue;
                }

                long indexStart;
                long indexEnd;
                if (previousIndex != null
                    && (nextIndex.start() - previousIndex.end()) <= (long) (indexDurationMillis * GAP_FILL_THRESHOLD)) {
                    indexStart = previousIndex.end();
                    indexEnd = nextIndex.start();
                } else {
                    // Tile backward in duration-sized slots anchored to nextIndex.start().
                    // k is the zero-based slot index counting back from nextIndex: slot 0 = [next-D, next), slot 1 = [next-2D, next-D), …
                    long k = Math.floorDiv(nextIndex.start() - ts - 1, indexDurationMillis);
                    indexEnd = nextIndex.start() - k * indexDurationMillis;
                    indexStart = indexEnd - indexDurationMillis;
                    if (previousIndex != null) {
                        indexStart = Math.max(indexStart, previousIndex.end());
                    }
                }

                String pastIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, dataStream.getGeneration() + 1, indexStart);
                updatedClusterState = createDataStreamService.createPastBackingIndex(
                    systemIndices,
                    updatedClusterState,
                    projectResolver.getProjectId(),
                    RerouteBehavior.SKIP_REROUTE,
                    rerouteCompletionIsNotRequired(),
                    dataStream,
                    pastIndexName,
                    Instant.ofEpochMilli(indexStart),
                    Instant.ofEpochMilli(indexEnd)
                );
                logger.info("created past TSDB backing index [{}] for data stream [{}]", pastIndexName, dataStreamName);
                createdIndexNames.add(pastIndexName);
                // The new index is pushed on top; it becomes the anchor for any timestamps further in the past.
                stack.push(new IndexBoundaries(indexStart, indexEnd));
                coveredTimestamps.add(Instant.ofEpochMilli(ts));
            }
            return updatedClusterState;
        }

        // For testing
        static Deque<IndexBoundaries> sortAndRetrieveExistingBackingIndices(DataStream dataStream, ProjectMetadata currentProject) {
            List<IndexBoundaries> sortedExistingBackingIndices = new ArrayList<>();
            for (Index existingIndex : dataStream.getIndices()) {
                IndexMetadata im = currentProject.index(existingIndex);
                if (im == null || im.getIndexMode() != IndexMode.TIME_SERIES) {
                    continue;
                }
                assert im.getTimeSeriesStart() != null && im.getTimeSeriesEnd() != null : "TSDB indices always have start and end time";
                sortedExistingBackingIndices.add(new IndexBoundaries(im.getTimeSeriesStart(), im.getTimeSeriesEnd()));
            }
            sortedExistingBackingIndices.sort(Comparator.comparingLong(IndexBoundaries::start));
            Deque<IndexBoundaries> sortedIndexBoundaries = new ArrayDeque<>();
            for (int i = sortedExistingBackingIndices.size() - 1; i >= 0; i--) {
                sortedIndexBoundaries.push(sortedExistingBackingIndices.get(i));
            }
            return sortedIndexBoundaries;
        }

        public void init() {
            clusterService.getClusterSettings()
                .addSettingsUpdateConsumer(PAST_TSDB_INDEX_DURATION, tv -> indexDurationMillis = tv.millis());
        }
    }

    record IndexBoundaries(long start, long end) {

        IndexBoundaries(Instant start, Instant end) {
            this(start.toEpochMilli(), end.toEpochMilli());
        }
    }

}
