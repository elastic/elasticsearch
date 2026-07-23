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
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetentionSettings;
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
import org.elasticsearch.dlm.TimeSeriesEligibleWriteWindowLocator;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.action.admin.indices.create.AutoCreateAction.AUTO_CREATE_INDEX_PRIORITY_SETTING;
import static org.elasticsearch.cluster.routing.allocation.allocator.AllocationActionListener.rerouteCompletionIsNotRequired;

/**
 * Internal action that creates one or more historical TSDB backing indices to cover past timestamps.
 * It submits a cluster update per data stream and creates backing indices anchored to the start time of
 * the next existing backing index, tiling backward in {@link #PAST_TSDB_INDEX_INTERVAL}-sized slots.
 * When the gap between two neighbouring indices is small enough (≤ {@link #GAP_FILL_THRESHOLD} × interval),
 * a single index is created that fills the entire gap.
 */
public class TransportPastTimeSeriesIndexCreationAction extends TransportMasterNodeAction<
    PastTimeSeriesIndexCreationAction.Request,
    PastTimeSeriesIndexCreationAction.Response> {

    /**
     * Controls the size of each historical TSDB backing index created by this action.
     * Defaults to 1 day; valid range is [1h, 7d].
     */
    public static final Setting<TimeValue> PAST_TSDB_INDEX_INTERVAL = Setting.timeSetting(
        "data_streams.past_tsdb_index_interval",
        TimeValue.timeValueDays(1),
        TimeValue.timeValueHours(1),
        TimeValue.timeValueDays(7),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Maximum gap between two existing backing indices, expressed as a multiple of {@link #PAST_TSDB_INDEX_INTERVAL},
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
        Settings settings,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        AllocationService allocationService,
        MetadataCreateDataStreamService createDataStreamService,
        ProjectResolver projectResolver,
        TimeSeriesEligibleWriteWindowLocator timeSeriesEligibleWriteWindowLocator,
        DataStreamGlobalRetentionSettings dataStreamGlobalRetentionSettings
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
            projectResolver,
            timeSeriesEligibleWriteWindowLocator,
            dataStreamGlobalRetentionSettings
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

        try {
            validateDataStream(dataStreamName, dataStream, project);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        // Short-circuit when every requested timestamp is already covered by an existing backing index.
        Set<Instant> alreadyCovered = new HashSet<>();
        Set<Instant> remaining = new HashSet<>();
        for (Instant ts : request.timestamps()) {
            if (dataStream.selectTimeSeriesWriteIndex(ts, project) != null) {
                alreadyCovered.add(ts);
            } else {
                remaining.add(ts);
            }
        }
        if (remaining.isEmpty()) {
            listener.onResponse(new PastTimeSeriesIndexCreationAction.Response(true, alreadyCovered));
            return;
        }

        taskQueue.submitTask(
            "past-tsdb-index-creation [" + dataStreamName + "]",
            new PastTsdbIndexCreationTask(
                request.dataStreamName(),
                remaining.stream().mapToLong(Instant::toEpochMilli).sorted().toArray(),
                alreadyCovered,
                request.ackTimeout(),
                listener,
                request.requestStartTime()
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
        Set<Instant> alreadyCovered,
        TimeValue ackTimeout,
        ActionListener<PastTimeSeriesIndexCreationAction.Response> listener,
        long requestStartTimestamp
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
        private final ProjectResolver projectResolver;
        private final TimeSeriesEligibleWriteWindowLocator timeSeriesEligibleWriteWindowLocator;
        private final DataStreamGlobalRetentionSettings dataStreamGlobalRetentionSettings;
        private long indexIntervalMillis;

        PastTimeSeriesIndexCreationExecutor(
            Settings settings,
            ClusterService clusterService,
            AllocationService allocationService,
            MetadataCreateDataStreamService createDataStreamService,
            ProjectResolver projectResolver,
            TimeSeriesEligibleWriteWindowLocator timeSeriesEligibleWriteWindowLocator,
            DataStreamGlobalRetentionSettings dataStreamGlobalRetentionSettings
        ) {
            this.clusterService = clusterService;
            this.allocationService = allocationService;
            this.createDataStreamService = createDataStreamService;
            this.projectResolver = projectResolver;
            this.timeSeriesEligibleWriteWindowLocator = timeSeriesEligibleWriteWindowLocator;
            this.dataStreamGlobalRetentionSettings = dataStreamGlobalRetentionSettings;
            this.indexIntervalMillis = PAST_TSDB_INDEX_INTERVAL.get(settings).millis();
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
                    // We start with the already-covered timestamp
                    Set<Instant> coveredTimestamps = new HashSet<>(task.alreadyCovered());
                    Map<Instant, String> rejectedTimestamps = new HashMap<>();
                    ProjectMetadata projectMetadata = state.projectState(projectResolver.getProjectId()).metadata();
                    DataStream dataStream = projectMetadata.dataStreams().get(task.dataStreamName());
                    long eligibleWriteWindowStart = dataStream == null
                        ? -1L
                        : timeSeriesEligibleWriteWindowLocator.getEligibleWriteWindowStart(
                            dataStream,
                            projectMetadata,
                            dataStreamGlobalRetentionSettings.get(),
                            task.requestStartTimestamp()
                        );
                    state = executeTask(
                        state,
                        projectResolver,
                        createDataStreamService,
                        task,
                        createdIndexNames,
                        coveredTimestamps,
                        rejectedTimestamps,
                        indexIntervalMillis,
                        eligibleWriteWindowStart,
                        task.requestStartTimestamp
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
                                    .onResponse(
                                        new PastTimeSeriesIndexCreationAction.Response(
                                            true,
                                            Set.copyOf(coveredTimestamps),
                                            Map.copyOf(rejectedTimestamps)
                                        )
                                    );
                                return;
                            }
                            ActiveShardsObserver.waitForActiveShards(
                                clusterService,
                                projectResolver.getProjectId(),
                                createdIndexNames.toArray(String[]::new),
                                ActiveShardCount.DEFAULT,
                                task.ackTimeout(),
                                multiListener.delay(task.listener())
                                    .map(
                                        ok -> new PastTimeSeriesIndexCreationAction.Response(
                                            true,
                                            Set.copyOf(coveredTimestamps),
                                            Map.copyOf(rejectedTimestamps)
                                        )
                                    )
                            );
                        }

                        @Override
                        public void onAckFailure(Exception e) {
                            multiListener.delay(task.listener())
                                .onResponse(
                                    new PastTimeSeriesIndexCreationAction.Response(
                                        false,
                                        Set.copyOf(coveredTimestamps),
                                        Map.copyOf(rejectedTimestamps)
                                    )
                                );
                        }

                        @Override
                        public void onAckTimeout() {
                            multiListener.delay(task.listener())
                                .onResponse(
                                    new PastTimeSeriesIndexCreationAction.Response(
                                        false,
                                        Set.copyOf(coveredTimestamps),
                                        Map.copyOf(rejectedTimestamps)
                                    )
                                );
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

        // Package-visible for testing
        static ClusterState executeTask(
            ClusterState clusterState,
            ProjectResolver projectResolver,
            MetadataCreateDataStreamService createDataStreamService,
            PastTsdbIndexCreationTask task,
            List<String> createdIndexNames,
            Set<Instant> coveredTimestamps,
            Map<Instant, String> rejectedTimestamps,
            long indexIntervalMillis,
            long eligibleWriteWindowStart,
            long requestStartTime
        ) throws Exception {
            String dataStreamName = task.dataStreamName();
            long[] timestamps = task.sortedTimestamps();

            ProjectState projectState = clusterState.projectState(projectResolver.getProjectId());
            ProjectMetadata currentProject = projectState.metadata();
            DataStream dataStream = currentProject.dataStreams().get(dataStreamName);
            validateDataStream(dataStreamName, dataStream, currentProject);
            ClusterState updatedClusterState = clusterState;

            // From the oldest to the newest
            Deque<CoveredTimeWindow> stack = retrieveSortedTimeWindows(dataStream, currentProject);
            if (stack.isEmpty()) {
                throw new IllegalStateException(
                    "Cannot create past TSDB backing index for data stream ["
                        + dataStreamName
                        + "] because it has no time series backing index. Please rollover first."
                );
            }
            CoveredTimeWindow previousIndex = null;

            for (long ts : timestamps) {
                // We need to reload the data stream metadata to have the latest generation
                dataStream = updatedClusterState.projectState(projectResolver.getProjectId()).metadata().dataStreams().get(dataStreamName);
                Instant timestampInstant = Instant.ofEpochMilli(ts);
                if (ts > requestStartTime) {
                    rejectedTimestamps.put(
                        timestampInstant,
                        "the document timestamp ["
                            + timestampInstant
                            + "] is after the request start time ["
                            + Instant.ofEpochMilli(requestStartTime)
                            + "]"
                    );
                    continue;
                }
                if (eligibleWriteWindowStart > 0 && ts < eligibleWriteWindowStart) {
                    rejectedTimestamps.put(
                        timestampInstant,
                        "the document timestamp ["
                            + timestampInstant
                            + "] is earlier than the lifecycle permits writes, starting ["
                            + Instant.ofEpochMilli(eligibleWriteWindowStart)
                            + "]"
                    );
                    continue;
                }
                // Advance past indices whose range ends at or before ts.
                while (stack.isEmpty() == false && stack.peek().end() <= ts) {
                    previousIndex = stack.pop();
                }
                CoveredTimeWindow nextIndex = stack.isEmpty() ? null : stack.peek();
                assert nextIndex != null : "there should always be a next index, ultimately the write index";
                if (nextIndex.start() <= ts) {
                    coveredTimestamps.add(timestampInstant);
                    continue;
                }

                /*
                  Determining the new index time boundaries
                  - If the gap between the previous and next indices could be covered by a single index, we use these boundaries
                  - Otherwise, we jump backwards steps of size `indexIntervalMillis` from the next index's start time until we
                    find the time window that would cover our new timestamp. For example, we try to cover timestamp 2026-06-17T02:02:02Z
                    in a data stream that has one index [2026-06-19T09:12:00Z, 2026-06-21T14:34:02Z); the algorithm will jump 2
                    time windows [2026-06-18T09:12:00Z, 2026-06-19TT09:12:00Z), [2026-06-17T09:12:00Z, 2026-06-18TT09:12:00Z), and
                    it will create [2026-06-16T09:12:00Z, 2026-06-17TT09:12:00Z).
                 */
                long indexStart;
                long indexEnd;
                if (previousIndex != null
                    && (nextIndex.start() - previousIndex.end()) <= (long) (indexIntervalMillis * GAP_FILL_THRESHOLD)) {
                    indexStart = previousIndex.end();
                    indexEnd = nextIndex.start();
                } else {
                    // Jump backward in interval-sized windows anchored to nextIndex.start().
                    long numberOfJumps = (nextIndex.start() - ts - 1) / indexIntervalMillis;
                    indexEnd = nextIndex.start() - numberOfJumps * indexIntervalMillis;
                    indexStart = indexEnd - indexIntervalMillis;
                    if (previousIndex != null) {
                        indexStart = Math.max(indexStart, previousIndex.end());
                    }
                }

                String pastIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, dataStream.getGeneration() + 1, indexStart);
                updatedClusterState = createDataStreamService.createPastBackingIndex(
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
                // We're processing timestamps in order, so subsequent timestamps can't be mapped to an earlier interval.
                stack.push(new CoveredTimeWindow(indexStart, indexEnd));
                coveredTimestamps.add(timestampInstant);
            }
            return updatedClusterState;
        }

        /**
         * Iterates over the backing index time boundaries and consolidates them into continuous time windows sorted in a queue.
         */
        static Deque<CoveredTimeWindow> retrieveSortedTimeWindows(DataStream dataStream, ProjectMetadata currentProject) {
            List<CoveredTimeWindow> coveredTimeWindows = new ArrayList<>();
            for (Index existingIndex : dataStream.getIndices()) {
                IndexMetadata im = currentProject.index(existingIndex);
                if (im == null || IndexMode.isTsdb(im.getIndexMode()) == false) {
                    continue;
                }
                assert im.getTimeSeriesStart() != null && im.getTimeSeriesEnd() != null : "TSDB indices always have start and end time";
                coveredTimeWindows.add(new CoveredTimeWindow(im.getTimeSeriesStart(), im.getTimeSeriesEnd()));
            }

            coveredTimeWindows.sort(Comparator.comparingLong(CoveredTimeWindow::start));
            Deque<CoveredTimeWindow> sortedTimeWindows = new ArrayDeque<>();
            long currentWindowStart = -1;
            long currentWindowEnd = -1;
            for (int i = coveredTimeWindows.size() - 1; i >= 0; i--) {
                CoveredTimeWindow nextWindow = coveredTimeWindows.get(i);
                if (currentWindowStart == -1) {
                    currentWindowStart = nextWindow.start();
                    currentWindowEnd = nextWindow.end();
                } else if (currentWindowStart == nextWindow.end()) {
                    currentWindowStart = nextWindow.start();
                } else {
                    sortedTimeWindows.push(new CoveredTimeWindow(currentWindowStart, currentWindowEnd));
                    currentWindowStart = nextWindow.start();
                    currentWindowEnd = nextWindow.end();
                }
            }
            if (currentWindowStart != -1 && currentWindowEnd != -1) {
                sortedTimeWindows.push(new CoveredTimeWindow(currentWindowStart, currentWindowEnd));
            }
            return sortedTimeWindows;
        }

        public void init() {
            clusterService.getClusterSettings()
                .addSettingsUpdateConsumer(PAST_TSDB_INDEX_INTERVAL, tv -> indexIntervalMillis = tv.millis());
        }
    }

    record CoveredTimeWindow(long start, long end) {

        CoveredTimeWindow(Instant start, Instant end) {
            this(start.toEpochMilli(), end.toEpochMilli());
        }
    }

    private static void validateDataStream(String dataStreamName, DataStream dataStream, ProjectMetadata project) {
        if (dataStream == null) {
            throw new ResourceNotFoundException("Data stream [" + dataStreamName + "] not found");
        }
        if (dataStream.isReplicated()) {
            throw new IllegalArgumentException("Cannot create past TSDB backing index for replicated data stream [" + dataStreamName + "]");
        }
        if (dataStream.isSystem()) {
            throw new IllegalArgumentException("Cannot create past TSDB backing index for system data stream [" + dataStreamName + "]");
        }

        if (IndexMode.isTsdb(dataStream.getIndexMode()) == false) {
            throw new IllegalArgumentException(
                "Cannot create past TSDB backing index for data stream ["
                    + dataStreamName
                    + "] with mode ["
                    + dataStream.getIndexMode()
                    + "], it needs to be a time series data stream."
            );
        }
        Index writeIndex = dataStream.getWriteIndex();
        IndexMetadata writeIndexMetadata = writeIndex == null ? null : project.index(writeIndex);
        if (writeIndexMetadata == null || IndexMode.isTsdb(writeIndexMetadata.getIndexMode()) == false) {
            throw new IllegalStateException(
                "Cannot create past TSDB backing index for data stream ["
                    + dataStreamName
                    + "] because it requires to have yet at least one time series backing index. Please rollover first."
            );
        }
    }

}
