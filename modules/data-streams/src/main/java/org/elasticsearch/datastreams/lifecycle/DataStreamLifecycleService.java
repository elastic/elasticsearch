/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ResultDeduplicator;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeAction;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockRequest;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockResponse;
import org.elasticsearch.action.admin.indices.readonly.TransportAddIndexBlockAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverConfiguration;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.admin.indices.settings.put.TransportUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.datastreams.lifecycle.ErrorEntry;
import org.elasticsearch.action.downsample.DownsampleAction;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.IndexComponentSelector;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetentionSettings;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.ResolvedExpression;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.SelectorResolver;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.scheduler.SchedulerEngine;
import org.elasticsearch.common.scheduler.TimeValueSchedule;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.datastreams.lifecycle.downsampling.DeleteSourceAndAddDownsampleIndexExecutor;
import org.elasticsearch.datastreams.lifecycle.downsampling.DeleteSourceAndAddDownsampleToDS;
import org.elasticsearch.datastreams.lifecycle.health.DataStreamLifecycleHealthInfoPublisher;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.snapshots.SnapshotInProgressException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;

import java.io.Closeable;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock.WRITE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.DownsampleTaskStatus.STARTED;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_DOWNSAMPLE_STATUS;
import static org.elasticsearch.datastreams.DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY;

/**
 * This service will implement the needed actions (e.g. rollover, retention) to manage the data streams with a data stream lifecycle
 * configured. It runs on the master node and it schedules a job according to the configured
 * {@link DataStreamLifecycleService#DATA_STREAM_LIFECYCLE_POLL_INTERVAL_SETTING}.
 */
public class DataStreamLifecycleService implements ClusterStateListener, Closeable, SchedulerEngine.Listener {

    public static final String DATA_STREAM_LIFECYCLE_POLL_INTERVAL = "data_streams.lifecycle.poll_interval";
    public static final Setting<TimeValue> DATA_STREAM_LIFECYCLE_POLL_INTERVAL_SETTING = Setting.timeSetting(
        DATA_STREAM_LIFECYCLE_POLL_INTERVAL,
        TimeValue.timeValueMinutes(5),
        TimeValue.timeValueSeconds(1),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final ByteSizeValue ONE_HUNDRED_MB = ByteSizeValue.ofMb(100);

    public static final int TARGET_MERGE_FACTOR_VALUE = 16;

    public static final Setting<Integer> DATA_STREAM_MERGE_POLICY_TARGET_FACTOR_SETTING = Setting.intSetting(
        "data_streams.lifecycle.target.merge.policy.merge_factor",
        TARGET_MERGE_FACTOR_VALUE,
        2,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<ByteSizeValue> DATA_STREAM_MERGE_POLICY_TARGET_FLOOR_SEGMENT_SETTING = Setting.byteSizeSetting(
        "data_streams.lifecycle.target.merge.policy.floor_segment",
        ONE_HUNDRED_MB,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    /**
     * This setting controls how often we signal that an index is in the error state when it comes to its data stream lifecycle
     * progression.
     * The signalling is currently logging at the `error` level but in the future it can signify other types of signalling.
     */
    public static final Setting<Integer> DATA_STREAM_SIGNALLING_ERROR_RETRY_INTERVAL_SETTING = Setting.intSetting(
        "data_streams.lifecycle.signalling.error_retry_interval",
        10,
        1,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final String DOWNSAMPLED_INDEX_PREFIX = "downsample-";

    private static final Logger logger = LogManager.getLogger(DataStreamLifecycleService.class);
    /**
     * Name constant for the job that schedules the data stream lifecycle
     */
    private static final String LIFECYCLE_JOB_NAME = "data_stream_lifecycle";
    /*
     * This is the key for data stream lifecycle related custom index metadata.
     */
    public static final String FORCE_MERGE_COMPLETED_TIMESTAMP_METADATA_KEY = "force_merge_completed_timestamp";
    private final Settings settings;
    private final Client client;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    final ResultDeduplicator<Tuple<ProjectId, TransportRequest>, Void> transportActionsDeduplicator;
    final ResultDeduplicator<Tuple<ProjectId, String>, Void> clusterStateChangesDeduplicator;
    private final DataStreamLifecycleHealthInfoPublisher dslHealthInfoPublisher;
    private final DataStreamGlobalRetentionSettings globalRetentionSettings;
    private final ProjectResolver projectResolver;
    private LongSupplier nowSupplier;
    private final Clock clock;
    private final DataStreamLifecycleErrorStore errorStore;
    private volatile boolean isMaster = false;
    private volatile TimeValue pollInterval;
    private volatile RolloverConfiguration rolloverConfiguration;
    private SchedulerEngine.Job scheduledJob;
    private final SetOnce<SchedulerEngine> scheduler = new SetOnce<>();
    private final MasterServiceTaskQueue<UpdateForceMergeCompleteTask> forceMergeClusterStateUpdateTaskQueue;
    private final MasterServiceTaskQueue<DeleteSourceAndAddDownsampleToDS> swapSourceWithDownsampleIndexQueue;
    private volatile ByteSizeValue targetMergePolicyFloorSegment;
    private volatile int targetMergePolicyFactor;
    /**
     * The number of retries for a particular index and error after which DSL will emmit a signal (e.g. log statement)
     */
    private volatile int signallingErrorRetryInterval;

    /**
     * The following stats are tracking how the data stream lifecycle runs are performing time wise
     */
    private volatile Long lastRunStartedAt = null;
    private volatile Long lastRunDuration = null;
    private volatile Long timeBetweenStarts = null;

    private static final SimpleBatchedExecutor<UpdateForceMergeCompleteTask, Void> FORCE_MERGE_STATE_UPDATE_TASK_EXECUTOR =
        new SimpleBatchedExecutor<>() {
            @Override
            public Tuple<ClusterState, Void> executeTask(UpdateForceMergeCompleteTask task, ClusterState clusterState) throws Exception {
                return Tuple.tuple(task.execute(clusterState), null);
            }

            @Override
            public void taskSucceeded(UpdateForceMergeCompleteTask task, Void unused) {
                logger.trace("Updated cluster state for force merge of index [{}]", task.targetIndex);
                task.listener.onResponse(null);
            }
        };

    public DataStreamLifecycleService(
        Settings settings,
        Client client,
        ClusterService clusterService,
        Clock clock,
        ThreadPool threadPool,
        LongSupplier nowSupplier,
        DataStreamLifecycleErrorStore errorStore,
        AllocationService allocationService,
        DataStreamLifecycleHealthInfoPublisher dataStreamLifecycleHealthInfoPublisher,
        DataStreamGlobalRetentionSettings globalRetentionSettings,
        ProjectResolver projectResolver
    ) {
        this.settings = settings;
        this.client = client;
        this.clusterService = clusterService;
        this.clock = clock;
        this.threadPool = threadPool;
        this.transportActionsDeduplicator = new ResultDeduplicator<>(threadPool.getThreadContext());
        this.clusterStateChangesDeduplicator = new ResultDeduplicator<>(threadPool.getThreadContext());
        this.nowSupplier = nowSupplier;
        this.errorStore = errorStore;
        this.globalRetentionSettings = globalRetentionSettings;
        this.projectResolver = projectResolver;
        this.scheduledJob = null;
        this.pollInterval = DATA_STREAM_LIFECYCLE_POLL_INTERVAL_SETTING.get(settings);
        this.targetMergePolicyFloorSegment = DATA_STREAM_MERGE_POLICY_TARGET_FLOOR_SEGMENT_SETTING.get(settings);
        this.targetMergePolicyFactor = DATA_STREAM_MERGE_POLICY_TARGET_FACTOR_SETTING.get(settings);
        this.signallingErrorRetryInterval = DATA_STREAM_SIGNALLING_ERROR_RETRY_INTERVAL_SETTING.get(settings);
        this.rolloverConfiguration = clusterService.getClusterSettings()
            .get(DataStreamLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING);
        this.forceMergeClusterStateUpdateTaskQueue = clusterService.createTaskQueue(
            "data-stream-lifecycle-forcemerge-state-update",
            Priority.LOW,
            FORCE_MERGE_STATE_UPDATE_TASK_EXECUTOR
        );
        this.swapSourceWithDownsampleIndexQueue = clusterService.createTaskQueue(
            "data-stream-lifecycle-swap-source-with-downsample",
            Priority.URGENT, // urgent priority as this deletes indices
            new DeleteSourceAndAddDownsampleIndexExecutor(allocationService)
        );
        this.dslHealthInfoPublisher = dataStreamLifecycleHealthInfoPublisher;
    }

    /**
     * Initializer method to avoid the publication of a self reference in the constructor.
     */
    public void init() {
        clusterService.addListener(this);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(DATA_STREAM_LIFECYCLE_POLL_INTERVAL_SETTING, this::updatePollInterval);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(DataStreamLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING, this::updateRolloverConfiguration);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(DATA_STREAM_MERGE_POLICY_TARGET_FACTOR_SETTING, this::updateMergePolicyFactor);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(DATA_STREAM_MERGE_POLICY_TARGET_FLOOR_SEGMENT_SETTING, this::updateMergePolicyFloorSegment);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(DATA_STREAM_SIGNALLING_ERROR_RETRY_INTERVAL_SETTING, this::updateSignallingRetryThreshold);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // wait for the cluster state to be recovered
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }

        final boolean prevIsMaster = this.isMaster;
        if (prevIsMaster != event.localNodeMaster()) {
            this.isMaster = event.localNodeMaster();
            if (this.isMaster) {
                // we weren't the master, and now we are
                maybeScheduleJob();
            } else {
                // we were the master, and now we aren't
                cancelJob();
                // clear the deduplicator on master failover so we could re-send the requests in case we're re-elected
                transportActionsDeduplicator.clear();
                logger.trace("Clearing the error store as we are not the elected master anymore");
                errorStore.clearStore();
            }
        }
    }

    @Override
    public void close() {
        SchedulerEngine engine = scheduler.get();
        if (engine != null) {
            engine.stop();
        }
        logger.trace("Clearing the error store as we are closing");
        errorStore.clearStore();
    }

    @Override
    public void triggered(SchedulerEngine.Event event) {
        if (event.jobName().equals(LIFECYCLE_JOB_NAME)) {
            if (this.isMaster) {
                logger.trace(
                    "Data stream lifecycle job triggered: {}, {}, {}",
                    event.jobName(),
                    event.scheduledTime(),
                    event.triggeredTime()
                );
                run(clusterService.state());
                dslHealthInfoPublisher.publishDslErrorEntries(new ActionListener<>() {
                    @Override
                    public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                        assert acknowledgedResponse.isAcknowledged() : "updating the health info is always acknowledged";
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.debug(
                            String.format(
                                Locale.ROOT,
                                "unable to update the health cache with DSL errors related information "
                                    + "due to [%s]. Will retry on the next DSL run",
                                e.getMessage()
                            ),
                            e
                        );
                    }
                });
            }
        }
    }

    /**
     * Iterates over the data stream lifecycle managed data streams and executes the needed operations
     * to satisfy the configured {@link DataStreamLifecycle}.
     */
    // default visibility for testing purposes
    void run(ClusterState state) {
        long startTime = nowSupplier.getAsLong();
        if (lastRunStartedAt != null) {
            timeBetweenStarts = startTime - lastRunStartedAt;
        }
        lastRunStartedAt = startTime;
        for (var projectId : state.metadata().projects().keySet()) {
            // We catch inside the loop to avoid one broken project preventing DLM to run on other projects.
            try {
                run(state.projectState(projectId));
            } catch (Exception e) {
                logger.error(Strings.format("Data stream lifecycle failed to run on project [%s]", projectId), e);
            }
        }
    }

    private void run(ProjectState projectState) {
        final var project = projectState.metadata();
        int affectedIndices = 0;
        int affectedDataStreams = 0;
        for (DataStream dataStream : project.dataStreams().values()) {
            clearErrorStoreForUnmanagedIndices(project, dataStream);
            var dataLifecycleEnabled = dataStream.getDataLifecycle() != null && dataStream.getDataLifecycle().enabled();
            var failureLifecycle = dataStream.getFailuresLifecycle();
            var failuresLifecycleEnabled = failureLifecycle != null && failureLifecycle.enabled();
            if (dataLifecycleEnabled == false && failuresLifecycleEnabled == false) {
                continue;
            }

            // Retrieve the effective retention to ensure the same retention is used for this data stream
            // through all operations.
            var dataRetention = getEffectiveRetention(dataStream, globalRetentionSettings, false);
            var failuresRetention = getEffectiveRetention(dataStream, globalRetentionSettings, true);

            // the following indices should not be considered for the remainder of this service run, for various reasons.
            Set<Index> indicesToExcludeForRemainingRun = new HashSet<>();

            // These are the pre-rollover write indices. They may or may not be the write index after maybeExecuteRollover has executed,
            // depending on rollover criteria, for this reason we exclude them for the remaining run.
            indicesToExcludeForRemainingRun.add(maybeExecuteRollover(project, dataStream, dataRetention, false));
            Index failureStoreWriteIndex = maybeExecuteRollover(project, dataStream, failuresRetention, true);
            if (failureStoreWriteIndex != null) {
                indicesToExcludeForRemainingRun.add(failureStoreWriteIndex);
            }

            // tsds indices that are still within their time bounds (i.e. now < time_series.end_time) - we don't want these indices to be
            // deleted, forcemerged, or downsampled as they're still expected to receive large amounts of writes
            indicesToExcludeForRemainingRun.addAll(
                timeSeriesIndicesStillWithinTimeBounds(
                    project,
                    getTargetIndices(dataStream, indicesToExcludeForRemainingRun, project::index, false),
                    nowSupplier
                )
            );

            try {
                indicesToExcludeForRemainingRun.addAll(
                    maybeExecuteRetention(project, dataStream, dataRetention, failuresRetention, indicesToExcludeForRemainingRun)
                );
            } catch (Exception e) {
                // individual index errors would be reported via the API action listener for every delete call
                // we could potentially record errors at a data stream level and expose it via the _data_stream API?
                logger.error(
                    () -> String.format(
                        Locale.ROOT,
                        "Data stream lifecycle failed to execute retention for data stream [%s]",
                        dataStream.getName()
                    ),
                    e
                );
            }

            try {
                indicesToExcludeForRemainingRun.addAll(
                    maybeExecuteForceMerge(project, getTargetIndices(dataStream, indicesToExcludeForRemainingRun, project::index, true))
                );
            } catch (Exception e) {
                logger.error(
                    () -> String.format(
                        Locale.ROOT,
                        "Data stream lifecycle failed to execute force merge for data stream [%s]",
                        dataStream.getName()
                    ),
                    e
                );
            }

            try {
                indicesToExcludeForRemainingRun.addAll(
                    maybeExecuteDownsampling(
                        projectState,
                        dataStream,
                        getTargetIndices(dataStream, indicesToExcludeForRemainingRun, project::index, false)
                    )
                );
            } catch (Exception e) {
                logger.error(
                    () -> String.format(
                        Locale.ROOT,
                        "Data stream lifecycle failed to execute downsampling for data stream [%s]",
                        dataStream.getName()
                    ),
                    e
                );
            }

            affectedIndices += indicesToExcludeForRemainingRun.size();
            affectedDataStreams++;
        }
        lastRunDuration = nowSupplier.getAsLong() - lastRunStartedAt;
        logger.trace(
            "Data stream lifecycle service ran for {} and performed operations on [{}] indices, part of [{}] data streams, in project [{}]",
            TimeValue.timeValueMillis(lastRunDuration).toHumanReadableString(2),
            affectedIndices,
            affectedDataStreams,
            project.id()
        );
    }

    // visible for testing
    static Set<Index> timeSeriesIndicesStillWithinTimeBounds(ProjectMetadata project, List<Index> targetIndices, LongSupplier nowSupplier) {
        Set<Index> tsIndicesWithinBounds = new HashSet<>();
        for (Index index : targetIndices) {
            IndexMetadata backingIndex = project.index(index);
            assert backingIndex != null : "the data stream backing indices must exist";
            if (IndexSettings.MODE.get(backingIndex.getSettings()) == IndexMode.TIME_SERIES) {
                Instant configuredEndTime = IndexSettings.TIME_SERIES_END_TIME.get(backingIndex.getSettings());
                assert configuredEndTime != null
                    : "a time series index must have an end time configured but [" + index.getName() + "] does not";
                if (nowSupplier.getAsLong() <= configuredEndTime.toEpochMilli()) {
                    logger.trace(
                        "Data stream lifecycle will not perform any operations in this run on time series index [{}] because "
                            + "its configured [{}] end time has not lapsed",
                        index.getName(),
                        configuredEndTime
                    );
                    tsIndicesWithinBounds.add(index);
                }
            }
        }
        return tsIndicesWithinBounds;
    }

    /**
     * Data stream lifecycle supports configuring multiple rounds of downsampling for each managed index. When attempting to execute
     * downsampling we iterate through the ordered rounds of downsampling that match an index (ordered ascending according to the `after`
     * configuration) and try to figure out:
     * - if we started downsampling for an earlier round and is in progress, in which case we need to wait for it to complete
     * - if we started downsampling for an earlier round and it's finished but the downsampling index is not part of the data stream, in
     * which case we need to replace the backing index with the downsampling index and delete the backing index
     * - if we don't have any early rounds started or to add to the data stream, start downsampling the last matching round
     *
     * Note that the first time an index has a matching downsampling round we first mark it as read-only.
     *
     * Returns a set of indices that now have in-flight operations triggered by downsampling (it could be marking them as read-only,
     * replacing an index in the data stream, deleting a source index, or downsampling itself) so these indices can be skipped in case
     * there are other operations to be executed by the data stream lifecycle after downsampling.
     */
    Set<Index> maybeExecuteDownsampling(ProjectState projectState, DataStream dataStream, List<Index> targetIndices) {
        Set<Index> affectedIndices = new HashSet<>();
        final var project = projectState.metadata();
        for (Index index : targetIndices) {
            IndexMetadata backingIndexMeta = project.index(index);
            assert backingIndexMeta != null : "the data stream backing indices must exist";
            List<DataStreamLifecycle.DownsamplingRound> downsamplingRounds = dataStream.getDownsamplingRoundsFor(
                index,
                project::index,
                nowSupplier
            );
            if (downsamplingRounds.isEmpty()) {
                continue;
            }

            String indexName = index.getName();
            String downsamplingSourceIndex = IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_NAME.get(backingIndexMeta.getSettings());

            // if the current index is not a downsample we want to mark the index as read-only before proceeding with downsampling
            if (org.elasticsearch.common.Strings.hasText(downsamplingSourceIndex) == false
                && projectState.blocks().indexBlocked(project.id(), ClusterBlockLevel.WRITE, indexName) == false) {
                affectedIndices.add(index);
                addIndexBlockOnce(project.id(), indexName);
            } else {
                // we're not performing any operation for this index which means that it:
                // - has matching downsample rounds
                // - is read-only
                // So let's wait for an in-progress downsampling operation to succeed or trigger the last matching round
                affectedIndices.addAll(waitForInProgressOrTriggerDownsampling(dataStream, backingIndexMeta, downsamplingRounds, project));
            }
        }

        return affectedIndices;
    }

    /**
     * Iterate over the matching downsampling rounds for the backing index (if any) and either wait for an early round to complete,
     * add an early completed downsampling round to the data stream, or otherwise trigger the last matching downsampling round.
     *
     * Returns the indices for which we triggered an action/operation.
     */
    private Set<Index> waitForInProgressOrTriggerDownsampling(
        DataStream dataStream,
        IndexMetadata backingIndex,
        List<DataStreamLifecycle.DownsamplingRound> downsamplingRounds,
        ProjectMetadata project
    ) {
        assert dataStream.getIndices().contains(backingIndex.getIndex())
            : "the provided backing index must be part of data stream:" + dataStream.getName();
        assert downsamplingRounds.isEmpty() == false : "the index should be managed and have matching downsampling rounds";
        Set<Index> affectedIndices = new HashSet<>();
        DataStreamLifecycle.DownsamplingRound lastRound = downsamplingRounds.get(downsamplingRounds.size() - 1);

        Index index = backingIndex.getIndex();
        String indexName = index.getName();
        for (DataStreamLifecycle.DownsamplingRound round : downsamplingRounds) {
            // the downsample index name for each round is deterministic
            String downsampleIndexName = DownsampleConfig.generateDownsampleIndexName(
                DOWNSAMPLED_INDEX_PREFIX,
                backingIndex,
                round.config().getFixedInterval()
            );
            IndexMetadata targetDownsampleIndexMeta = project.index(downsampleIndexName);
            boolean targetDownsampleIndexExists = targetDownsampleIndexMeta != null;

            if (targetDownsampleIndexExists) {
                Set<Index> downsamplingNotComplete = evaluateDownsampleStatus(
                    project.id(),
                    dataStream,
                    INDEX_DOWNSAMPLE_STATUS.get(targetDownsampleIndexMeta.getSettings()),
                    round,
                    lastRound,
                    index,
                    targetDownsampleIndexMeta.getIndex()
                );
                if (downsamplingNotComplete.isEmpty() == false) {
                    affectedIndices.addAll(downsamplingNotComplete);
                    break;
                }
            } else {
                if (round.equals(lastRound)) {
                    // no maintenance needed for previously started downsampling actions and we are on the last matching round so it's time
                    // to kick off downsampling
                    affectedIndices.add(index);
                    downsampleIndexOnce(round, project.id(), indexName, downsampleIndexName);
                }
            }
        }
        return affectedIndices;
    }

    /**
     * Issues a request downsample the source index to the downsample index for the specified round.
     */
    private void downsampleIndexOnce(
        DataStreamLifecycle.DownsamplingRound round,
        ProjectId projectId,
        String sourceIndex,
        String downsampleIndexName
    ) {
        DownsampleAction.Request request = new DownsampleAction.Request(
            TimeValue.THIRTY_SECONDS /* TODO should this be longer/configurable? */,
            sourceIndex,
            downsampleIndexName,
            null,
            round.config()
        );
        transportActionsDeduplicator.executeOnce(
            Tuple.tuple(projectId, request),
            new ErrorRecordingActionListener(
                DownsampleAction.NAME,
                projectId,
                sourceIndex,
                errorStore,
                Strings.format(
                    "Data stream lifecycle encountered an error trying to downsample index [%s]. Data stream lifecycle will "
                        + "attempt to downsample the index on its next run.",
                    sourceIndex
                ),
                signallingErrorRetryInterval
            ),
            (req, reqListener) -> downsampleIndex(projectId, request, reqListener)
        );
    }

    /**
     * Checks the status of the downsampling operations for the provided backing index and its corresponding downsample index.
     * Depending on the status, we'll either error (if it's UNKNOWN and we've reached the last round), wait for it to complete (if it's
     * STARTED), or replace the backing index with the downsample index in the data stream (if the status is SUCCESS).
     */
    private Set<Index> evaluateDownsampleStatus(
        ProjectId projectId,
        DataStream dataStream,
        IndexMetadata.DownsampleTaskStatus downsampleStatus,
        DataStreamLifecycle.DownsamplingRound currentRound,
        DataStreamLifecycle.DownsamplingRound lastRound,
        Index backingIndex,
        Index downsampleIndex
    ) {
        Set<Index> affectedIndices = new HashSet<>();
        String indexName = backingIndex.getName();
        String downsampleIndexName = downsampleIndex.getName();
        return switch (downsampleStatus) {
            case UNKNOWN -> {
                if (currentRound.equals(lastRound)) {
                    // target downsampling index exists and is not a downsampling index (name clash?)
                    // we fail now but perhaps we should just randomise the name?

                    recordAndLogError(
                        projectId,
                        indexName,
                        errorStore,
                        new ResourceAlreadyExistsException(downsampleIndexName),
                        String.format(
                            Locale.ROOT,
                            "Data stream lifecycle service is unable to downsample backing index [%s] for data "
                                + "stream [%s] and donwsampling round [%s] because the target downsample index [%s] already exists",
                            indexName,
                            dataStream.getName(),
                            currentRound,
                            downsampleIndexName
                        ),
                        signallingErrorRetryInterval
                    );
                }
                yield affectedIndices;
            }
            case STARTED -> {
                // we'll wait for this round to complete
                // TODO add support for cancelling a current in-progress operation if another, later, round matches
                logger.trace(
                    "Data stream lifecycle service waits for index [{}] to be downsampled. Current status is [{}] and the "
                        + "downsample index name is [{}]",
                    indexName,
                    STARTED,
                    downsampleIndexName
                );
                // this request here might seem weird, but hear me out:
                // if we triggered a downsample operation, and then had a master failover (so DSL starts from scratch)
                // we can't really find out if the downsampling persistent task failed (if it was successful, no worries, the next case
                // SUCCESS branch will catch it and we will cruise forward)
                // if the downsampling persistent task failed, we will find out only via re-issuing the downsample request (and we will
                // continue to re-issue the request until we get SUCCESS)

                // NOTE that the downsample request is made through the deduplicator so it will only really be executed if
                // there isn't one already in-flight. This can happen if a previous request timed-out, failed, or there was a
                // master failover and data stream lifecycle needed to restart
                downsampleIndexOnce(currentRound, projectId, indexName, downsampleIndexName);
                affectedIndices.add(backingIndex);
                yield affectedIndices;
            }
            case SUCCESS -> {
                if (dataStream.getIndices().contains(downsampleIndex) == false) {
                    // at this point the source index is part of the data stream and the downsample index is complete but not
                    // part of the data stream. we need to replace the source index with the downsample index in the data stream
                    affectedIndices.add(backingIndex);
                    replaceBackingIndexWithDownsampleIndexOnce(projectId, dataStream, indexName, downsampleIndexName);
                }
                yield affectedIndices;
            }
        };
    }

    /**
     * Issues a request to replace the backing index with the downsample index through the cluster state changes deduplicator.
     */
    private void replaceBackingIndexWithDownsampleIndexOnce(
        ProjectId projectId,
        DataStream dataStream,
        String backingIndexName,
        String downsampleIndexName
    ) {
        String requestName = "dsl-replace-" + dataStream.getName() + "-" + backingIndexName + "-" + downsampleIndexName;
        clusterStateChangesDeduplicator.executeOnce(
            // we use a String key here as otherwise it's ... awkward as we have to create the DeleteSourceAndAddDownsampleToDS as the
            // key _without_ a listener (passing in null) and then below we create it again with the `reqListener`. We're using a String
            // as it seems to be clearer.
            Tuple.tuple(projectId, requestName),
            new ErrorRecordingActionListener(
                requestName,
                projectId,
                backingIndexName,
                errorStore,
                Strings.format(
                    "Data stream lifecycle encountered an error trying to replace index [%s] with index [%s] in data stream [%s]",
                    backingIndexName,
                    downsampleIndexName,
                    dataStream
                ),
                signallingErrorRetryInterval
            ),
            (req, reqListener) -> {
                logger.trace(
                    "Data stream lifecycle issues request to replace index [{}] with index [{}] in data stream [{}]",
                    backingIndexName,
                    downsampleIndexName,
                    dataStream
                );
                swapSourceWithDownsampleIndexQueue.submitTask(
                    "data-stream-lifecycle-delete-source[" + backingIndexName + "]-add-to-datastream-[" + downsampleIndexName + "]",
                    new DeleteSourceAndAddDownsampleToDS(
                        settings,
                        projectId,
                        dataStream.getName(),
                        backingIndexName,
                        downsampleIndexName,
                        reqListener
                    ),
                    null
                );
            }
        );
    }

    /**
     * Issues a request to delete the provided index through the transport action deduplicator.
     */
    private void deleteIndexOnce(ProjectId projectId, String indexName, String reason) {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName).masterNodeTimeout(TimeValue.MAX_VALUE);
        transportActionsDeduplicator.executeOnce(
            Tuple.tuple(projectId, deleteIndexRequest),
            new ErrorRecordingActionListener(
                TransportDeleteIndexAction.TYPE.name(),
                projectId,
                indexName,
                errorStore,
                Strings.format("Data stream lifecycle encountered an error trying to delete index [%s]", indexName),
                signallingErrorRetryInterval
            ),
            (req, reqListener) -> deleteIndex(projectId, deleteIndexRequest, reason, reqListener)
        );
    }

    /**
     * Issues a request to add a WRITE index block for the provided index through the transport action deduplicator.
     */
    private void addIndexBlockOnce(ProjectId projectId, String indexName) {
        AddIndexBlockRequest addIndexBlockRequest = new AddIndexBlockRequest(WRITE, indexName).masterNodeTimeout(TimeValue.MAX_VALUE);
        transportActionsDeduplicator.executeOnce(
            Tuple.tuple(projectId, addIndexBlockRequest),
            new ErrorRecordingActionListener(
                TransportAddIndexBlockAction.TYPE.name(),
                projectId,
                indexName,
                errorStore,
                Strings.format("Data stream lifecycle service encountered an error trying to mark index [%s] as readonly", indexName),
                signallingErrorRetryInterval
            ),
            (req, reqListener) -> addIndexBlock(projectId, addIndexBlockRequest, reqListener)
        );
    }

    /**
     * Returns the data stream lifecycle managed indices that are not part of the set of indices to exclude.
     */
    // For testing
    static List<Index> getTargetIndices(
        DataStream dataStream,
        Set<Index> indicesToExcludeForRemainingRun,
        Function<String, IndexMetadata> indexMetadataSupplier,
        boolean withFailureStore
    ) {
        List<Index> targetIndices = new ArrayList<>();
        for (Index index : dataStream.getIndices()) {
            if (dataStream.isIndexManagedByDataStreamLifecycle(index, indexMetadataSupplier)
                && indicesToExcludeForRemainingRun.contains(index) == false) {
                targetIndices.add(index);
            }
        }
        if (withFailureStore && dataStream.getFailureIndices().isEmpty() == false) {
            for (Index index : dataStream.getFailureIndices()) {
                if (dataStream.isIndexManagedByDataStreamLifecycle(index, indexMetadataSupplier)
                    && indicesToExcludeForRemainingRun.contains(index) == false) {
                    targetIndices.add(index);
                }
            }
        }
        return targetIndices;
    }

    /**
     * This clears the error store for the case where a data stream or some backing indices were managed by data stream lifecycle, failed in
     * their lifecycle execution, and then they were not managed by the data stream lifecycle (maybe they were switched to ILM).
     */
    private void clearErrorStoreForUnmanagedIndices(ProjectMetadata project, DataStream dataStream) {
        for (String indexName : errorStore.getAllIndices(project.id())) {
            IndexAbstraction indexAbstraction = project.getIndicesLookup().get(indexName);
            DataStream parentDataStream = indexAbstraction != null ? indexAbstraction.getParentDataStream() : null;
            if (indexAbstraction == null || parentDataStream == null) {
                logger.trace(
                    "Clearing recorded error for index [{}] because the index doesn't exist or is not a data stream backing index anymore",
                    indexName
                );
                errorStore.clearRecordedError(project.id(), indexName);
            } else if (parentDataStream.getName().equals(dataStream.getName())) {
                // we're only verifying the indices that pertain to this data stream
                IndexMetadata indexMeta = project.index(indexName);
                if (dataStream.isIndexManagedByDataStreamLifecycle(indexMeta.getIndex(), project::index) == false) {
                    logger.trace("Clearing recorded error for index [{}] because the index is not managed by DSL anymore", indexName);
                    errorStore.clearRecordedError(project.id(), indexName);
                }
            }
        }
    }

    @Nullable
    private Index maybeExecuteRollover(
        ProjectMetadata project,
        DataStream dataStream,
        TimeValue effectiveRetention,
        boolean rolloverFailureStore
    ) {
        Index currentRunWriteIndex = rolloverFailureStore ? dataStream.getWriteFailureIndex() : dataStream.getWriteIndex();
        if (currentRunWriteIndex == null) {
            return null;
        }
        try {
            if (dataStream.isIndexManagedByDataStreamLifecycle(currentRunWriteIndex, project::index)) {
                DataStreamLifecycle lifecycle = rolloverFailureStore ? dataStream.getFailuresLifecycle() : dataStream.getDataLifecycle();
                RolloverRequest rolloverRequest = getDefaultRolloverRequest(
                    rolloverConfiguration,
                    dataStream.getName(),
                    effectiveRetention,
                    rolloverFailureStore
                );
                transportActionsDeduplicator.executeOnce(
                    Tuple.tuple(project.id(), rolloverRequest),
                    new ErrorRecordingActionListener(
                        RolloverAction.NAME,
                        project.id(),
                        currentRunWriteIndex.getName(),
                        errorStore,
                        Strings.format(
                            "Data stream lifecycle encountered an error trying to roll over%s data stream [%s]",
                            rolloverFailureStore ? " the failure store of " : "",
                            dataStream.getName()
                        ),
                        signallingErrorRetryInterval
                    ),
                    (req, reqListener) -> rolloverDataStream(project.id(), currentRunWriteIndex.getName(), rolloverRequest, reqListener)
                );
            }
        } catch (Exception e) {
            logger.error(
                () -> String.format(
                    Locale.ROOT,
                    "Data stream lifecycle encountered an error trying to roll over%s data stream [%s]",
                    rolloverFailureStore ? " the failure store of " : "",
                    dataStream.getName()
                ),
                e
            );
            ProjectMetadata latestProject = clusterService.state().metadata().projects().get(project.id());
            DataStream latestDataStream = latestProject == null ? null : latestProject.dataStreams().get(dataStream.getName());
            if (latestDataStream != null) {
                if (latestDataStream.getWriteIndex().getName().equals(currentRunWriteIndex.getName())) {
                    // data stream has not been rolled over in the meantime so record the error against the write index we
                    // attempted the rollover
                    errorStore.recordError(project.id(), currentRunWriteIndex.getName(), e);
                }
            }
        }
        return currentRunWriteIndex;
    }

    /**
     * This method sends requests to delete any indices in the datastream that exceed its retention policy. It returns the set of indices
     * it has sent delete requests for.
     *
     * @param project                         The project metadata from which to get index metadata
     * @param dataStream                      The data stream
     * @param indicesToExcludeForRemainingRun Indices to exclude from retention even if it would be time for them to be deleted
     * @return The set of indices that delete requests have been sent for
     */
    Set<Index> maybeExecuteRetention(
        ProjectMetadata project,
        DataStream dataStream,
        TimeValue dataRetention,
        TimeValue failureRetention,
        Set<Index> indicesToExcludeForRemainingRun
    ) {
        if (dataRetention == null && failureRetention == null) {
            return Set.of();
        }
        List<Index> backingIndicesOlderThanRetention = dataStream.getIndicesPastRetention(
            project::index,
            nowSupplier,
            dataRetention,
            false
        );
        List<Index> failureIndicesOlderThanRetention = dataStream.getIndicesPastRetention(
            project::index,
            nowSupplier,
            failureRetention,
            true
        );
        if (backingIndicesOlderThanRetention.isEmpty() && failureIndicesOlderThanRetention.isEmpty()) {
            return Set.of();
        }
        Set<Index> indicesToBeRemoved = new HashSet<>();
        if (backingIndicesOlderThanRetention.isEmpty() == false) {
            assert dataStream.getDataLifecycle() != null : "data stream should have data lifecycle if we have 'old' indices";
            for (Index index : backingIndicesOlderThanRetention) {
                if (indicesToExcludeForRemainingRun.contains(index) == false) {
                    IndexMetadata backingIndex = project.index(index);
                    assert backingIndex != null : "the data stream backing indices must exist";

                    IndexMetadata.DownsampleTaskStatus downsampleStatus = INDEX_DOWNSAMPLE_STATUS.get(backingIndex.getSettings());
                    // we don't want to delete the source index if they have an in-progress downsampling operation because the
                    // target downsample index will remain in the system as a standalone index
                    if (downsampleStatus == STARTED) {
                        // there's an opportunity here to cancel downsampling and delete the source index now
                        logger.trace(
                            "Data stream lifecycle skips deleting index [{}] even though its retention period [{}] has lapsed "
                                + "because there's a downsampling operation currently in progress for this index. Current downsampling "
                                + "status is [{}]. When downsampling completes, DSL will delete this index.",
                            index.getName(),
                            dataRetention,
                            downsampleStatus
                        );
                    } else {
                        // UNKNOWN is the default value, and has no real use. So index should be deleted
                        // SUCCESS meaning downsampling completed successfully and there is nothing in progress, so we can also delete
                        indicesToBeRemoved.add(index);

                        // there's an opportunity here to batch the delete requests (i.e. delete 100 indices / request)
                        // let's start simple and reevaluate
                        String indexName = backingIndex.getIndex().getName();
                        deleteIndexOnce(project.id(), indexName, "the lapsed [" + dataRetention + "] retention period");
                    }
                }
            }
        }
        if (failureIndicesOlderThanRetention.isEmpty() == false) {
            assert dataStream.getFailuresLifecycle() != null : "data stream should have failures lifecycle if we have 'old' indices";
            for (Index index : failureIndicesOlderThanRetention) {
                if (indicesToExcludeForRemainingRun.contains(index) == false) {
                    IndexMetadata failureIndex = project.index(index);
                    assert failureIndex != null : "the data stream failure indices must exist";
                    indicesToBeRemoved.add(index);
                    // there's an opportunity here to batch the delete requests (i.e. delete 100 indices / request)
                    // let's start simple and reevaluate
                    String indexName = failureIndex.getIndex().getName();
                    deleteIndexOnce(project.id(), indexName, "the lapsed [" + failureRetention + "] retention period");
                }
            }
        }
        return indicesToBeRemoved;
    }

    /*
     * This method force merges the given indices in the datastream. It writes a timestamp in the cluster state upon completion of the
     * force merge.
     */
    private Set<Index> maybeExecuteForceMerge(ProjectMetadata project, List<Index> indices) {
        Set<Index> affectedIndices = new HashSet<>();
        for (Index index : indices) {
            IndexMetadata backingIndex = project.index(index);
            assert backingIndex != null : "the data stream backing indices must exist";
            String indexName = index.getName();
            boolean alreadyForceMerged = isForceMergeComplete(backingIndex);
            if (alreadyForceMerged) {
                logger.trace("Already force merged {}", indexName);
                continue;
            }

            ByteSizeValue configuredFloorSegmentMerge = MergePolicyConfig.INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING.get(
                backingIndex.getSettings()
            );
            Integer configuredMergeFactor = MergePolicyConfig.INDEX_MERGE_POLICY_MERGE_FACTOR_SETTING.get(backingIndex.getSettings());
            if ((configuredFloorSegmentMerge == null || configuredFloorSegmentMerge.equals(targetMergePolicyFloorSegment) == false)
                || (configuredMergeFactor == null || configuredMergeFactor.equals(targetMergePolicyFactor) == false)) {
                UpdateSettingsRequest updateMergePolicySettingsRequest = new UpdateSettingsRequest();
                updateMergePolicySettingsRequest.indices(indexName);
                updateMergePolicySettingsRequest.settings(
                    Settings.builder()
                        .put(MergePolicyConfig.INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING.getKey(), targetMergePolicyFloorSegment)
                        .put(MergePolicyConfig.INDEX_MERGE_POLICY_MERGE_FACTOR_SETTING.getKey(), targetMergePolicyFactor)
                );
                updateMergePolicySettingsRequest.masterNodeTimeout(TimeValue.MAX_VALUE);
                affectedIndices.add(index);
                transportActionsDeduplicator.executeOnce(
                    Tuple.tuple(project.id(), updateMergePolicySettingsRequest),
                    new ErrorRecordingActionListener(
                        TransportUpdateSettingsAction.TYPE.name(),
                        project.id(),
                        indexName,
                        errorStore,
                        Strings.format(
                            "Data stream lifecycle encountered an error trying to to update settings [%s] for index [%s]",
                            updateMergePolicySettingsRequest.settings().keySet(),
                            indexName
                        ),
                        signallingErrorRetryInterval
                    ),
                    (req, reqListener) -> updateIndexSetting(project.id(), updateMergePolicySettingsRequest, reqListener)
                );
            } else {
                affectedIndices.add(index);
                ForceMergeRequest forceMergeRequest = new ForceMergeRequest(indexName);
                // time to force merge the index
                transportActionsDeduplicator.executeOnce(
                    Tuple.tuple(project.id(), new ForceMergeRequestWrapper(forceMergeRequest)),
                    new ErrorRecordingActionListener(
                        ForceMergeAction.NAME,
                        project.id(),
                        indexName,
                        errorStore,
                        Strings.format(
                            "Data stream lifecycle encountered an error trying to force merge index [%s]. Data stream lifecycle will "
                                + "attempt to force merge the index on its next run.",
                            indexName
                        ),
                        signallingErrorRetryInterval
                    ),
                    (req, reqListener) -> forceMergeIndex(project.id(), forceMergeRequest, reqListener)
                );
            }
        }
        return affectedIndices;
    }

    private void rolloverDataStream(
        ProjectId projectId,
        String writeIndexName,
        RolloverRequest rolloverRequest,
        ActionListener<Void> listener
    ) {
        // "saving" the rollover target name here so we don't capture the entire request
        ResolvedExpression resolvedRolloverTarget = SelectorResolver.parseExpression(
            rolloverRequest.getRolloverTarget(),
            rolloverRequest.indicesOptions()
        );
        logger.trace("Data stream lifecycle issues rollover request for data stream [{}]", rolloverRequest.getRolloverTarget());
        projectResolver.projectClient(client, projectId).admin().indices().rolloverIndex(rolloverRequest, new ActionListener<>() {
            @Override
            public void onResponse(RolloverResponse rolloverResponse) {
                // Log only when the conditions were met and the index was rolled over.
                if (rolloverResponse.isRolledOver()) {
                    List<String> metConditions = rolloverResponse.getConditionStatus()
                        .entrySet()
                        .stream()
                        .filter(Map.Entry::getValue)
                        .map(Map.Entry::getKey)
                        .toList();
                    logger.info(
                        "Data stream lifecycle successfully rolled over datastream [{}] due to the following met rollover "
                            + "conditions {}. The new index is [{}]",
                        rolloverRequest.getRolloverTarget(),
                        metConditions,
                        rolloverResponse.getNewIndex()
                    );
                }
                listener.onResponse(null);
            }

            @Override
            public void onFailure(Exception e) {
                ProjectMetadata latestProject = clusterService.state().metadata().projects().get(projectId);
                DataStream dataStream = latestProject == null ? null : latestProject.dataStreams().get(resolvedRolloverTarget.resource());
                boolean targetsFailureStore = IndexComponentSelector.FAILURES == resolvedRolloverTarget.selector();
                if (dataStream == null || Objects.equals(getWriteIndexName(dataStream, targetsFailureStore), writeIndexName) == false) {
                    // the data stream has another write index so no point in recording an error for the previous write index we were
                    // attempting to roll over
                    // if there are persistent issues with rolling over this data stream, the next data stream lifecycle run will attempt to
                    // rollover the _current_ write index and the error problem should surface then
                    listener.onResponse(null);
                } else {
                    // the data stream has NOT been rolled over since we issued our rollover request, so let's record the
                    // error against the data stream's write index.
                    listener.onFailure(e);
                }
            }
        });
    }

    @Nullable
    private String getWriteIndexName(DataStream dataStream, boolean failureStore) {
        if (dataStream == null) {
            return null;
        }
        if (failureStore) {
            return dataStream.getWriteFailureIndex() == null ? null : dataStream.getWriteFailureIndex().getName();
        }
        return dataStream.getWriteIndex().getName();
    }

    private void updateIndexSetting(ProjectId projectId, UpdateSettingsRequest updateSettingsRequest, ActionListener<Void> listener) {
        assert updateSettingsRequest.indices() != null && updateSettingsRequest.indices().length == 1
            : "Data stream lifecycle service updates the settings for one index at a time";
        // "saving" the index name here so we don't capture the entire request
        String targetIndex = updateSettingsRequest.indices()[0];
        logger.trace(
            "Data stream lifecycle service issues request to update settings [{}] for index [{}]",
            updateSettingsRequest.settings().keySet(),
            targetIndex
        );
        projectResolver.projectClient(client, projectId).admin().indices().updateSettings(updateSettingsRequest, new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                logger.info(
                    "Data stream lifecycle service successfully updated settings [{}] for index index [{}]",
                    updateSettingsRequest.settings().keySet(),
                    targetIndex
                );
                listener.onResponse(null);
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof IndexNotFoundException) {
                    // index was already deleted, treat this as a success
                    logger.trace("Clearing recorded error for index [{}] because the index was deleted", targetIndex);
                    errorStore.clearRecordedError(projectId, targetIndex);
                    listener.onResponse(null);
                    return;
                }

                listener.onFailure(e);
            }
        });
    }

    private void addIndexBlock(ProjectId projectId, AddIndexBlockRequest addIndexBlockRequest, ActionListener<Void> listener) {
        assert addIndexBlockRequest.indices() != null && addIndexBlockRequest.indices().length == 1
            : "Data stream lifecycle service updates the index block for one index at a time";
        // "saving" the index name here so we don't capture the entire request
        String targetIndex = addIndexBlockRequest.indices()[0];
        logger.trace(
            "Data stream lifecycle service issues request to add block [{}] for index [{}]",
            addIndexBlockRequest.getBlock(),
            targetIndex
        );
        projectResolver.projectClient(client, projectId).admin().indices().addBlock(addIndexBlockRequest, new ActionListener<>() {
            @Override
            public void onResponse(AddIndexBlockResponse addIndexBlockResponse) {
                if (addIndexBlockResponse.isAcknowledged()) {
                    logger.info(
                        "Data stream lifecycle service successfully added block [{}] for index index [{}]",
                        addIndexBlockRequest.getBlock(),
                        targetIndex
                    );
                    listener.onResponse(null);
                } else {
                    Optional<AddIndexBlockResponse.AddBlockResult> resultForTargetIndex = addIndexBlockResponse.getIndices()
                        .stream()
                        .filter(blockResult -> blockResult.getIndex().getName().equals(targetIndex))
                        .findAny();
                    if (resultForTargetIndex.isEmpty()) {
                        // blimey
                        // this is weird, we don't have a result for our index, so let's treat this as a success and the next DSL run will
                        // check if we need to retry adding the block for this index
                        logger.trace(
                            "Data stream lifecycle service received an unacknowledged response when attempting to add the "
                                + "read-only block to index [{}], but the response didn't contain an explicit result for the index.",
                            targetIndex
                        );
                        listener.onFailure(
                            new ElasticsearchException("request to mark index [" + targetIndex + "] as read-only was not acknowledged")
                        );
                    } else if (resultForTargetIndex.get().hasFailures()) {
                        AddIndexBlockResponse.AddBlockResult blockResult = resultForTargetIndex.get();
                        if (blockResult.getException() != null) {
                            listener.onFailure(blockResult.getException());
                        } else {
                            List<AddIndexBlockResponse.AddBlockShardResult.Failure> shardFailures = new ArrayList<>(
                                blockResult.getShards().length
                            );
                            for (AddIndexBlockResponse.AddBlockShardResult shard : blockResult.getShards()) {
                                if (shard.hasFailures()) {
                                    shardFailures.addAll(Arrays.asList(shard.getFailures()));
                                }
                            }
                            assert shardFailures.isEmpty() == false
                                : "The block response must have shard failures as the global "
                                    + "exception is null. The block result is: "
                                    + blockResult;
                            String errorMessage = org.elasticsearch.common.Strings.collectionToDelimitedString(
                                shardFailures.stream().map(org.elasticsearch.common.Strings::toString).collect(Collectors.toList()),
                                ","
                            );
                            listener.onFailure(new ElasticsearchException(errorMessage));
                        }
                    } else {
                        listener.onFailure(
                            new ElasticsearchException("request to mark index [" + targetIndex + "] as read-only was not acknowledged")
                        );
                    }
                }
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof IndexNotFoundException) {
                    // index was already deleted, treat this as a success
                    logger.trace("Clearing recorded error for index [{}] because the index was deleted", targetIndex);
                    errorStore.clearRecordedError(projectId, targetIndex);
                    listener.onResponse(null);
                    return;
                }

                listener.onFailure(e);
            }
        });
    }

    private void deleteIndex(ProjectId projectId, DeleteIndexRequest deleteIndexRequest, String reason, ActionListener<Void> listener) {
        assert deleteIndexRequest.indices() != null && deleteIndexRequest.indices().length == 1
            : "Data stream lifecycle deletes one index at a time";
        // "saving" the index name here so we don't capture the entire request
        String targetIndex = deleteIndexRequest.indices()[0];
        logger.trace("Data stream lifecycle issues request to delete index [{}]", targetIndex);
        projectResolver.projectClient(client, projectId).admin().indices().delete(deleteIndexRequest, new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                if (acknowledgedResponse.isAcknowledged()) {
                    logger.info("Data stream lifecycle successfully deleted index [{}] due to {}", targetIndex, reason);
                } else {
                    logger.trace(
                        "The delete request for index [{}] was not acknowledged. Data stream lifecycle service will retry on the"
                            + " next run if the index still exists",
                        targetIndex
                    );
                }
                listener.onResponse(null);
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof IndexNotFoundException) {
                    logger.trace("Data stream lifecycle did not delete index [{}] as it was already deleted", targetIndex);
                    // index was already deleted, treat this as a success
                    errorStore.clearRecordedError(projectId, targetIndex);
                    listener.onResponse(null);
                    return;
                }

                if (e instanceof SnapshotInProgressException) {
                    logger.info(
                        "Data stream lifecycle was unable to delete index [{}] because it's currently being snapshot. Retrying on "
                            + "the next data stream lifecycle run",
                        targetIndex
                    );
                }
                listener.onFailure(e);
            }
        });
    }

    private void downsampleIndex(ProjectId projectId, DownsampleAction.Request request, ActionListener<Void> listener) {
        String sourceIndex = request.getSourceIndex();
        String downsampleIndex = request.getTargetIndex();
        logger.info("Data stream lifecycle issuing request to downsample index [{}] to index [{}]", sourceIndex, downsampleIndex);
        projectResolver.projectClient(client, projectId).execute(DownsampleAction.INSTANCE, request, new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                assert acknowledgedResponse.isAcknowledged() : "the downsample response is always acknowledged";
                logger.info("Data stream lifecycle successfully downsampled index [{}] to index [{}]", sourceIndex, downsampleIndex);
                listener.onResponse(null);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    /*
     * This method executes the given force merge request. Once the request has completed successfully it writes a timestamp as custom
     * metadata in the cluster state indicating when the force merge has completed. The listener is notified after the cluster state
     * update has been made, or when the forcemerge fails or the write of the to the cluster state fails.
     */
    private void forceMergeIndex(ProjectId projectId, ForceMergeRequest forceMergeRequest, ActionListener<Void> listener) {
        assert forceMergeRequest.indices() != null && forceMergeRequest.indices().length == 1
            : "Data stream lifecycle force merges one index at a time";
        final String targetIndex = forceMergeRequest.indices()[0];
        logger.info("Data stream lifecycle is issuing a request to force merge index [{}]", targetIndex);
        projectResolver.projectClient(client, projectId).admin().indices().forceMerge(forceMergeRequest, new ActionListener<>() {
            @Override
            public void onResponse(BroadcastResponse forceMergeResponse) {
                if (forceMergeResponse.getFailedShards() > 0) {
                    DefaultShardOperationFailedException[] failures = forceMergeResponse.getShardFailures();
                    String message = Strings.format(
                        "Data stream lifecycle failed to forcemerge %d shards for index [%s] due to failures [%s]",
                        forceMergeResponse.getFailedShards(),
                        targetIndex,
                        failures == null
                            ? "unknown"
                            : Arrays.stream(failures).map(DefaultShardOperationFailedException::toString).collect(Collectors.joining(","))
                    );
                    onFailure(new ElasticsearchException(message));
                } else if (forceMergeResponse.getTotalShards() != forceMergeResponse.getSuccessfulShards()) {
                    String message = Strings.format(
                        "Force merge request only had %d successful shards out of a total of %d",
                        forceMergeResponse.getSuccessfulShards(),
                        forceMergeResponse.getTotalShards()
                    );
                    onFailure(new ElasticsearchException(message));
                } else {
                    logger.info("Data stream lifecycle successfully force merged index [{}]", targetIndex);
                    setForceMergeCompletedTimestamp(projectId, targetIndex, listener);
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    /*
     * This method sets the value of the custom index metadata field "force_merge_completed_timestamp" within the field
     * "data_stream_lifecycle" to value. The method returns immediately, but the update happens asynchronously and listener is notified on
     * success or failure.
     */
    private void setForceMergeCompletedTimestamp(ProjectId projectId, String targetIndex, ActionListener<Void> listener) {
        forceMergeClusterStateUpdateTaskQueue.submitTask(
            Strings.format("Adding force merge complete marker to cluster state for [%s]", targetIndex),
            new UpdateForceMergeCompleteTask(listener, projectId, targetIndex, threadPool),
            null
        );
    }

    /*
     * Returns true if a value has been set for the custom index metadata field "force_merge_completed_timestamp" within the field
     * "data_stream_lifecycle".
     */
    private static boolean isForceMergeComplete(IndexMetadata backingIndex) {
        Map<String, String> customMetadata = backingIndex.getCustomData(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY);
        return customMetadata != null && customMetadata.containsKey(FORCE_MERGE_COMPLETED_TIMESTAMP_METADATA_KEY);
    }

    @Nullable
    private static TimeValue getEffectiveRetention(
        DataStream dataStream,
        DataStreamGlobalRetentionSettings globalRetentionSettings,
        boolean failureStore
    ) {
        DataStreamLifecycle lifecycle = failureStore ? dataStream.getFailuresLifecycle() : dataStream.getDataLifecycle();
        return lifecycle == null || lifecycle.enabled() == false
            ? null
            : lifecycle.getEffectiveDataRetention(globalRetentionSettings.get(failureStore), dataStream.isInternal());
    }

    /**
     * @return the duration of the last run in millis or null if the service hasn't completed a run yet.
     */
    @Nullable
    public Long getLastRunDuration() {
        return lastRunDuration;
    }

    /**
     * @return the time passed between the start times of the last two consecutive runs or null if the service hasn't started twice yet.
     */
    @Nullable
    public Long getTimeBetweenStarts() {
        return timeBetweenStarts;
    }

    /**
     * Action listener that records the encountered failure using the provided recordError callback for the
     * provided target index. If the listener is notified of success it will clear the recorded entry for the provided
     * target index using the clearErrorRecord callback.
     */
    static class ErrorRecordingActionListener implements ActionListener<Void> {

        private final String actionName;
        private final ProjectId projectId;
        private final String targetIndex;
        private final DataStreamLifecycleErrorStore errorStore;
        private final String errorLogMessage;
        private final int signallingErrorRetryThreshold;

        ErrorRecordingActionListener(
            String actionName,
            ProjectId projectId,
            String targetIndex,
            DataStreamLifecycleErrorStore errorStore,
            String errorLogMessage,
            int signallingErrorRetryThreshold
        ) {
            this.actionName = actionName;
            this.projectId = projectId;
            this.targetIndex = targetIndex;
            this.errorStore = errorStore;
            this.errorLogMessage = errorLogMessage;
            this.signallingErrorRetryThreshold = signallingErrorRetryThreshold;
        }

        @Override
        public void onResponse(Void unused) {
            logger.trace("Clearing recorded error for index [{}] because the [{}] action was successful", targetIndex, actionName);
            errorStore.clearRecordedError(projectId, targetIndex);
        }

        @Override
        public void onFailure(Exception e) {
            recordAndLogError(projectId, targetIndex, errorStore, e, errorLogMessage, signallingErrorRetryThreshold);
        }
    }

    /**
     * Records the provided error for the index in the error store and logs the error message at `ERROR` level if the error for the index
     * is different to what's already in the error store or if the same error was in the error store for a number of retries divible by
     * the provided signallingErrorRetryThreshold (i.e. we log to level `error` every signallingErrorRetryThreshold retries, if the error
     * stays the same)
     * This allows us to not spam the logs, but signal to the logs if DSL is not making progress.
     */
    static void recordAndLogError(
        ProjectId projectId,
        String targetIndex,
        DataStreamLifecycleErrorStore errorStore,
        Exception e,
        String logMessage,
        int signallingErrorRetryThreshold
    ) {
        ErrorEntry previousError = errorStore.recordError(projectId, targetIndex, e);
        ErrorEntry currentError = errorStore.getError(projectId, targetIndex);
        if (previousError == null || (currentError != null && previousError.error().equals(currentError.error()) == false)) {
            logger.error(logMessage, e);
        } else {
            if (currentError != null) {
                if (currentError.retryCount() % signallingErrorRetryThreshold == 0) {
                    logger.error(
                        String.format(
                            Locale.ROOT,
                            "%s\nFailing since [%d], operation retried [%d] times",
                            logMessage,
                            currentError.firstOccurrenceTimestamp(),
                            currentError.retryCount()
                        ),
                        e
                    );
                } else {
                    logger.trace(
                        String.format(
                            Locale.ROOT,
                            "%s\nFailing since [%d], operation retried [%d] times",
                            logMessage,
                            currentError.firstOccurrenceTimestamp(),
                            currentError.retryCount()
                        ),
                        e
                    );
                }
            } else {
                logger.trace(
                    String.format(
                        Locale.ROOT,
                        "Index [%s] encountered error [%s] but there's no record in the error store anymore",
                        targetIndex,
                        logMessage
                    ),
                    e
                );
            }
        }
    }

    static RolloverRequest getDefaultRolloverRequest(
        RolloverConfiguration rolloverConfiguration,
        String dataStream,
        TimeValue dataRetention,
        boolean rolloverFailureStore
    ) {
        var rolloverTarget = rolloverFailureStore
            ? IndexNameExpressionResolver.combineSelector(dataStream, IndexComponentSelector.FAILURES)
            : dataStream;
        RolloverRequest rolloverRequest = new RolloverRequest(rolloverTarget, null).masterNodeTimeout(TimeValue.MAX_VALUE);
        rolloverRequest.setConditions(rolloverConfiguration.resolveRolloverConditions(dataRetention));
        return rolloverRequest;
    }

    private void updatePollInterval(TimeValue newInterval) {
        this.pollInterval = newInterval;
        maybeScheduleJob();
    }

    private void updateRolloverConfiguration(RolloverConfiguration newRolloverConfiguration) {
        this.rolloverConfiguration = newRolloverConfiguration;
    }

    private void updateMergePolicyFloorSegment(ByteSizeValue newFloorSegment) {
        this.targetMergePolicyFloorSegment = newFloorSegment;
    }

    private void updateMergePolicyFactor(int newFactor) {
        this.targetMergePolicyFactor = newFactor;
    }

    public void updateSignallingRetryThreshold(int retryThreshold) {
        this.signallingErrorRetryInterval = retryThreshold;
    }

    private void cancelJob() {
        if (scheduler.get() != null) {
            scheduler.get().remove(LIFECYCLE_JOB_NAME);
            scheduledJob = null;
        }
    }

    private boolean isClusterServiceStoppedOrClosed() {
        final Lifecycle.State state = clusterService.lifecycleState();
        return state == Lifecycle.State.STOPPED || state == Lifecycle.State.CLOSED;
    }

    private void maybeScheduleJob() {
        if (this.isMaster == false) {
            return;
        }

        // don't schedule the job if the node is shutting down
        if (isClusterServiceStoppedOrClosed()) {
            logger.trace(
                "Skipping scheduling a data stream lifecycle job due to the cluster lifecycle state being: [{}] ",
                clusterService.lifecycleState()
            );
            return;
        }

        if (scheduler.get() == null) {
            scheduler.set(new SchedulerEngine(settings, clock));
            scheduler.get().register(this);
        }

        assert scheduler.get() != null : "scheduler should be available";
        scheduledJob = new SchedulerEngine.Job(LIFECYCLE_JOB_NAME, new TimeValueSchedule(pollInterval));
        scheduler.get().add(scheduledJob);
    }

    // public visibility for testing
    public DataStreamLifecycleErrorStore getErrorStore() {
        return errorStore;
    }

    // visible for testing
    public void setNowSupplier(LongSupplier nowSupplier) {
        this.nowSupplier = nowSupplier;
    }

    /**
     * This is a ClusterStateTaskListener that writes the force_merge_completed_timestamp into the cluster state. It is meant to run in
     * STATE_UPDATE_TASK_EXECUTOR.
     */
    static class UpdateForceMergeCompleteTask implements ClusterStateTaskListener {
        private final ActionListener<Void> listener;
        private final ProjectId projectId;
        private final String targetIndex;
        private final ThreadPool threadPool;

        UpdateForceMergeCompleteTask(ActionListener<Void> listener, ProjectId projectId, String targetIndex, ThreadPool threadPool) {
            this.listener = listener;
            this.projectId = projectId;
            this.targetIndex = targetIndex;
            this.threadPool = threadPool;
        }

        ClusterState execute(ClusterState currentState) throws Exception {
            logger.debug("Updating cluster state with force merge complete marker for {}", targetIndex);
            final var currentProject = currentState.metadata().getProject(projectId);
            IndexMetadata indexMetadata = currentProject.index(targetIndex);
            Map<String, String> customMetadata = indexMetadata.getCustomData(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY);
            Map<String, String> newCustomMetadata = new HashMap<>();
            if (customMetadata != null) {
                newCustomMetadata.putAll(customMetadata);
            }
            newCustomMetadata.put(FORCE_MERGE_COMPLETED_TIMESTAMP_METADATA_KEY, Long.toString(threadPool.absoluteTimeInMillis()));
            IndexMetadata updatededIndexMetadata = new IndexMetadata.Builder(indexMetadata).putCustom(
                LIFECYCLE_CUSTOM_INDEX_METADATA_KEY,
                newCustomMetadata
            ).build();
            final var updatedProject = ProjectMetadata.builder(currentProject).put(updatededIndexMetadata, true);
            return ClusterState.builder(currentState).putProjectMetadata(updatedProject).build();
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * This wrapper exists only to provide equals and hashCode implementations of a ForceMergeRequest for transportActionsDeduplicator.
     * It intentionally ignores forceMergeUUID (which ForceMergeRequest's equals/hashCode would have to if they existed) because we don't
     * care about it for data stream lifecycle deduplication. This class is non-private for the sake of unit testing, but should not be used
     * outside of Data Stream Lifecycle Service.
     */
    static final class ForceMergeRequestWrapper extends ForceMergeRequest {
        ForceMergeRequestWrapper(ForceMergeRequest original) {
            super(original.indices());
            this.maxNumSegments(original.maxNumSegments());
            this.onlyExpungeDeletes(original.onlyExpungeDeletes());
            this.flush(original.flush());
            this.indicesOptions(original.indicesOptions());
            this.setShouldStoreResult(original.getShouldStoreResult());
            this.setRequestId(original.getRequestId());
            this.timeout(original.timeout());
            this.setParentTask(original.getParentTask());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ForceMergeRequest that = (ForceMergeRequest) o;
            return Arrays.equals(indices, that.indices())
                && maxNumSegments() == that.maxNumSegments()
                && onlyExpungeDeletes() == that.onlyExpungeDeletes()
                && flush() == that.flush()
                && Objects.equals(indicesOptions(), that.indicesOptions())
                && getShouldStoreResult() == that.getShouldStoreResult()
                && getRequestId() == that.getRequestId()
                && Objects.equals(timeout(), that.timeout())
                && Objects.equals(getParentTask(), that.getParentTask());
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                Arrays.hashCode(indices),
                maxNumSegments(),
                onlyExpungeDeletes(),
                flush(),
                indicesOptions(),
                getShouldStoreResult(),
                getRequestId(),
                timeout(),
                getParentTask()
            );
        }
    }
}
