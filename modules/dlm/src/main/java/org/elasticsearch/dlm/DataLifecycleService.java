/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.dlm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ResultDeduplicator;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverConfiguration;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.metadata.DataLifecycle;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.scheduler.SchedulerEngine;
import org.elasticsearch.common.scheduler.TimeValueSchedule;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.snapshots.SnapshotInProgressException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;

import java.io.Closeable;
import java.time.Clock;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * This service will implement the needed actions (e.g. rollover, retention) to manage the data streams with a DLM lifecycle configured.
 * It runs on the master node and it schedules a job according to the configured
 * {@link DataLifecycleService#DATA_STREAM_LIFECYCLE_POLL_INTERVAL_SETTING}.
 */
public class DataLifecycleService implements ClusterStateListener, Closeable, SchedulerEngine.Listener {

    public static final String DATA_STREAM_LIFECYCLE_POLL_INTERVAL = "data_streams.lifecycle.poll_interval";
    public static final Setting<TimeValue> DATA_STREAM_LIFECYCLE_POLL_INTERVAL_SETTING = Setting.timeSetting(
        DATA_STREAM_LIFECYCLE_POLL_INTERVAL,
        TimeValue.timeValueMinutes(10),
        TimeValue.timeValueSeconds(1),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(DataLifecycleService.class);
    /**
     * Name constant for the job DLM schedules
     */
    private static final String DATA_LIFECYCLE_JOB_NAME = "data_stream_lifecycle";
    /*
     * This is the key for DLM-related custom index metadata.
     */
    static final String DLM_CUSTOM_INDEX_METADATA_KEY = "data_stream_lifecycle";
    static final String FORCE_MERGE_COMPLETED_TIMESTAMP_METADATA_KEY = "force_merge_completed_timestamp";

    private final Settings settings;
    private final Client client;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    final ResultDeduplicator<TransportRequest, Void> transportActionsDeduplicator;
    private final LongSupplier nowSupplier;
    private final Clock clock;
    private final DataLifecycleErrorStore errorStore;
    private volatile boolean isMaster = false;
    private volatile TimeValue pollInterval;
    private volatile RolloverConfiguration rolloverConfiguration;
    private SchedulerEngine.Job scheduledJob;
    private final SetOnce<SchedulerEngine> scheduler = new SetOnce<>();
    private final MasterServiceTaskQueue<UpdateForceMergeCompleteTask> forceMergeClusterStateUpdateTaskQueue;

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

    public DataLifecycleService(
        Settings settings,
        Client client,
        ClusterService clusterService,
        Clock clock,
        ThreadPool threadPool,
        LongSupplier nowSupplier,
        DataLifecycleErrorStore errorStore
    ) {
        this.settings = settings;
        this.client = client;
        this.clusterService = clusterService;
        this.clock = clock;
        this.threadPool = threadPool;
        this.transportActionsDeduplicator = new ResultDeduplicator<>(threadPool.getThreadContext());
        this.nowSupplier = nowSupplier;
        this.errorStore = errorStore;
        this.scheduledJob = null;
        this.pollInterval = DATA_STREAM_LIFECYCLE_POLL_INTERVAL_SETTING.get(settings);
        this.rolloverConfiguration = clusterService.getClusterSettings().get(DataLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING);
        this.forceMergeClusterStateUpdateTaskQueue = clusterService.createTaskQueue(
            "dlm-forcemerge-state-update",
            Priority.LOW,
            FORCE_MERGE_STATE_UPDATE_TASK_EXECUTOR
        );
    }

    /**
     * Initializer method to avoid the publication of a self reference in the constructor.
     */
    public void init() {
        clusterService.addListener(this);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(DATA_STREAM_LIFECYCLE_POLL_INTERVAL_SETTING, this::updatePollInterval);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(DataLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING, this::updateRolloverConfiguration);
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
        errorStore.clearStore();
    }

    @Override
    public void triggered(SchedulerEngine.Event event) {
        if (event.getJobName().equals(DATA_LIFECYCLE_JOB_NAME)) {
            if (this.isMaster) {
                logger.trace("DLM job triggered: {}, {}, {}", event.getJobName(), event.getScheduledTime(), event.getTriggeredTime());
                run(clusterService.state());
            }
        }
    }

    /**
     * Iterates over the DLM managed data streams and executes the needed operations
     * to satisfy the configured {@link org.elasticsearch.cluster.metadata.DataLifecycle}.
     */
    // default visibility for testing purposes
    void run(ClusterState state) {
        for (DataStream dataStream : state.metadata().dataStreams().values()) {
            clearErrorStoreForUnmanagedIndices(dataStream);
            if (dataStream.getLifecycle() == null) {
                continue;
            }

            /*
             * This is the pre-rollover write index. It may or may not be the write index after maybeExecuteRollover has executed, depending
             * on rollover criteria. We're keeping a reference to it because regardless of whether it's rolled over or not we want to
             * exclude it from force merging later in this DLM run.
             */
            Index originalWriteIndex = dataStream.getWriteIndex();
            try {
                maybeExecuteRollover(state, dataStream);
            } catch (Exception e) {
                logger.error(() -> String.format(Locale.ROOT, "DLM failed to rollver data stream [%s]", dataStream.getName()), e);
                DataStream latestDataStream = clusterService.state().metadata().dataStreams().get(dataStream.getName());
                if (latestDataStream != null) {
                    if (latestDataStream.getWriteIndex().getName().equals(originalWriteIndex.getName())) {
                        // data stream has not been rolled over in the meantime so record the error against the write index we
                        // attempted the rollover
                        errorStore.recordError(originalWriteIndex.getName(), e);
                    }
                }
            }
            Set<Index> indicesBeingRemoved;
            try {
                indicesBeingRemoved = maybeExecuteRetention(state, dataStream);
            } catch (Exception e) {
                indicesBeingRemoved = Set.of();
                // individual index errors would be reported via the API action listener for every delete call
                // we could potentially record errors at a data stream level and expose it via the _data_stream API?
                logger.error(
                    () -> String.format(Locale.ROOT, "DLM failed to execute retention for data stream [%s]", dataStream.getName()),
                    e
                );
            }

            try {
                /*
                 * When considering indices for force merge, we want to exclude several indices: (1) We exclude the current write index
                 * because obviously it is still likely to get writes, (2) we exclude the most recent previous write index because since
                 * we just switched over it might still be getting some writes, and (3) we exclude any indices that we're in the process
                 * of deleting because they'll be gone soon anyway.
                 */
                Set<Index> indicesToExclude = new HashSet<>();
                Index currentWriteIndex = dataStream.getWriteIndex();
                indicesToExclude.add(currentWriteIndex);
                indicesToExclude.add(originalWriteIndex); // Could be the same as currentWriteIndex, but that's fine
                indicesToExclude.addAll(indicesBeingRemoved);
                List<Index> potentialForceMergeIndices = dataStream.getIndices()
                    .stream()
                    .filter(index -> indicesToExclude.contains(index) == false)
                    .toList();
                maybeExecuteForceMerge(state, dataStream, potentialForceMergeIndices);
            } catch (Exception e) {
                logger.error(
                    () -> String.format(Locale.ROOT, "DLM failed to execute force merge for data stream [%s]", dataStream.getName()),
                    e
                );
            }
        }
    }

    /**
     * This clears the error store for the case where a data stream or some backing indices were managed by DLM, failed in their
     * lifecycle execution, and then they were not managed by DLM (maybe they were switched to ILM).
     */
    private void clearErrorStoreForUnmanagedIndices(DataStream dataStream) {
        Metadata metadata = clusterService.state().metadata();
        for (String indexName : errorStore.getAllIndices()) {
            IndexMetadata indexMeta = metadata.index(indexName);
            if (indexMeta == null) {
                errorStore.clearRecordedError(indexName);
            } else if (dataStream.isIndexManagedByDLM(indexMeta.getIndex(), metadata::index) == false) {
                errorStore.clearRecordedError(indexName);
            }
        }
    }

    private void maybeExecuteRollover(ClusterState state, DataStream dataStream) {
        Index writeIndex = dataStream.getWriteIndex();
        if (dataStream.isIndexManagedByDLM(writeIndex, state.metadata()::index)) {
            RolloverRequest rolloverRequest = getDefaultRolloverRequest(
                rolloverConfiguration,
                dataStream.getName(),
                dataStream.getLifecycle().getEffectiveDataRetention()
            );
            transportActionsDeduplicator.executeOnce(
                rolloverRequest,
                new ErrorRecordingActionListener(writeIndex.getName(), errorStore),
                (req, reqListener) -> rolloverDataStream(writeIndex.getName(), rolloverRequest, reqListener)
            );
        }
    }

    /**
     * This method sends requests to delete any indices in the datastream that exceed its retention policy. It returns the set of indices
     * it has sent delete requests for.
     * @param state The cluster state from which to get index metadata
     * @param dataStream The datastream
     * @return The set of indices that delete requests have been sent for
     */
    private Set<Index> maybeExecuteRetention(ClusterState state, DataStream dataStream) {
        TimeValue retention = getRetentionConfiguration(dataStream);
        Set<Index> indicesToBeRemoved = new HashSet<>();
        if (retention != null) {
            Metadata metadata = state.metadata();
            List<Index> backingIndicesOlderThanRetention = dataStream.getIndicesPastRetention(metadata::index, nowSupplier);

            for (Index index : backingIndicesOlderThanRetention) {
                indicesToBeRemoved.add(index);
                IndexMetadata backingIndex = metadata.index(index);
                assert backingIndex != null : "the data stream backing indices must exist";

                // there's an opportunity here to batch the delete requests (i.e. delete 100 indices / request)
                // let's start simple and reevaluate
                String indexName = backingIndex.getIndex().getName();
                DeleteIndexRequest deleteRequest = new DeleteIndexRequest(indexName).masterNodeTimeout(TimeValue.MAX_VALUE);

                // time to delete the index
                transportActionsDeduplicator.executeOnce(
                    deleteRequest,
                    new ErrorRecordingActionListener(indexName, errorStore),
                    (req, reqListener) -> deleteIndex(deleteRequest, retention, reqListener)
                );
            }
        }
        return indicesToBeRemoved;
    }

    /*
     * This method force merges the given indices in the datastream. It writes a timestamp in the cluster state upon completion of the
     * force merge.
     */
    private void maybeExecuteForceMerge(ClusterState state, DataStream dataStream, List<Index> indices) {
        Metadata metadata = state.metadata();
        for (Index index : indices) {
            if (dataStream.isIndexManagedByDLM(index, state.metadata()::index)) {
                IndexMetadata backingIndex = metadata.index(index);
                assert backingIndex != null : "the data stream backing indices must exist";
                String indexName = index.getName();
                boolean alreadyForceMerged = isForceMergeComplete(backingIndex);
                if (alreadyForceMerged) {
                    logger.trace("Already force merged {}", indexName);
                    continue;
                }
                ForceMergeRequest forceMergeRequest = new ForceMergeRequest(indexName);
                // time to force merge the index
                transportActionsDeduplicator.executeOnce(
                    new ForceMergeRequestWrapper(forceMergeRequest),
                    new ErrorRecordingActionListener(indexName, errorStore),
                    (req, reqListener) -> forceMergeIndex(forceMergeRequest, reqListener)
                );
            }
        }
    }

    private void rolloverDataStream(String writeIndexName, RolloverRequest rolloverRequest, ActionListener<Void> listener) {
        // "saving" the rollover target name here so we don't capture the entire request
        String rolloverTarget = rolloverRequest.getRolloverTarget();
        logger.trace("DLM issues rollover request for data stream [{}]", rolloverTarget);
        client.admin().indices().rolloverIndex(rolloverRequest, new ActionListener<>() {
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
                        "DLM successfully rolled over datastream [{}] due to the following met rollover conditions {}. The new index is "
                            + "[{}]",
                        rolloverTarget,
                        metConditions,
                        rolloverResponse.getNewIndex()
                    );
                }
                listener.onResponse(null);
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(() -> Strings.format("DLM encountered an error trying to rollover data steam [%s]", rolloverTarget), e);
                DataStream dataStream = clusterService.state().metadata().dataStreams().get(rolloverTarget);
                if (dataStream == null || dataStream.getWriteIndex().getName().equals(writeIndexName) == false) {
                    // the data stream has another write index so no point in recording an error for the previous write index we were
                    // attempting to rollover
                    // if there are persistent issues with rolling over this data stream, the next DLM run will attempt to rollover the
                    // _current_ write index and the error problem should surface then
                    listener.onResponse(null);
                } else {
                    // the data stream has NOT been rolled over since we issued our rollover request, so let's record the
                    // error against the data stream's write index.
                    listener.onFailure(e);
                }
            }
        });
    }

    private void deleteIndex(DeleteIndexRequest deleteIndexRequest, TimeValue retention, ActionListener<Void> listener) {
        assert deleteIndexRequest.indices() != null && deleteIndexRequest.indices().length == 1 : "DLM deletes one index at a time";
        // "saving" the index name here so we don't capture the entire request
        String targetIndex = deleteIndexRequest.indices()[0];
        logger.trace("DLM issues request to delete index [{}]", targetIndex);
        client.admin().indices().delete(deleteIndexRequest, new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                logger.info("DLM successfully deleted index [{}] due to the lapsed [{}] retention period", targetIndex, retention);
                listener.onResponse(null);
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof IndexNotFoundException) {
                    // index was already deleted, treat this as a success
                    errorStore.clearRecordedError(targetIndex);
                    listener.onResponse(null);
                    return;
                }

                if (e instanceof SnapshotInProgressException) {
                    logger.info(
                        "DLM was unable to delete index [{}] because it's currently being snapshotted. Retrying on the next DLM run",
                        targetIndex
                    );
                } else {
                    logger.error(() -> Strings.format("DLM encountered an error trying to delete index [%s]", targetIndex), e);
                }
                listener.onFailure(e);
            }
        });
    }

    /*
     * This method executes the given force merge request. Once the request has completed successfully it writes a timestamp as custom
     * metadata in the cluster state indicating when the force merge has completed. The listener is notified after the cluster state
     * update has been made, or when the forcemerge fails or the write of the to the cluster state fails.
     */
    private void forceMergeIndex(ForceMergeRequest forceMergeRequest, ActionListener<Void> listener) {
        assert forceMergeRequest.indices() != null && forceMergeRequest.indices().length == 1 : "DLM force merges one index at a time";
        final String targetIndex = forceMergeRequest.indices()[0];
        logger.info("DLM is issuing a request to force merge index [{}]", targetIndex);
        client.admin().indices().forceMerge(forceMergeRequest, new ActionListener<>() {
            @Override
            public void onResponse(ForceMergeResponse forceMergeResponse) {
                if (forceMergeResponse.getFailedShards() > 0) {
                    DefaultShardOperationFailedException[] failures = forceMergeResponse.getShardFailures();
                    String message = Strings.format(
                        "DLM failed to forcemerge %d shards for index [%s] due to failures [%s]",
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
                    logger.info("DLM successfully force merged index [{}]", targetIndex);
                    setForceMergeCompletedTimestamp(targetIndex, listener);
                }
            }

            @Override
            public void onFailure(Exception e) {
                String previousError = errorStore.getError(targetIndex);
                /*
                 * Note that this call to onFailure has to happen before the logging because we choose whether to log or not based on a
                 * side effect of the onFailure call (it updates the error in the errorStore).
                 */
                listener.onFailure(e);
                // To avoid spamming our logs, we only want to log the error once.
                if (previousError == null || previousError.equals(errorStore.getError(targetIndex)) == false) {
                    logger.warn(
                        () -> Strings.format(
                            "DLM encountered an error trying to force merge index [%s]. DLM will attempt to force merge the index on its "
                                + "next run.",
                            targetIndex
                        ),
                        e
                    );
                }
            }
        });
    }

    /*
     * This method sets the value of the custom index metadata field "force_merge_completed_timestamp" within the field
     * "data_stream_lifecycle" to value. The method returns immediately, but the update happens asynchronously and listener is notified on
     * success or failure.
     */
    private void setForceMergeCompletedTimestamp(String targetIndex, ActionListener<Void> listener) {
        forceMergeClusterStateUpdateTaskQueue.submitTask(
            Strings.format("Adding force merge complete marker to cluster state for [%s]", targetIndex),
            new UpdateForceMergeCompleteTask(listener, targetIndex, threadPool),
            null
        );
    }

    /*
     * Returns true if a value has been set for the custom index metadata field "force_merge_completed_timestamp" within the field
     * "data_stream_lifecycle".
     */
    private boolean isForceMergeComplete(IndexMetadata backingIndex) {
        Map<String, String> customMetadata = backingIndex.getCustomData(DLM_CUSTOM_INDEX_METADATA_KEY);
        return customMetadata != null && customMetadata.containsKey(FORCE_MERGE_COMPLETED_TIMESTAMP_METADATA_KEY);
    }

    @Nullable
    static TimeValue getRetentionConfiguration(DataStream dataStream) {
        if (dataStream.getLifecycle() == null) {
            return null;
        }
        return dataStream.getLifecycle().getEffectiveDataRetention();
    }

    /**
     * Action listener that records the encountered failure using the provided recordError callback for the
     * provided target index. If the listener is notified of success it will clear the recorded entry for the provided
     * target index using the clearErrorRecord callback.
     */
    static class ErrorRecordingActionListener implements ActionListener<Void> {
        private final String targetIndex;
        private final DataLifecycleErrorStore errorStore;

        ErrorRecordingActionListener(String targetIndex, DataLifecycleErrorStore errorStore) {
            this.targetIndex = targetIndex;
            this.errorStore = errorStore;
        }

        @Override
        public void onResponse(Void unused) {
            errorStore.clearRecordedError(targetIndex);
        }

        @Override
        public void onFailure(Exception e) {
            errorStore.recordError(targetIndex, e);
        }
    }

    static RolloverRequest getDefaultRolloverRequest(
        RolloverConfiguration rolloverConfiguration,
        String dataStream,
        TimeValue dataRetention
    ) {
        RolloverRequest rolloverRequest = new RolloverRequest(dataStream, null).masterNodeTimeout(TimeValue.MAX_VALUE);
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

    private void cancelJob() {
        if (scheduler.get() != null) {
            scheduler.get().remove(DATA_LIFECYCLE_JOB_NAME);
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
            logger.trace("Skipping scheduling a DLM job due to the cluster lifecycle state being: [{}] ", clusterService.lifecycleState());
            return;
        }

        if (scheduler.get() == null) {
            scheduler.set(new SchedulerEngine(settings, clock));
            scheduler.get().register(this);
        }

        assert scheduler.get() != null : "scheduler should be available";
        scheduledJob = new SchedulerEngine.Job(DATA_LIFECYCLE_JOB_NAME, new TimeValueSchedule(pollInterval));
        scheduler.get().add(scheduledJob);
    }

    // public visibility for testing
    public DataLifecycleErrorStore getErrorStore() {
        return errorStore;
    }

    /**
     * This is a ClusterStateTaskListener that writes the force_merge_completed_timestamp into the cluster state. It is meant to run in
     * STATE_UPDATE_TASK_EXECUTOR.
     */
    static class UpdateForceMergeCompleteTask implements ClusterStateTaskListener {
        private final ActionListener<Void> listener;
        private final String targetIndex;
        private final ThreadPool threadPool;

        UpdateForceMergeCompleteTask(ActionListener<Void> listener, String targetIndex, ThreadPool threadPool) {
            this.listener = listener;
            this.targetIndex = targetIndex;
            this.threadPool = threadPool;
        }

        ClusterState execute(ClusterState currentState) throws Exception {
            logger.debug("Updating cluster state with force merge complete marker for {}", targetIndex);
            IndexMetadata indexMetadata = currentState.metadata().index(targetIndex);
            Map<String, String> customMetadata = indexMetadata.getCustomData(DLM_CUSTOM_INDEX_METADATA_KEY);
            Map<String, String> newCustomMetadata = new HashMap<>();
            if (customMetadata != null) {
                newCustomMetadata.putAll(customMetadata);
            }
            newCustomMetadata.put(FORCE_MERGE_COMPLETED_TIMESTAMP_METADATA_KEY, Long.toString(threadPool.absoluteTimeInMillis()));
            IndexMetadata updatededIndexMetadata = new IndexMetadata.Builder(indexMetadata).putCustom(
                DLM_CUSTOM_INDEX_METADATA_KEY,
                newCustomMetadata
            ).build();
            Metadata metadata = Metadata.builder(currentState.metadata()).put(updatededIndexMetadata, true).build();
            return ClusterState.builder(currentState).metadata(metadata).build();
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * This wrapper exists only to provide equals and hashCode implementations of a ForceMergeRequest for transportActionsDeduplicator.
     * It intentionally ignores forceMergeUUID (which ForceMergeRequest's equals/hashCode would have to if they existed) because we don't
     * care about it for DLM deduplication. This class is non-private for the sake of unit testing, but should not be used outside of
     * DataLifecycleService.
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
