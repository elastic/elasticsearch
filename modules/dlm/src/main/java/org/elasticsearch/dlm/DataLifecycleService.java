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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ResultDeduplicator;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverConditions;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.DataLifecycle;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.scheduler.SchedulerEngine;
import org.elasticsearch.common.scheduler.TimeValueSchedule;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;

import java.io.Closeable;
import java.time.Clock;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * This service will implement the needed actions (e.g. rollover, retention) to manage the data streams with a DLM lifecycle configured.
 * It runs on the master node and it schedules a job according to the configured {@link DataLifecycleService#DLM_POLL_INTERVAL_SETTING}.
 */
public class DataLifecycleService implements ClusterStateListener, Closeable, SchedulerEngine.Listener {

    public static final String DLM_POLL_INTERVAL = "indices.dlm.poll_interval";
    public static final Setting<TimeValue> DLM_POLL_INTERVAL_SETTING = Setting.timeSetting(
        DLM_POLL_INTERVAL,
        TimeValue.timeValueMinutes(10),
        TimeValue.timeValueSeconds(1),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    private static final Logger logger = LogManager.getLogger(DataLifecycleService.class);
    /**
     * Name constant for the job DLM schedules
     */
    private static final String DATA_LIFECYCLE_JOB_NAME = "dlm";

    private final Settings settings;
    private final Client client;
    private final ClusterService clusterService;
    private final ResultDeduplicator<TransportRequest, Void> transportActionsDeduplicator;
    private final LongSupplier nowSupplier;
    private final Clock clock;
    private volatile boolean isMaster = false;
    private volatile TimeValue pollInterval;
    private volatile RolloverConditions rolloverConditions;
    private SchedulerEngine.Job scheduledJob;
    private final SetOnce<SchedulerEngine> scheduler = new SetOnce<>();

    public DataLifecycleService(
        Settings settings,
        Client client,
        ClusterService clusterService,
        Clock clock,
        ThreadPool threadPool,
        LongSupplier nowSupplier
    ) {
        this.settings = settings;
        this.client = client;
        this.clusterService = clusterService;
        this.clock = clock;
        this.transportActionsDeduplicator = new ResultDeduplicator<>(threadPool.getThreadContext());
        this.nowSupplier = nowSupplier;
        this.scheduledJob = null;
        this.pollInterval = DLM_POLL_INTERVAL_SETTING.get(settings);
        this.rolloverConditions = clusterService.getClusterSettings().get(DataLifecycle.CLUSTER_DLM_DEFAULT_ROLLOVER_SETTING);
    }

    /**
     * Initializer method to avoid the publication of a self reference in the constructor.
     */
    public void init() {
        clusterService.addListener(this);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(DLM_POLL_INTERVAL_SETTING, this::updatePollInterval);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(DataLifecycle.CLUSTER_DLM_DEFAULT_ROLLOVER_SETTING, this::updateRolloverConditions);
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
            }
        }
    }

    @Override
    public void close() {
        SchedulerEngine engine = scheduler.get();
        if (engine != null) {
            engine.stop();
        }
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
            if (dataStream.getLifecycle() == null) {
                continue;
            }

            try {
                maybeExecuteRollover(state, dataStream);
            } catch (Exception e) {
                logger.error(() -> String.format(Locale.ROOT, "DLM failed to rollver data stream [%s]", dataStream.getName()), e);
            }

            try {
                maybeExecuteRetention(state, dataStream);
            } catch (Exception e) {
                logger.error(
                    () -> String.format(Locale.ROOT, "DLM failed to execute retention for data stream [%s]", dataStream.getName()),
                    e
                );
            }
        }
    }

    private void maybeExecuteRollover(ClusterState state, DataStream dataStream) {
        if (dataStream.isIndexManagedByDLM(dataStream.getWriteIndex(), state.metadata()::index)) {
            RolloverRequest rolloverRequest = getDefaultRolloverRequest(dataStream.getName());
            transportActionsDeduplicator.executeOnce(
                rolloverRequest,
                ActionListener.noop(),
                (req, reqListener) -> rolloverDataStream(rolloverRequest, reqListener)
            );
        }
    }

    private void maybeExecuteRetention(ClusterState state, DataStream dataStream) {
        TimeValue retention = getRetentionConfiguration(dataStream);
        if (retention != null) {
            Metadata metadata = state.metadata();
            List<Index> backingIndicesOlderThanRetention = dataStream.getIndicesPastRetention(metadata::index, nowSupplier);

            for (Index index : backingIndicesOlderThanRetention) {
                IndexMetadata backingIndex = metadata.index(index);
                assert backingIndex != null : "the data stream backing indices must exist";

                // there's an opportunity here to batch the delete requests (i.e. delete 100 indices / request)
                // let's start simple and reevaluate
                DeleteIndexRequest deleteRequest = new DeleteIndexRequest(backingIndex.getIndex().getName()).masterNodeTimeout(
                    TimeValue.MAX_VALUE
                );

                // time to delete the index
                transportActionsDeduplicator.executeOnce(
                    deleteRequest,
                    ActionListener.noop(),
                    (req, reqListener) -> deleteIndex(deleteRequest, retention, reqListener)
                );
            }
        }
    }

    private void rolloverDataStream(RolloverRequest rolloverRequest, ActionListener<Void> listener) {
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
                logger.error(() -> Strings.format("DLM rollover of [%s] failed", rolloverTarget), e);
                listener.onFailure(e);
            }
        });
    }

    private void deleteIndex(DeleteIndexRequest deleteIndexRequest, TimeValue retention, ActionListener<Void> listener) {
        assert deleteIndexRequest.indices() != null && deleteIndexRequest.indices().length == 1 : "DLM deletes one index at a time";
        // "saving" the index name here so we don't capture the entire request
        String targetIndex = deleteIndexRequest.indices()[0];
        logger.trace("DLM issue delete request for index [{}]", targetIndex);
        client.admin().indices().delete(deleteIndexRequest, new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                logger.info("DLM successfully deleted index [{}] due to the lapsed [{}] retention period", targetIndex, retention);
                listener.onResponse(null);
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(() -> Strings.format("DLM request to delete [%s] failed", targetIndex), e);
                listener.onFailure(e);
            }
        });
    }

    @Nullable
    static TimeValue getRetentionConfiguration(DataStream dataStream) {
        if (dataStream.getLifecycle() == null) {
            return null;
        }
        return dataStream.getLifecycle().getDataRetention();
    }

    /**
     * Calculate the age of the index since creation or rollover time if the index was already rolled.
     * The rollover target is the data stream name the index is a part of.
     */
    static TimeValue getCreationOrRolloverDate(String rolloverTarget, IndexMetadata index) {
        RolloverInfo rolloverInfo = index.getRolloverInfos().get(rolloverTarget);
        if (rolloverInfo != null) {
            return TimeValue.timeValueMillis(rolloverInfo.getTime());
        } else {
            return TimeValue.timeValueMillis(index.getCreationDate());
        }
    }

    private RolloverRequest getDefaultRolloverRequest(String dataStream) {
        RolloverRequest rolloverRequest = new RolloverRequest(dataStream, null).masterNodeTimeout(TimeValue.MAX_VALUE);
        rolloverRequest.setConditions(rolloverConditions);
        return rolloverRequest;
    }

    private void updatePollInterval(TimeValue newInterval) {
        this.pollInterval = newInterval;
        maybeScheduleJob();
    }

    private void updateRolloverConditions(RolloverConditions newRolloverConditions) {
        this.rolloverConditions = newRolloverConditions;
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
}
