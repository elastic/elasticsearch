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
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.scheduler.SchedulerEngine;
import org.elasticsearch.common.scheduler.TimeValueSchedule;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
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
import java.util.function.Function;
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
    private SchedulerEngine.Job scheduledJob;
    private final SetOnce<SchedulerEngine> scheduler = new SetOnce<>();
    // we use this rollover supplier to facilitate testing until we'll be able to read the
    // rollover configuration from a cluster setting
    private Function<String, RolloverRequest> defaultRolloverRequestSupplier;

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
        this.defaultRolloverRequestSupplier = this::getDefaultRolloverRequest;
    }

    /**
     * Initializer method to avoid the publication of a self reference in the constructor.
     */
    public void init() {
        clusterService.addListener(this);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(DLM_POLL_INTERVAL_SETTING, this::updatePollInterval);
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
        IndexMetadata writeIndex = state.metadata().index(dataStream.getWriteIndex());
        if (writeIndex != null && isManagedByDLM(dataStream, writeIndex)) {
            RolloverRequest rolloverRequest = defaultRolloverRequestSupplier.apply(dataStream.getName());
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
            List<Index> backingIndices = dataStream.getIndices();
            // we'll look at the current write index in the next run if it's rolled over (and not the write index anymore)
            for (int i = 0; i < backingIndices.size() - 1; i++) {
                IndexMetadata backingIndex = state.metadata().index(backingIndices.get(i));
                if (backingIndex == null || isManagedByDLM(dataStream, backingIndex) == false) {
                    continue;
                }

                if (isTimeToBeDeleted(dataStream.getName(), backingIndex, nowSupplier, retention)) {
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
    }

    /**
     * Checks if the provided index is ready to be deleted according to the configured retention.
     */
    static boolean isTimeToBeDeleted(
        String dataStreamName,
        IndexMetadata backingIndex,
        LongSupplier nowSupplier,
        TimeValue configuredRetention
    ) {
        TimeValue indexLifecycleDate = getCreationOrRolloverDate(dataStreamName, backingIndex);

        long nowMillis = nowSupplier.getAsLong();
        return nowMillis >= indexLifecycleDate.getMillis() + configuredRetention.getMillis();
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

    /**
     * This is quite a shallow method but the purpose of its existence is to have only one place to modify once we
     * introduce the index.lifecycle.prefer_ilm setting. Once the prefer_ilm setting exists the method will also
     * make more sense as it will encapsulate a bit more logic.
     */
    private static boolean isManagedByDLM(DataStream parentDataStream, IndexMetadata indexMetadata) {
        return indexMetadata.getLifecyclePolicyName() == null && parentDataStream.getLifecycle() != null;
    }

    private RolloverRequest getDefaultRolloverRequest(String dataStream) {
        RolloverRequest rolloverRequest = new RolloverRequest(dataStream, null).masterNodeTimeout(TimeValue.MAX_VALUE);
        rolloverRequest.setConditions(
            RolloverConditions.newBuilder()
                // TODO get rollover from cluster setting once we have it
                .addMaxIndexAgeCondition(TimeValue.timeValueDays(7))
                .addMaxPrimaryShardSizeCondition(ByteSizeValue.ofGb(50))
                .addMaxPrimaryShardDocsCondition(200_000_000L)
                // don't rollover an empty index
                .addMinIndexDocsCondition(1L)
                .build()
        );
        return rolloverRequest;
    }

    private void updatePollInterval(TimeValue newInterval) {
        this.pollInterval = newInterval;
        maybeScheduleJob();
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

    // package visibility for testing
    void setDefaultRolloverRequestSupplier(Function<String, RolloverRequest> defaultRolloverRequestSupplier) {
        this.defaultRolloverRequestSupplier = defaultRolloverRequestSupplier;
    }
}
