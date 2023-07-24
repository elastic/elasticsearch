/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.datastreams;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.index.mapper.DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;

/**
 * A component that updates the 'index.time_series.end_time' index setting for the most recently added backing index of a tsdb data stream.
 */
public class UpdateTimeSeriesRangeService extends AbstractLifecycleComponent implements LocalNodeMasterListener {

    private static final Logger LOGGER = LogManager.getLogger(UpdateTimeSeriesRangeService.class);

    private final ThreadPool threadPool;
    private final ClusterService clusterService;

    volatile TimeValue pollInterval;
    volatile Scheduler.Cancellable job;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final MasterServiceTaskQueue<UpdateTimeSeriesTask> taskQueue;

    UpdateTimeSeriesRangeService(Settings settings, ThreadPool threadPool, ClusterService clusterService) {
        this.pollInterval = DataStreamsPlugin.TIME_SERIES_POLL_INTERVAL.get(settings);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        clusterService.getClusterSettings().addSettingsUpdateConsumer(DataStreamsPlugin.TIME_SERIES_POLL_INTERVAL, this::setPollInterval);
        this.taskQueue = clusterService.createTaskQueue("update-time-series-range", Priority.URGENT, new UpdateTimeSeriesExecutor());
    }

    void perform(Runnable onComplete) {
        if (running.compareAndSet(false, true)) {
            LOGGER.debug("starting tsdb update task");
            var task = new UpdateTimeSeriesTask(e -> {
                if (e != null) {
                    LOGGER.warn("failed to update tsdb data stream end times", e);
                }
                running.set(false);
                onComplete.run();
            });
            taskQueue.submitTask("update_tsdb_data_stream_end_times", task, null);
        } else {
            LOGGER.debug("not starting tsdb update task, because another execution is still running");
        }
    }

    void setPollInterval(TimeValue newValue) {
        LOGGER.info(
            "updating [{}] setting from [{}] to [{}]",
            DataStreamsPlugin.TIME_SERIES_POLL_INTERVAL.getKey(),
            pollInterval,
            newValue
        );
        this.pollInterval = newValue;

        // Only re-schedule if we've been scheduled, this should only be the case on elected master node.
        if (job != null) {
            unschedule();
            scheduleTask();
        }
    }

    ClusterState updateTimeSeriesTemporalRange(ClusterState current, Instant now) {
        Metadata.Builder mBuilder = null;
        for (DataStream dataStream : current.metadata().dataStreams().values()) {
            if (dataStream.getIndexMode() != IndexMode.TIME_SERIES) {
                continue;
            }
            if (dataStream.isReplicated()) {
                continue;
            }

            // getWriteIndex() selects the latest added index:
            Index head = dataStream.getWriteIndex();
            IndexMetadata im = current.metadata().getIndexSafe(head);
            Instant currentEnd = IndexSettings.TIME_SERIES_END_TIME.get(im.getSettings());
            TimeValue lookAheadTime = DataStreamsPlugin.LOOK_AHEAD_TIME.get(im.getSettings());
            Instant newEnd = DataStream.getCanonicalTimestampBound(
                now.plus(lookAheadTime.getMillis(), ChronoUnit.MILLIS).plus(pollInterval.getMillis(), ChronoUnit.MILLIS)
            );
            if (newEnd.isAfter(currentEnd)) {
                try {
                    Settings settings = Settings.builder()
                        .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), DEFAULT_DATE_TIME_FORMATTER.format(newEnd))
                        .build();
                    LOGGER.debug(
                        "updating [{}] setting from [{}] to [{}] for data stream [{}]",
                        IndexSettings.TIME_SERIES_END_TIME.getKey(),
                        currentEnd,
                        newEnd,
                        dataStream.getName()
                    );
                    if (mBuilder == null) {
                        mBuilder = Metadata.builder(current.metadata());
                    }
                    mBuilder.updateSettings(settings, head.getName());
                    // Verify that all temporal ranges of each backing index is still valid:
                    dataStream.validate(mBuilder::get);
                } catch (Exception e) {
                    LOGGER.error(
                        () -> format(
                            "unable to update [%s] for data stream [%s] and backing index [%s]",
                            IndexSettings.TIME_SERIES_END_TIME.getKey(),
                            dataStream.getName(),
                            head.getName()
                        ),
                        e
                    );
                }
            }
        }

        if (mBuilder != null) {
            return ClusterState.builder(current).metadata(mBuilder).build();
        } else {
            return current;
        }
    }

    void scheduleTask() {
        if (job == null) {
            LOGGER.debug("schedule tsdb update task");
            job = threadPool.scheduleWithFixedDelay(
                () -> perform(() -> LOGGER.debug("completed tsdb update task")),
                pollInterval,
                ThreadPool.Names.SAME
            );
        }
    }

    void unschedule() {
        if (job != null) {
            job.cancel();
            job = null;
        }
    }

    @Override
    protected void doStart() {
        clusterService.addLocalNodeMasterListener(this);
    }

    @Override
    protected void doStop() {
        unschedule();
    }

    @Override
    protected void doClose() throws IOException {
        unschedule();
    }

    @Override
    public void onMaster() {
        scheduleTask();
    }

    @Override
    public void offMaster() {
        unschedule();
    }

    private record UpdateTimeSeriesTask(Consumer<Exception> listener) implements ClusterStateTaskListener {
        @Override
        public void onFailure(Exception e) {
            listener.accept(e);
        }
    }

    private class UpdateTimeSeriesExecutor implements ClusterStateTaskExecutor<UpdateTimeSeriesTask> {
        @Override
        public ClusterState execute(BatchExecutionContext<UpdateTimeSeriesTask> batchExecutionContext) throws Exception {
            final ClusterState result;
            try (var ignored = batchExecutionContext.dropHeadersContext()) {
                result = updateTimeSeriesTemporalRange(batchExecutionContext.initialState(), Instant.now());
            }
            for (final var taskContext : batchExecutionContext.taskContexts()) {
                taskContext.success(() -> taskContext.getTask().listener().accept(null));
            }
            return result;
        }
    }
}
