/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.datastreams;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.elasticsearch.index.mapper.DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;

public class TSDBDataStreamService extends AbstractLifecycleComponent implements LocalNodeMasterListener {

    private static final Logger LOGGER = LogManager.getLogger(TSDBDataStreamService.class);

    private final TimeValue pollInterval;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;

    private volatile Scheduler.Cancellable job;

    public TSDBDataStreamService(Settings settings, ThreadPool threadPool, ClusterService clusterService) {
        this.pollInterval = LifecycleSettings.LIFECYCLE_POLL_INTERVAL_SETTING.get(settings);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
    }

    void perform() {
        job = null;
        clusterService.submitStateUpdateTask("update_tsdb_data_stream_end_times", new ClusterStateUpdateTask(Priority.URGENT) {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return updateTimeSeriesEndTimes(currentState);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                scheduleTask();
            }

            @Override
            public void onFailure(String source, Exception e) {
                LOGGER.warn("failed to update tsdb data stream end times", e);
                scheduleTask();
            }
        });
    }

    ClusterState updateTimeSeriesEndTimes(ClusterState current) {
        Metadata.Builder mBuilder = null;
        for (DataStream dataStream : current.metadata().dataStreams().values()) {
            if (dataStream.getType() != DataStream.Type.TSDB) {
                continue;
            }
            if (dataStream.isReplicated()) {
                continue;
            }

            String head = dataStream.getIndices().get(0).getName();
            IndexMetadata im = current.metadata().getIndices().get(head);
            Instant currentEnd = Instant.ofEpochMilli(
                DEFAULT_DATE_TIME_FORMATTER.parseMillis(im.getSettings().get(IndexSettings.TIME_SERIES_END_TIME.getKey()))
            );
            if (Instant.now().plus(pollInterval.getMillis() * 2, ChronoUnit.MILLIS).compareTo(currentEnd) >= 0) {
                Instant newEnd = currentEnd.plus(DataStream.DEFAULT_LOOK_AHEAD_TIME).plus(pollInterval.getMillis(), ChronoUnit.MILLIS);
                Settings settings = Settings.builder()
                    .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), DEFAULT_DATE_TIME_FORMATTER.format(newEnd))
                    .build();
                LOGGER.info(
                    "updating [{}] setting from [{}] to [{}] for index [{}]",
                    IndexSettings.TIME_SERIES_END_TIME.getKey(),
                    currentEnd,
                    newEnd,
                    head
                );
                if (mBuilder == null) {
                    mBuilder = Metadata.builder(current.metadata());
                }
                mBuilder.updateSettings(settings, head);
            } else {
                LOGGER.info(
                    "not updating [{}] setting with value [{}] for index [{}]",
                    IndexSettings.TIME_SERIES_END_TIME.getKey(),
                    currentEnd,
                    head
                );
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
            LOGGER.info("schedule tsdb update task");
            job = threadPool.schedule(this::perform, pollInterval, ThreadPool.Names.SAME);
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

    }

    @Override
    public void onMaster() {
        scheduleTask();
    }

    @Override
    public void offMaster() {
        unschedule();
    }
}
