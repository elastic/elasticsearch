/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xpack.meter.ix;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.locks.ReentrantLock;

public class MeterIXPoller implements LifecycleComponent {
    private static final Logger logger = LogManager.getLogger(MeterIXPoller.class);
    final ThreadPool threadPool;
    final IndicesService indicesService;
    Lifecycle.State state = Lifecycle.State.INITIALIZED;
    Scheduler.ScheduledCancellable next = null;
    private final ReentrantLock mutex = new ReentrantLock();

    public MeterIXPoller(ThreadPool threadPool, IndicesService indicesService) {
        this.threadPool = threadPool;
        this.indicesService = indicesService;
    }

    @Override
    public void close() {
        mutex.lock();
        try {
            if (state != Lifecycle.State.STOPPED && state != Lifecycle.State.CLOSED) {
                state = Lifecycle.State.CLOSED;
                if (next != null) {
                    next.cancel();
                }
                next = null;
            }
        } finally {
            mutex.unlock();
        }
    }

    @Override
    public Lifecycle.State lifecycleState() {
        return null;
    }

    @Override
    public void addLifecycleListener(LifecycleListener listener) {

    }

    @Override
    public void start() {
        mutex.lock();
        try {
            if (state == Lifecycle.State.INITIALIZED) {
                state = Lifecycle.State.STARTED;
                setScheduleNext();
            }
        } finally {
            mutex.unlock();
        }
    }

    @Override
    public void stop() {
        mutex.lock();
        try {
            if (state != Lifecycle.State.STOPPED && state != Lifecycle.State.CLOSED) {
                state = Lifecycle.State.STOPPED;
                if (next != null) {
                    next.cancel();
                }
                next = null;
            }
        } finally {
            mutex.unlock();
        }
    }

    private void setScheduleNext() {
        next = threadPool.schedule(this::logAndSchedule, TimeValue.timeValueSeconds(20), ThreadPool.Names.GENERIC);
    }

    public void logAndSchedule() {
        logIndexStats();
        mutex.lock();
        try {
            if (state == Lifecycle.State.STARTED) {
                setScheduleNext();
            }
        } finally {
            mutex.unlock();
        }
    }

    public void logIndexStats() {
        logger.warn("[STU] logIndexStats");
        logFromShard();
        logFromStats();
        logger.warn("[STU] done logIndexStats");
    }

    public void logFromShard() {
        logger.info("[STU] logFromShard");
        for (final IndexService indexService : indicesService) {
            for (final IndexShard shard : indexService) {
                //SegmentsStats segmentsStats = engine.segmentsStats(true, false);
                for (final Segment segment: shard.segments()) {
                    logger.info("{}:{} -> {}", shard.shardId(), segment.getName(), segment.getSize().getBytes());
                }
            }
        }
        logger.info("[STU] done logFromShard");
    }

    public void logFromEngine() {
        logger.info("[STU] logFromEngine");
        for (final IndexService indexService : indicesService) {
            for (final IndexShard shard : indexService) {
                Engine engine = shard.getEngineOrNull();
                if (engine == null) {
                    continue;
                }
                engine.getLastCommittedSegmentInfos();
                //SegmentsStats segmentsStats = engine.segmentsStats(true, false);
                for (final Segment segment: shard.segments()) {
                    logger.info("{}:{} -> {}", shard.shardId(), segment.getName(), segment.getSize().getBytes());
                }
            }
        }
        logger.info("[STU] done logFromEngine");
    }

    public void logFromStats() {
        NodeIndicesStats stats = indicesService.stats(new CommonStatsFlags(CommonStatsFlags.Flag.Segments).includeSegmentFileSizes(true));
        SegmentsStats segmentsStats = stats.getSegments();
        var files = segmentsStats.getFiles();
        logger.info("files {}", files);
    }
}
