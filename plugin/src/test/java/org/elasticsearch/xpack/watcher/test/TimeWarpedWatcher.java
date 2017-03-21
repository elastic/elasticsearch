/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.test;

import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.watcher.Watcher;
import org.elasticsearch.xpack.watcher.execution.ExecutionService;
import org.elasticsearch.xpack.watcher.execution.SyncTriggerEventConsumer;
import org.elasticsearch.xpack.watcher.execution.WatchExecutor;
import org.elasticsearch.xpack.watcher.trigger.ScheduleTriggerEngineMock;
import org.elasticsearch.xpack.watcher.trigger.TriggerEngine;
import org.elasticsearch.xpack.watcher.trigger.TriggerEvent;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleRegistry;

import java.time.Clock;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class TimeWarpedWatcher extends Watcher {

    public TimeWarpedWatcher(Settings settings) {
        super(settings);
        Loggers.getLogger(TimeWarpedWatcher.class, settings).info("using time warped watchers plugin");

    }

    @Override
    protected TriggerEngine getTriggerEngine(Clock clock, ScheduleRegistry scheduleRegistry) {
        return new ScheduleTriggerEngineMock(settings, scheduleRegistry, clock);
    }

    @Override
    protected WatchExecutor getWatchExecutor(ThreadPool threadPool) {
        return new SameThreadExecutor();
    }

    @Override
    protected Consumer<Iterable<TriggerEvent>> getTriggerEngineListener(ExecutionService executionService) {
        return new SyncTriggerEventConsumer(settings, executionService);
    }

    public static class SameThreadExecutor implements WatchExecutor {

        @Override
        public Stream<Runnable> tasks() {
            return Stream.empty();
        }

        @Override
        public BlockingQueue<Runnable> queue() {
            return new ArrayBlockingQueue<>(1);
        }

        @Override
        public long largestPoolSize() {
            return 1;
        }

        @Override
        public void execute(Runnable runnable) {
            runnable.run();
        }
    }
}
