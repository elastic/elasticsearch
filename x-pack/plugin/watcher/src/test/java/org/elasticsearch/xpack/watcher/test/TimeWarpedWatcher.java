/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.watcher.trigger.TriggerEvent;
import org.elasticsearch.xpack.core.watcher.watch.ClockMock;
import org.elasticsearch.xpack.watcher.Watcher;
import org.elasticsearch.xpack.watcher.execution.ExecutionService;
import org.elasticsearch.xpack.watcher.execution.SyncTriggerEventConsumer;
import org.elasticsearch.xpack.watcher.execution.WatchExecutor;
import org.elasticsearch.xpack.watcher.trigger.ScheduleTriggerEngineMock;
import org.elasticsearch.xpack.watcher.trigger.TriggerEngine;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleRegistry;

import java.nio.file.Path;
import java.time.Clock;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class TimeWarpedWatcher extends LocalStateCompositeXPackPlugin {
    private static final Logger logger = LogManager.getLogger(TimeWarpedWatcher.class);

    // use a single clock across all nodes using this plugin, this lets keep it static
    private static final ClockMock clock = new ClockMock();

    public TimeWarpedWatcher(final Settings settings, final Path configPath) throws Exception {
        super(settings, configPath);
        logger.info("using time warped watchers plugin");

        TimeWarpedWatcher thisVar = this;

        plugins.add(new Watcher(settings) {
            @Override
            protected SSLService getSslService() {
                return thisVar.getSslService();
            }

            @Override
            protected XPackLicenseState getLicenseState() {
                return thisVar.getLicenseState();
            }

            @Override
            protected Clock getClock() {
                return clock;
            }

            @Override
            protected TriggerEngine<?, ?> getTriggerEngine(Clock clock, ScheduleRegistry scheduleRegistry) {
                return new ScheduleTriggerEngineMock(scheduleRegistry, clock);
            }

            @Override
            protected WatchExecutor getWatchExecutor(ThreadPool threadPool) {
                return new SameThreadExecutor();
            }

            @Override
            protected Consumer<Iterable<TriggerEvent>> getTriggerEngineListener(ExecutionService executionService) {
                return new SyncTriggerEventConsumer(executionService);
            }
        });
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
