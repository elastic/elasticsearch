/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.test;

import org.elasticsearch.alerts.AlertsPlugin;
import org.elasticsearch.alerts.history.AlertsExecutor;
import org.elasticsearch.alerts.history.HistoryModule;
import org.elasticsearch.alerts.scheduler.SchedulerMock;
import org.elasticsearch.alerts.scheduler.SchedulerModule;
import org.elasticsearch.alerts.support.clock.Clock;
import org.elasticsearch.alerts.support.clock.ClockMock;
import org.elasticsearch.alerts.support.clock.ClockModule;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 *
 */
public class TimeWarpedAlertsPlugin extends AlertsPlugin {

    public TimeWarpedAlertsPlugin(Settings settings) {
        super(settings);
        Loggers.getLogger(TimeWarpedAlertsPlugin.class, settings).info("using time warped alerts plugin");

    }

    @Override
    public Collection<Class<? extends Module>> modules() {
        return ImmutableList.<Class<? extends Module>>of(AlertsModule.class);
    }

    /**
     *
     */
    public static class AlertsModule extends org.elasticsearch.alerts.AlertsModule {

        @Override
        public Iterable<? extends Module> spawnModules() {
            List<Module> modules = new ArrayList<>();
            for (Module module : super.spawnModules()) {

                if (module instanceof SchedulerModule) {
                    // replacing scheduler module so we'll
                    // have control on when it fires a job
                    modules.add(new MockSchedulerModule());

                } else if (module instanceof ClockModule) {
                    // replacing the clock module so we'll be able
                    // to control time in tests
                    modules.add(new MockClockModule());

                } else if (module instanceof HistoryModule) {
                    // replacing the history module so all the alerts will be
                    // executed on the same thread as the schedule fire
                    modules.add(new MockHistoryModule());

                } else {
                    modules.add(module);
                }
            }
            return modules;
        }

        public static class MockSchedulerModule extends SchedulerModule {

            public MockSchedulerModule() {
                super(SchedulerMock.class);
            }

        }

        public static class MockClockModule extends ClockModule {
            @Override
            protected void configure() {
                bind(ClockMock.class).asEagerSingleton();
                bind(Clock.class).to(ClockMock.class);
            }
        }

        public static class MockHistoryModule extends HistoryModule {

            public MockHistoryModule() {
                super(SameThreadExecutor.class);
            }

            public static class SameThreadExecutor implements AlertsExecutor {

                @Override
                public BlockingQueue queue() {
                    return new ArrayBlockingQueue(1);
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
    }
}
