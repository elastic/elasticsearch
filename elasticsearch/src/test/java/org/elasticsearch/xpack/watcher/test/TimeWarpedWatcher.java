/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.test;

import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.watcher.Watcher;
import org.elasticsearch.xpack.watcher.execution.ExecutionModule;
import org.elasticsearch.xpack.watcher.execution.SyncTriggerListener;
import org.elasticsearch.xpack.watcher.execution.WatchExecutor;
import org.elasticsearch.xpack.watcher.trigger.ScheduleTriggerEngineMock;
import org.elasticsearch.xpack.watcher.trigger.TriggerModule;
import org.elasticsearch.xpack.watcher.trigger.manual.ManualTriggerEngine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Stream;

public class TimeWarpedWatcher extends Watcher {

    public TimeWarpedWatcher(Settings settings) {
        super(settings);
        Loggers.getLogger(TimeWarpedWatcher.class, settings).info("using time warped watchers plugin");

    }

    @Override
    public Collection<Module> nodeModules() {
        if (!enabled) {
            return super.nodeModules();
        }
        List<Module> modules = new ArrayList<>(super.nodeModules());
        for (int i = 0; i < modules.size(); ++i) {
            Module module = modules.get(i);
            if (module instanceof TriggerModule) {
                // replacing scheduler module so we'll
                // have control on when it fires a job
                modules.set(i, new MockTriggerModule(settings));
            } else if (module instanceof ExecutionModule) {
                // replacing the execution module so all the watches will be
                // executed on the same thread as the trigger engine
                modules.set(i, new MockExecutionModule());
            }
        }
        return modules;
    }


    public static class MockTriggerModule extends TriggerModule {

        public MockTriggerModule(Settings settings) {
            super(settings);
        }

        @Override
        protected void registerStandardEngines() {
            registerEngine(ScheduleTriggerEngineMock.class);
            registerEngine(ManualTriggerEngine.class);
        }
    }

    public static class MockExecutionModule extends ExecutionModule {

        public MockExecutionModule() {
            super(SameThreadExecutor.class, SyncTriggerListener.class);
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

}
