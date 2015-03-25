/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.test.bench;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.watcher.WatcherPlugin;
import org.elasticsearch.watcher.client.WatchSourceBuilder;
import org.elasticsearch.watcher.client.WatcherClient;
import org.elasticsearch.watcher.scheduler.Scheduler;
import org.elasticsearch.watcher.scheduler.SchedulerMock;
import org.elasticsearch.watcher.scheduler.SchedulerModule;
import org.elasticsearch.watcher.transport.actions.put.PutWatchRequest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.watcher.actions.ActionBuilders.indexAction;
import static org.elasticsearch.watcher.condition.ConditionBuilders.scriptCondition;
import static org.elasticsearch.watcher.input.InputBuilders.searchInput;
import static org.elasticsearch.watcher.scheduler.schedule.Schedules.interval;

/**
 */
public class WatcherBenchmark {

    public static void main(String[] args) throws Exception {
        Settings settings = ImmutableSettings.builder()
                .put("plugins.load_classpath_plugins", false)
                .put("plugin.types", WatcherBenchmarkPlugin.class.getName())
                .put("cluster.name", WatcherBenchmark.class.getSimpleName())
                .put("http.cors.enabled", true)
                .build();
        InternalNode node = (InternalNode) NodeBuilder.nodeBuilder().settings(settings).data(false).node();
        node.client().admin().cluster().prepareHealth().setWaitForGreenStatus().get();
        Thread.sleep(5000);
        WatcherClient watcherClient = node.injector().getInstance(WatcherClient.class);

        int numAlerts = 1000;
        for (int i = 0; i < numAlerts; i++) {
            final String name = "_name" + i;
            PutWatchRequest putAlertRequest = new PutWatchRequest(name, new WatchSourceBuilder()
                            .schedule(interval("5s"))
                            .input(searchInput(new SearchRequest().source(
                                    new SearchSourceBuilder()
                            )))
                            .condition(scriptCondition("1 == 1"))
                            .addAction(indexAction("index", "type")));
            putAlertRequest.setName(name);
            watcherClient.putWatch(putAlertRequest).actionGet();
        }

        int numThreads = 50;
        int watchersPerThread = numAlerts / numThreads;
        final SchedulerMock scheduler = (SchedulerMock) node.injector().getInstance(Scheduler.class);
        Thread[] threads = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            final int begin = i * watchersPerThread;
            final int end = (i + 1) * watchersPerThread;
            Runnable r = new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        for (int j = begin; j < end; j++) {
                            scheduler.fire("_name" + j);
                        }
                    }
                }
            };
            threads[i] = new Thread(r);
            threads[i].start();
        }

        
        for (Thread thread : threads) {
            thread.join();
        }
    }

    public static final class WatcherBenchmarkPlugin extends WatcherPlugin {

        public WatcherBenchmarkPlugin(Settings settings) {
            super(settings);
            Loggers.getLogger(WatcherBenchmarkPlugin.class, settings).info("using watcher benchmark plugin");
        }

        @Override
        public Collection<Class<? extends Module>> modules() {
            return ImmutableList.<Class<? extends Module>>of(WatcherModule.class);
        }

        public static class WatcherModule extends org.elasticsearch.watcher.WatcherModule {

            public WatcherModule(Settings settings) {
                super(settings);
            }

            @Override
            public Iterable<? extends Module> spawnModules() {
                List<Module> modules = new ArrayList<>();
                for (Module module : super.spawnModules()) {
                    if (module instanceof SchedulerModule) {
                        // replacing scheduler module so we'll
                        // have control on when it fires a job
                        modules.add(new MockSchedulerModule());

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
        }
    }

}
