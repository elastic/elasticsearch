/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.test.bench;

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
public class AlertsBenchmark {

    public static void main(String[] args) throws Exception {
        Settings settings = ImmutableSettings.builder()
                .put("plugins.load_classpath_plugins", false)
                .put("plugin.types", BenchmarkAlertPlugin.class.getName())
                .put("cluster.name", AlertsBenchmark.class.getSimpleName())
                .put("http.cors.enabled", true)
                .build();
        InternalNode node = (InternalNode) NodeBuilder.nodeBuilder().settings(settings).data(false).node();
        node.client().admin().cluster().prepareHealth().setWaitForGreenStatus().get();
        Thread.sleep(5000);
        WatcherClient alertsClient = node.injector().getInstance(WatcherClient.class);

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
            alertsClient.putWatch(putAlertRequest).actionGet();
        }

        int numThreads = 50;
        int alertsPerThread = numAlerts / numThreads;
        final SchedulerMock scheduler = (SchedulerMock) node.injector().getInstance(Scheduler.class);
        Thread[] threads = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            final int begin = i * alertsPerThread;
            final int end = (i + 1) * alertsPerThread;
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

    public static final class BenchmarkAlertPlugin extends WatcherPlugin {

        public BenchmarkAlertPlugin(Settings settings) {
            super(settings);
            Loggers.getLogger(BenchmarkAlertPlugin.class, settings).info("using benchmark alerts plugin");
        }

        @Override
        public Collection<Class<? extends Module>> modules() {
            return ImmutableList.<Class<? extends Module>>of(WatcherModule.class);
        }

        public static class WatcherModule extends org.elasticsearch.watcher.WatcherModule {

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
