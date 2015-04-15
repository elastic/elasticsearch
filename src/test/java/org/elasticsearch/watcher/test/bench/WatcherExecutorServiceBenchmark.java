/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.test.bench;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.plugin.LicensePlugin;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.watcher.WatcherPlugin;
import org.elasticsearch.watcher.client.WatchSourceBuilder;
import org.elasticsearch.watcher.client.WatcherClient;
import org.elasticsearch.watcher.support.http.HttpRequestTemplate;
import org.elasticsearch.watcher.transport.actions.put.PutWatchRequest;
import org.elasticsearch.watcher.trigger.ScheduleTriggerEngineMock;
import org.elasticsearch.watcher.trigger.TriggerModule;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.watcher.actions.ActionBuilders.indexAction;
import static org.elasticsearch.watcher.condition.ConditionBuilders.scriptCondition;
import static org.elasticsearch.watcher.input.InputBuilders.httpInput;
import static org.elasticsearch.watcher.input.InputBuilders.searchInput;
import static org.elasticsearch.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.interval;

/**
 * Starts a master only node with watcher and benchmarks the executor service side, so no scheduling. The benchmark
 * uses the mock scheduler to trigger watches.
 *
 * A date node needs to be started outside this benchmark. This the removes non watcher noise like indexing.
 */
public class WatcherExecutorServiceBenchmark {

    private final static Settings SETTINGS = ImmutableSettings.builder()
            .put("plugins.load_classpath_plugins", false)
            .put("shield.enabled", false)
            .put("plugin.types", WatcherBenchmarkPlugin.class.getName() + "," + LicensePlugin.class.getName())
            .put("cluster.name", "bench")
            .put("network.host", "localhost")
            .put("script.disable_dynamic", false)
            .put("discovery.zen.ping.unicast.hosts", "localhost")
            .put("discovery.zen.ping.multicast.enabled", false)
            .put("http.cors.enabled", true)
            .put("cluster.routing.allocation.disk.threshold_enabled", false)
//                .put("recycler.page.limit.heap", "60%")
            .build();

    private static Client client;
    private static WatcherClient watcherClient;
    private static ScheduleTriggerEngineMock scheduler;

    protected static void start() throws Exception {
        InternalNode node = (InternalNode) NodeBuilder.nodeBuilder().settings(SETTINGS).data(false).node();
        client = node.client();
        client.admin().cluster().prepareHealth("*").setWaitForGreenStatus().get();
        Thread.sleep(5000);
        watcherClient = node.injector().getInstance(WatcherClient.class);
        scheduler = node.injector().getInstance(ScheduleTriggerEngineMock.class);
    }

    public static final class SmallSearchInput extends WatcherExecutorServiceBenchmark {

        public static void main(String[] args) throws Exception {
            start();
            client.admin().indices().prepareCreate("test").get();
            client.prepareIndex("test", "test", "1").setSource("{}").get();

            int numAlerts = 1000;
            for (int i = 0; i < numAlerts; i++) {
                final String name = "_name" + i;
                PutWatchRequest putAlertRequest = new PutWatchRequest(name, new WatchSourceBuilder()
                        .trigger(schedule(interval("5s")))
                        .input(searchInput(new SearchRequest("test")
                                        .source(new SearchSourceBuilder()))
                        )
                        .condition(scriptCondition("ctx.payload.hits.total > 0")));
                putAlertRequest.setId(name);
                watcherClient.putWatch(putAlertRequest).actionGet();
            }

            int numThreads = 50;
            int watchersPerThread = numAlerts / numThreads;
            Thread[] threads = new Thread[numThreads];
            for (int i = 0; i < numThreads; i++) {
                final int begin = i * watchersPerThread;
                final int end = (i + 1) * watchersPerThread;
                Runnable r = new Runnable() {
                    @Override
                    public void run() {
                        while (true) {
                            for (int j = begin; j < end; j++) {
                                scheduler.trigger("_name" + j);
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

    }

    public static final class BigSearchInput extends WatcherExecutorServiceBenchmark {

        public static void main(String[] args) throws Exception {
            start();
            int numAlerts = 1000;
            for (int i = 0; i < numAlerts; i++) {
                final String name = "_name" + i;
                PutWatchRequest putAlertRequest = new PutWatchRequest(name, new WatchSourceBuilder()
                        .trigger(schedule(interval("5s")))
                        .input(searchInput(new SearchRequest()
                                        .source(new SearchSourceBuilder()))
                                        .extractKeys("hits.total")
                        )
                        .condition(scriptCondition("1 == 1"))
                        .addAction("_id", indexAction("index", "type")));
                putAlertRequest.setId(name);
                watcherClient.putWatch(putAlertRequest).actionGet();
            }

            int numThreads = 50;
            int watchersPerThread = numAlerts / numThreads;
            Thread[] threads = new Thread[numThreads];
            for (int i = 0; i < numThreads; i++) {
                final int begin = i * watchersPerThread;
                final int end = (i + 1) * watchersPerThread;
                Runnable r = new Runnable() {
                    @Override
                    public void run() {
                        while (true) {
                            for (int j = begin; j < end; j++) {
                                scheduler.trigger("_name" + j);
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

    }

    public static final class HttpInput extends WatcherExecutorServiceBenchmark {

        public static void main(String[] args) throws Exception {
            start();
            int numAlerts = 1000;
            for (int i = 0; i < numAlerts; i++) {
                final String name = "_name" + i;
                PutWatchRequest putAlertRequest = new PutWatchRequest(name, new WatchSourceBuilder()
                        .trigger(schedule(interval("5s")))
                        .input(httpInput(HttpRequestTemplate.builder("localhost", 9200)))
                        .condition(scriptCondition("ctx.payload.tagline == \"You Know, for Search\"")));
                putAlertRequest.setId(name);
                watcherClient.putWatch(putAlertRequest).actionGet();
            }

            int numThreads = 50;
            int watchersPerThread = numAlerts / numThreads;
            Thread[] threads = new Thread[numThreads];
            for (int i = 0; i < numThreads; i++) {
                final int begin = i * watchersPerThread;
                final int end = (i + 1) * watchersPerThread;
                Runnable r = new Runnable() {
                    @Override
                    public void run() {
                        while (true) {
                            for (int j = begin; j < end; j++) {
                                scheduler.trigger("_name" + j);
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
                    if (module instanceof TriggerModule) {
                        // replacing scheduler module so we'll
                        // have control on when it fires a job
                        modules.add(new MockTriggerModule(settings));

                    } else {
                        modules.add(module);
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
                }

            }
        }
    }

}
