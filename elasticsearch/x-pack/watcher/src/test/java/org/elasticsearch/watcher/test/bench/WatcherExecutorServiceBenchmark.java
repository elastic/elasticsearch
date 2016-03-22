/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.test.bench;

import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.watcher.Watcher;
import org.elasticsearch.watcher.client.WatchSourceBuilder;
import org.elasticsearch.watcher.client.WatcherClient;
import org.elasticsearch.watcher.support.http.HttpRequestTemplate;
import org.elasticsearch.watcher.transport.actions.put.PutWatchRequest;
import org.elasticsearch.watcher.trigger.ScheduleTriggerEngineMock;
import org.elasticsearch.watcher.trigger.TriggerModule;
import org.elasticsearch.xpack.XPackPlugin;

import java.util.ArrayList;
import java.util.Arrays;
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

    private final static Settings SETTINGS = Settings.builder()
            .put("xpack.security.enabled", false)
            .put("cluster.name", "bench")
            .put("network.host", "localhost")
            .put("script.disable_dynamic", false)
            .put("discovery.zen.ping.unicast.hosts", "localhost")
            .put("http.cors.enabled", true)
            .put("cluster.routing.allocation.disk.threshold_enabled", false)
//                .put("recycler.page.limit.heap", "60%")
            .build();

    private static Client client;
    private static WatcherClient watcherClient;
    private static ScheduleTriggerEngineMock scheduler;

    protected static void start() throws Exception {
        Node node = new MockNode(Settings.builder().put(SETTINGS).put("node.data", false).build(), Version.CURRENT,
                Arrays.asList(XPackBenchmarkPlugin.class));
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

    public static final class XPackBenchmarkPlugin extends XPackPlugin {


        public XPackBenchmarkPlugin(Settings settings) {
            super(settings);
            watcher = new BenchmarkWatcher(settings);
        }

        public static class BenchmarkWatcher extends Watcher {

            public BenchmarkWatcher(Settings settings) {
                super(settings);
                Loggers.getLogger(XPackBenchmarkPlugin.class, settings).info("using watcher benchmark plugin");
            }

            @Override
            public Collection<Module> nodeModules() {
                List<Module> modules = new ArrayList<>(super.nodeModules());
                for (int i = 0; i < modules.size(); ++i) {
                    Module module = modules.get(i);
                    if (module instanceof TriggerModule) {
                        // replacing scheduler module so we'll
                        // have control on when it fires a job
                        modules.set(i, new MockTriggerModule(settings));
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
