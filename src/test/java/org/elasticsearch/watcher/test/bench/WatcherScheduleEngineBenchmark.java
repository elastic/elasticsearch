/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.test.bench;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.license.plugin.LicensePlugin;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.elasticsearch.watcher.WatcherPlugin;
import org.elasticsearch.watcher.actions.ActionBuilders;
import org.elasticsearch.watcher.actions.logging.LoggingLevel;
import org.elasticsearch.watcher.client.WatchSourceBuilder;
import org.elasticsearch.watcher.client.WatcherClient;
import org.elasticsearch.watcher.history.HistoryStore;
import org.elasticsearch.watcher.support.clock.Clock;
import org.elasticsearch.watcher.WatcherService;
import org.elasticsearch.watcher.watch.WatchStore;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.search.aggregations.AggregationBuilders.*;
import static org.elasticsearch.watcher.condition.ConditionBuilders.scriptCondition;
import static org.elasticsearch.watcher.input.InputBuilders.searchInput;
import static org.elasticsearch.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.interval;

/**
 */
public class WatcherScheduleEngineBenchmark {

    private final static Settings SETTINGS = ImmutableSettings.builder()
            .put("plugins.load_classpath_plugins", false)
            .put("shield.enabled", false)
            .put("plugin.types", WatcherPlugin.class.getName() + "," + LicensePlugin.class.getName())
            .put("cluster.name", "bench")
            .put("script.disable_dynamic", false)
            .put("http.cors.enabled", true)
            .build();

    public static void main(String[] args) throws Exception {
        System.setProperty("es.logger.prefix", "");

        String[] engines = new String[]{"ticker", "scheduler"};
        int numWatches = 2000;
        int benchTime = 60000;
        int interval = 1;

        if (args.length % 2 != 0) {
            throw new IllegalArgumentException("Uneven number of arguments");
        }
        for (int i = 0; i < args.length; i += 2) {
            String value = args[i + 1];
            if ("--num_watches".equals(args[i])) {
                numWatches = Integer.valueOf(value);
            } else if ("--bench_time".equals(args[i])) {
                benchTime = Integer.valueOf(value);
            } else if ("--interval".equals(args[i])) {
                interval = Integer.valueOf(value);
            } else if ("--engines".equals(args[i])) {
                engines = Strings.commaDelimitedListToStringArray(value);
            }
        }
        System.out.println("Running schedule benchmark with:");
        System.out.println("numWatches=" + numWatches + " benchTime=" + benchTime + " interval=" + interval + " engines=" + Arrays.toString(engines));
        System.out.println("and heap_max=" + JvmInfo.jvmInfo().getMem().getHeapMax());


        // First clean everything and index the watcher (but not via put alert api!)
        Settings settings = ImmutableSettings.builder().put(SETTINGS).put("plugin.types", "").build();
        try (InternalNode node = (InternalNode) NodeBuilder.nodeBuilder().settings(settings).data(false).node()) {
            try (Client client = node.client()) {
                ClusterHealthResponse response = client.admin().cluster().prepareHealth().setWaitForNodes("2").get();
                if (response.getNumberOfNodes() != 2 && response.getNumberOfDataNodes() != 1) {
                    throw new IllegalStateException("This benchmark needs one extra data only node running outside this benchmark");
                }

                client.admin().indices().prepareDelete("_all").get();
                client.admin().indices().prepareCreate("test").get();
                client.prepareIndex("test", "test", "1").setSource("{}").get();

                System.out.println("===============> indexing [" + numWatches + "] watches");
                for (int i = 0; i < numWatches; i++) {
                    final String id = "_id_" + i;
                    client.prepareIndex(WatchStore.INDEX, WatchStore.DOC_TYPE, id)
                            .setSource(new WatchSourceBuilder()
                                            .trigger(schedule(interval(interval + "s")))
                                            .input(searchInput(new SearchRequest("test")
                                                            .source(new SearchSourceBuilder()))
                                            )
                                            .condition(scriptCondition("ctx.payload.hits.total > 0"))
                                            .addAction("logging", ActionBuilders.loggingAction("test").setLevel(LoggingLevel.TRACE))
                                            .buildAsBytes(XContentType.JSON)
                            ).get();
                }
                client.admin().indices().prepareFlush(WatchStore.INDEX, "test").get();
                System.out.println("===============> indexed [" + numWatches + "] watches");
            }
        }


        // Now for each scheduler impl run the benchmark
        Map<String, BenchStats> results = new HashMap<>();
        for (String engine : engines) {
            BenchStats stats = new BenchStats(engine, numWatches);
            results.put(engine, stats);
            System.out.println("===============> testing engine [" + engine + "]");
            System.gc();
            settings = ImmutableSettings.builder()
                    .put(SETTINGS)
                    .put("watcher.trigger.schedule.engine", engine)
                    .build();
            try (InternalNode node = (InternalNode) NodeBuilder.nodeBuilder().settings(settings).data(false).node()) {
                try (final Client client = node.client()) {
                    client.admin().cluster().prepareHealth().setWaitForNodes("2").get();
                    client.admin().indices().prepareDelete(HistoryStore.INDEX_PREFIX + "*").get();
                    client.admin().cluster().prepareHealth(WatchStore.INDEX, "test").setWaitForYellowStatus().get();

                    Clock clock = node.injector().getInstance(Clock.class);
                    WatcherClient watcherClient = node.injector().getInstance(WatcherClient.class);
                    while (watcherClient.prepareWatcherStats().get().getWatchServiceState() != WatcherService.State.STARTED) {
                        Thread.sleep(100);
                    }
                    long actualLoadedWatches = watcherClient.prepareWatcherStats().get().getWatchesCount();
                    if (actualLoadedWatches != numWatches) {
                        throw new IllegalStateException("Expected [" + numWatches + "] watched to be loaded, but only [" + actualLoadedWatches + "] watches were actually loaded");
                    }
                    long startTime = clock.millis();
                    System.out.println("==> watcher started, waiting [" + benchTime + "] seconds now...");

                    final AtomicBoolean start = new AtomicBoolean(true);
                    final MeanMetric jvmUsedHeapSpace = new MeanMetric();
                    Thread sampleThread = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                while (start.get()) {
                                    NodesStatsResponse response = client.admin().cluster().prepareNodesStats("_master").setJvm(true).get();
                                    ByteSizeValue heapUsed = response.getNodes()[0].getJvm().getMem().getHeapUsed();
                                    jvmUsedHeapSpace.inc(heapUsed.bytes());
                                    Thread.sleep(1000);
                                }
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    });
                    sampleThread.start();
                    Thread.sleep(benchTime);
                    long endTime =  clock.millis();
                    start.set(false);
                    sampleThread.join();

                    NodesStatsResponse response = client.admin().cluster().prepareNodesStats().setThreadPool(true).get();
                    for (NodeStats nodeStats : response) {
                        for (ThreadPoolStats.Stats threadPoolStats : nodeStats.getThreadPool()) {
                            if ("watcher".equals(threadPoolStats.getName())) {
                                stats.setWatcherThreadPoolStats(threadPoolStats);
                            }
                        }
                    }
                    client.admin().indices().prepareRefresh(HistoryStore.INDEX_PREFIX + "*").get();
                    SearchResponse searchResponse = client.prepareSearch(HistoryStore.INDEX_PREFIX + "*")
                            .setQuery(QueryBuilders.rangeQuery("trigger_event.schedule.scheduled_time").gte(startTime).lte(endTime))
                            .addAggregation(terms("state").field("state"))
                            .addAggregation(histogram("delay")
                                            .script("doc['trigger_event.schedule.triggered_time'].value - doc['trigger_event.schedule.scheduled_time'].value")
                                            .interval(10)
                            )
                            .addAggregation(percentiles("percentile_delay")
                                            .script("doc['trigger_event.schedule.triggered_time'].value - doc['trigger_event.schedule.scheduled_time'].value")
                                            .percentiles(1.0, 20.0, 50.0, 80.0, 99.0)
                            )
                            .get();
                    Terms terms = searchResponse.getAggregations().get("state");
                    stats.setStateStats(terms);
                    Histogram histogram = searchResponse.getAggregations().get("delay");
                    stats.setDelayStats(histogram);
                    System.out.println("===> State");
                    for (Terms.Bucket bucket : terms.getBuckets()) {
                        System.out.println("\t" + bucket.getKey() + "=" + bucket.getDocCount());
                    }
                    System.out.println("===> Delay");
                    for (Histogram.Bucket bucket : histogram.getBuckets()) {
                        System.out.println("\t" + bucket.getKey() + "=" + bucket.getDocCount());
                    }
                    Percentiles percentiles = searchResponse.getAggregations().get("percentile_delay");
                    stats.setDelayPercentiles(percentiles);
                    stats.setAvgJvmUsed(jvmUsedHeapSpace);
                    watcherClient.prepareWatchService().stop().get();
                }
            }
        }

        // Finally print out the results in an asciidoc table:
        System.out.println("## Ran with [" + numWatches + "] watches, interval [" + interval + "] and bench_time [" + benchTime + "]");
        System.out.println();
        System.out.println("### Watcher execution and watcher thread pool stats");
        System.out.println();
        System.out.println("   Name    | avg heap used | wtp rejected | wtp completed");
        System.out.println("---------- | ------------- | ------------ | -------------");
        for (BenchStats benchStats : results.values()) {
            benchStats.printThreadStats();
        }
        System.out.println();
        System.out.println("### Watch record state");
        System.out.println();
        System.out.println("   Name    | # state executed | # state failed | # state throttled | # state awaits_execution");
        System.out.println("---------- | ---------------- | -------------- | ----------------- | ------------------------");
        for (BenchStats benchStats : results.values()) {
            benchStats.printWatchRecordState();
        }

        System.out.println();
        System.out.println("### Trigger delay");
        System.out.println();
        System.out.println("   Name    | 1% delayed | 20% delayed | 50% delayed | 80% delayed | 99% delayed");
        System.out.println("---------- | ---------- | ----------- | ----------- | ----------- | -----------");
        for (BenchStats benchStats : results.values()) {
            benchStats.printTriggerDelay();
        }
    }

    private static class BenchStats {

        private final String name;
        private final int numWatches;
        private ThreadPoolStats.Stats watcherThreadPoolStats;

        private Terms stateStats;
        private Histogram delayStats;

        private Percentiles delayPercentiles;

        private long avgHeapUsed;

        private BenchStats(String name, int numWatches) {
            this.name = name;
            this.numWatches = numWatches;
        }

        public String getName() {
            return name;
        }

        public int getNumWatches() {
            return numWatches;
        }

        public ThreadPoolStats.Stats getWatcherThreadPoolStats() {
            return watcherThreadPoolStats;
        }

        public void setWatcherThreadPoolStats(ThreadPoolStats.Stats watcherThreadPoolStats) {
            this.watcherThreadPoolStats = watcherThreadPoolStats;
        }

        public Terms getStateStats() {
            return stateStats;
        }

        public void setStateStats(Terms stateStats) {
            this.stateStats = stateStats;
        }

        public Histogram getDelayStats() {
            return delayStats;
        }

        public void setDelayStats(Histogram delayStats) {
            this.delayStats = delayStats;
        }

        public Percentiles getDelayPercentiles() {
            return delayPercentiles;
        }

        public void setDelayPercentiles(Percentiles delayPercentiles) {
            this.delayPercentiles = delayPercentiles;
        }

        public void setAvgJvmUsed(MeanMetric jvmMemUsed) {
            avgHeapUsed = Math.round(jvmMemUsed.mean());
        }

        public void printThreadStats() throws IOException {
            System.out.printf(
                    Locale.ENGLISH,
                    "%10s | %13s | %12d | %13d \n",
                    name, new ByteSizeValue(avgHeapUsed),
                    watcherThreadPoolStats.getRejected(), watcherThreadPoolStats.getCompleted()
            );
        }

        public void printWatchRecordState() throws IOException {
            Terms.Bucket executed = stateStats.getBucketByKey("executed");
            Terms.Bucket failed = stateStats.getBucketByKey("failed");
            Terms.Bucket throttled = stateStats.getBucketByKey("throttled");
            Terms.Bucket awaitsExecution = stateStats.getBucketByKey("awaits_execution");
            System.out.printf(
                    Locale.ENGLISH,
                    "%10s | %16d | %14d | %17d | %24d \n",
                    name, executed != null ? executed.getDocCount() : 0,
                    failed != null ? failed.getDocCount() : 0,
                    throttled != null ? throttled.getDocCount() : 0,
                    awaitsExecution != null ? awaitsExecution.getDocCount() : 0
            );
        }

        public void printTriggerDelay() throws Exception {
            String _1thPercentile = String.valueOf(Math.round(delayPercentiles.percentile(1.0)));
            String _20thPercentile = String.valueOf(Math.round(delayPercentiles.percentile(20.0)));
            String _50thPercentile = String.valueOf(Math.round(delayPercentiles.percentile(50.0)));
            String _80thPercentile = String.valueOf(Math.round(delayPercentiles.percentile(80.0)));
            String _99thPercentile = String.valueOf(Math.round(delayPercentiles.percentile(99.0)));
            System.out.printf(
                    Locale.ENGLISH,
                    "%10s | %10s | %11s | %11s | %11s | %11s \n",
                    name, _1thPercentile, _20thPercentile, _50thPercentile, _80thPercentile, _99thPercentile
            );
        }
    }

}
