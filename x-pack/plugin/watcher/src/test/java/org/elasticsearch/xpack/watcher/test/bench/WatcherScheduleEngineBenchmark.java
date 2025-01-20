/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.test.bench;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.node.InternalSettingsPreparer;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.PluginsLoader;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Percentiles;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.watcher.WatcherState;
import org.elasticsearch.xpack.core.watcher.client.WatchSourceBuilder;
import org.elasticsearch.xpack.core.watcher.history.HistoryStoreField;
import org.elasticsearch.xpack.core.watcher.transport.actions.service.WatcherServiceRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.stats.WatcherStatsRequestBuilder;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.actions.ActionBuilders;
import org.elasticsearch.xpack.watcher.actions.logging.LoggingLevel;
import org.elasticsearch.xpack.watcher.condition.ScriptCondition;
import org.elasticsearch.xpack.watcher.test.LocalStateWatcher;

import java.io.IOException;
import java.time.Clock;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.percentiles;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.searchInput;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.templateRequest;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;

@SuppressForbidden(reason = "benchmark")
public class WatcherScheduleEngineBenchmark {

    private static final Settings SETTINGS = Settings.builder()
        .put("xpack.security.enabled", false)
        .put("cluster.name", "bench")
        .put("script.disable_dynamic", false)
        .put("http.cors.enabled", true)
        .build();

    public static void main(String[] args) throws Exception {
        String[] engines = new String[] { "ticker", "scheduler" };
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
        System.out.println(
            "numWatches=" + numWatches + " benchTime=" + benchTime + " interval=" + interval + " engines=" + Arrays.toString(engines)
        );
        System.out.println("and heap_max=" + JvmInfo.jvmInfo().getMem().getHeapMax());

        Environment internalNodeEnv = InternalSettingsPreparer.prepareEnvironment(
            Settings.builder().put(SETTINGS).put("node.data", false).build(),
            emptyMap(),
            null,
            () -> {
                throw new IllegalArgumentException("settings must have [node.name]");
            }
        );

        // First clean everything and index the watcher (but not via put alert api!)
        try (
            Node node = new Node(
                internalNodeEnv,
                PluginsLoader.createPluginsLoader(internalNodeEnv.modulesFile(), internalNodeEnv.pluginsFile())
            ).start()
        ) {
            final Client client = node.client();
            ClusterHealthResponse response = client.admin().cluster().prepareHealth(TimeValue.THIRTY_SECONDS).setWaitForNodes("2").get();
            if (response.getNumberOfNodes() != 2 && response.getNumberOfDataNodes() != 1) {
                throw new IllegalStateException("This benchmark needs one extra data only node running outside this benchmark");
            }

            client.admin().indices().prepareDelete("_all").get();
            client.admin().indices().prepareCreate("test").get();
            client.prepareIndex().setIndex("test").setId("1").setSource("{}", XContentType.JSON).get();

            System.out.println("===============> indexing [" + numWatches + "] watches");
            for (int i = 0; i < numWatches; i++) {
                final String id = "_id_" + i;
                client.prepareIndex()
                    .setIndex(Watch.INDEX)
                    .setId(id)
                    .setSource(
                        new WatchSourceBuilder().trigger(schedule(interval(interval + "s")))
                            .input(searchInput(templateRequest(new SearchSourceBuilder(), "test")))
                            .condition(
                                new ScriptCondition(
                                    new Script(
                                        ScriptType.INLINE,
                                        Script.DEFAULT_SCRIPT_LANG,
                                        "ctx.payload.hits.total.value > 0",
                                        emptyMap()
                                    )
                                )
                            )
                            .addAction("logging", ActionBuilders.loggingAction("test").setLevel(LoggingLevel.TRACE))
                            .buildAsBytes(XContentType.JSON),
                        XContentType.JSON
                    )
                    .get();
            }
            client.admin().indices().prepareFlush(Watch.INDEX, "test").get();
            System.out.println("===============> indexed [" + numWatches + "] watches");
        }

        // Now for each scheduler impl run the benchmark
        Map<String, BenchStats> results = new HashMap<>();
        for (String engine : engines) {
            BenchStats stats = new BenchStats(engine, numWatches);
            results.put(engine, stats);
            System.out.println("===============> testing engine [" + engine + "]");
            System.gc();
            Settings settings = Settings.builder()
                .put(SETTINGS)
                .put("xpack.watcher.trigger.schedule.engine", engine)
                .put("node.data", false)
                .build();
            try (Node node = new MockNode(settings, Arrays.asList(LocalStateWatcher.class))) {
                final Client client = node.client();
                client.admin().cluster().prepareHealth(TimeValue.THIRTY_SECONDS).setWaitForNodes("2").get();
                client.admin().indices().prepareDelete(HistoryStoreField.DATA_STREAM + "*").get();
                client.admin().cluster().prepareHealth(TimeValue.THIRTY_SECONDS, Watch.INDEX, "test").setWaitForYellowStatus().get();

                Clock clock = node.injector().getInstance(Clock.class);
                while (new WatcherStatsRequestBuilder(client).get()
                    .getNodes()
                    .stream()
                    .allMatch(r -> r.getWatcherState() == WatcherState.STARTED) == false) {
                    Thread.sleep(100);
                }
                long actualLoadedWatches = new WatcherStatsRequestBuilder(client).get().getWatchesCount();
                if (actualLoadedWatches != numWatches) {
                    throw new IllegalStateException(
                        "Expected ["
                            + numWatches
                            + "] watched to be loaded, but only ["
                            + actualLoadedWatches
                            + "] watches were actually loaded"
                    );
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
                                ByteSizeValue heapUsed = response.getNodes().get(0).getJvm().getMem().getHeapUsed();
                                jvmUsedHeapSpace.inc(heapUsed.getBytes());
                                Thread.sleep(1000);
                            }
                        } catch (InterruptedException ignored) {}
                    }
                });
                sampleThread.start();
                Thread.sleep(benchTime);
                long endTime = clock.millis();
                start.set(false);
                sampleThread.join();

                NodesStatsResponse response = client.admin().cluster().prepareNodesStats().setThreadPool(true).get();
                for (NodeStats nodeStats : response.getNodes()) {
                    for (ThreadPoolStats.Stats threadPoolStats : nodeStats.getThreadPool()) {
                        if ("watcher".equals(threadPoolStats.name())) {
                            stats.setWatcherThreadPoolStats(threadPoolStats);
                        }
                    }
                }
                client.admin().indices().prepareRefresh(HistoryStoreField.DATA_STREAM + "*").get();
                Script script = new Script(
                    ScriptType.INLINE,
                    Script.DEFAULT_SCRIPT_LANG,
                    "doc['trigger_event.schedule.triggered_time'].value - doc['trigger_event.schedule.scheduled_time'].value",
                    emptyMap()
                );
                assertResponse(
                    client.prepareSearch(HistoryStoreField.DATA_STREAM + "*")
                        .setQuery(QueryBuilders.rangeQuery("trigger_event.schedule.scheduled_time").gte(startTime).lte(endTime))
                        .addAggregation(terms("state").field("state"))
                        .addAggregation(histogram("delay").script(script).interval(10))
                        .addAggregation(percentiles("percentile_delay").script(script).percentiles(1.0, 20.0, 50.0, 80.0, 99.0)),
                    searchResponse -> {
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
                        new WatcherServiceRequestBuilder(ESTestCase.TEST_REQUEST_TIMEOUT, client).stop().get();
                    }
                );
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

    @SuppressForbidden(reason = "benchmark")
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
                name,
                ByteSizeValue.ofBytes(avgHeapUsed),
                watcherThreadPoolStats.rejected(),
                watcherThreadPoolStats.completed()
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
                name,
                executed != null ? executed.getDocCount() : 0,
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
                name,
                _1thPercentile,
                _20thPercentile,
                _50thPercentile,
                _80thPercentile,
                _99thPercentile
            );
        }
    }

}
