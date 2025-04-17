/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.threadpool;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.TaskExecutionTimeTrackingEsThreadPoolExecutor;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.hamcrest.CoreMatchers;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.threadpool.ThreadPool.DEFAULT_INDEX_AUTOSCALING_EWMA_ALPHA;
import static org.elasticsearch.threadpool.ThreadPool.WRITE_THREAD_POOLS_EWMA_ALPHA_SETTING;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.matchesRegex;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class SimpleThreadPoolIT extends ESIntegTestCase {
    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestTelemetryPlugin.class);
    }

    public void testThreadNames() throws Exception {
        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        Set<String> preNodeStartThreadNames = new HashSet<>();
        for (long l : threadBean.getAllThreadIds()) {
            ThreadInfo threadInfo = threadBean.getThreadInfo(l);
            if (threadInfo != null) {
                preNodeStartThreadNames.add(threadInfo.getThreadName());
            }
        }
        logger.info("pre node threads are {}", preNodeStartThreadNames);
        internalCluster().startNode();
        logger.info("do some indexing, flushing, optimize, and searches");
        int numDocs = randomIntBetween(2, 100);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; ++i) {
            builders[i] = prepareIndex("idx").setSource(
                jsonBuilder().startObject()
                    .field("str_value", "s" + i)
                    .array("str_values", new String[] { "s" + (i * 2), "s" + (i * 2 + 1) })
                    .field("l_value", i)
                    .array("l_values", new int[] { i * 2, i * 2 + 1 })
                    .field("d_value", i)
                    .array("d_values", new double[] { i * 2, i * 2 + 1 })
                    .endObject()
            );
        }
        indexRandom(true, builders);
        int numSearches = randomIntBetween(2, 100);
        for (int i = 0; i < numSearches; i++) {
            assertNoFailures(prepareSearch("idx").setQuery(QueryBuilders.termQuery("str_value", "s" + i)));
            assertNoFailures(prepareSearch("idx").setQuery(QueryBuilders.termQuery("l_value", i)));
        }
        Set<String> threadNames = new HashSet<>();
        for (long l : threadBean.getAllThreadIds()) {
            ThreadInfo threadInfo = threadBean.getThreadInfo(l);
            if (threadInfo != null) {
                threadNames.add(threadInfo.getThreadName());
            }
        }
        logger.info("post node threads are {}", threadNames);
        threadNames.removeAll(preNodeStartThreadNames);
        logger.info("post node *new* threads are {}", threadNames);
        for (String threadName : threadNames) {
            // ignore some shared threads we know that are created within the same VM, like the shared discovery one
            // or the ones that are occasionally come up from ESSingleNodeTestCase
            if (threadName.contains("[node_s_0]") // TODO: this can't possibly be right! single node and integ test are unrelated!
                || threadName.contains("Keep-Alive-Timer")
                || threadName.contains("readiness-service")
                || threadName.contains("JVMCI-native") // GraalVM Compiler Thread
                || threadName.contains("file-watcher[") // AbstractFileWatchingService
                || threadName.contains("FileSystemWatch")) { // FileSystemWatchService(Linux/Windows), FileSystemWatcher(BSD/AIX)
                continue;
            }
            String nodePrefix = "("
                + Pattern.quote(ESIntegTestCase.SUITE_CLUSTER_NODE_PREFIX)
                + "|"
                + Pattern.quote(ESIntegTestCase.TEST_CLUSTER_NODE_PREFIX)
                + ")";
            assertThat(threadName, matchesRegex("elasticsearch\\[" + nodePrefix + "\\d+\\].*"));
        }
    }

    public void testThreadPoolMetrics() throws Exception {
        internalCluster().startNode();

        final String dataNodeName = internalCluster().getRandomNodeName();
        final TestTelemetryPlugin plugin = internalCluster().getInstance(PluginsService.class, dataNodeName)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();

        logger.info("do some indexing, flushing, optimize, and searches");
        int numDocs = randomIntBetween(2, 100);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; ++i) {
            builders[i] = prepareIndex("idx").setSource(
                jsonBuilder().startObject()
                    .field("str_value", "s" + i)
                    .array("str_values", new String[] { "s" + (i * 2), "s" + (i * 2 + 1) })
                    .field("l_value", i)
                    .array("l_values", new int[] { i * 2, i * 2 + 1 })
                    .field("d_value", i)
                    .array("d_values", new double[] { i * 2, i * 2 + 1 })
                    .endObject()
            );
        }
        indexRandom(true, builders);
        int numSearches = randomIntBetween(2, 100);
        for (int i = 0; i < numSearches; i++) {
            assertNoFailures(prepareSearch("idx").setQuery(QueryBuilders.termQuery("str_value", "s" + i)));
            assertNoFailures(prepareSearch("idx").setQuery(QueryBuilders.termQuery("l_value", i)));
        }

        final var tp = internalCluster().getInstance(ThreadPool.class, dataNodeName);
        final var tps = new ThreadPoolStats[1];
        // wait for all threads to complete so that we get deterministic results
        waitUntil(() -> (tps[0] = tp.stats()).stats().stream().allMatch(s -> s.active() == 0));

        plugin.collect();
        ArrayList<String> registeredMetrics = plugin.getRegisteredMetrics(InstrumentType.LONG_GAUGE);
        registeredMetrics.addAll(plugin.getRegisteredMetrics(InstrumentType.LONG_ASYNC_COUNTER));

        tps[0].forEach(stats -> {
            Map<String, MetricDefinition<?>> metricDefinitions = Map.of(
                ThreadPool.THREAD_POOL_METRIC_NAME_COMPLETED,
                new MetricDefinition<>(stats.completed(), TestTelemetryPlugin::getLongAsyncCounterMeasurement, Measurement::getLong),
                ThreadPool.THREAD_POOL_METRIC_NAME_ACTIVE,
                new MetricDefinition<>(0L, TestTelemetryPlugin::getLongGaugeMeasurement, Measurement::getLong),
                ThreadPool.THREAD_POOL_METRIC_NAME_CURRENT,
                new MetricDefinition<>(0L, TestTelemetryPlugin::getLongGaugeMeasurement, Measurement::getLong),
                ThreadPool.THREAD_POOL_METRIC_NAME_LARGEST,
                new MetricDefinition<>((long) stats.largest(), TestTelemetryPlugin::getLongGaugeMeasurement, Measurement::getLong),
                ThreadPool.THREAD_POOL_METRIC_NAME_QUEUE,
                new MetricDefinition<>(0L, TestTelemetryPlugin::getLongGaugeMeasurement, Measurement::getLong)
            );

            // TaskExecutionTimeTrackingEsThreadPoolExecutor also publishes a utilization metric
            if (tp.executor(stats.name()) instanceof TaskExecutionTimeTrackingEsThreadPoolExecutor) {
                metricDefinitions = Maps.copyMapWithAddedEntry(
                    metricDefinitions,
                    ThreadPool.THREAD_POOL_METRIC_NAME_UTILIZATION,
                    new MetricDefinition<>(0.0d, TestTelemetryPlugin::getDoubleGaugeMeasurement, Measurement::getDouble)
                );
            }

            metricDefinitions = metricDefinitions.entrySet()
                .stream()
                .collect(Collectors.toUnmodifiableMap(e -> stats.name() + e.getKey(), Map.Entry::getValue));

            logger.info(
                "Measurements of `{}`: {}",
                stats.name(),
                metricDefinitions.entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getMeasurements(plugin, e.getKey())))
            );

            // Validate all metrics
            metricDefinitions.forEach((name, md) -> md.assertValid(plugin, name));
        });
    }

    private static class MetricDefinition<T extends Comparable<T>> {

        private final T minimumValue;
        private final BiFunction<TestTelemetryPlugin, String, List<Measurement>> metricExtractor;
        private final Function<Measurement, T> valueExtractor;

        MetricDefinition(
            T minimumValue,
            BiFunction<TestTelemetryPlugin, String, List<Measurement>> metricExtractor,
            Function<Measurement, T> valueExtractor
        ) {
            this.minimumValue = minimumValue;
            this.metricExtractor = metricExtractor;
            this.valueExtractor = valueExtractor;
        }

        public List<T> getMeasurements(TestTelemetryPlugin testTelemetryPlugin, String metricSuffix) {
            return metricExtractor.apply(testTelemetryPlugin, ThreadPool.THREAD_POOL_METRIC_PREFIX + metricSuffix)
                .stream()
                .map(valueExtractor)
                .toList();
        }

        public void assertValid(TestTelemetryPlugin testTelemetryPlugin, String metricSuffix) {
            List<T> metrics = getMeasurements(testTelemetryPlugin, metricSuffix);
            assertThat(
                ThreadPool.THREAD_POOL_METRIC_PREFIX + metricSuffix + " is populated",
                metrics,
                contains(greaterThanOrEqualTo(minimumValue))
            );
        }
    }

    public void testWriteThreadpoolEwmaAlphaSetting() {
        Settings settings = Settings.EMPTY;
        var ewmaAlpha = DEFAULT_INDEX_AUTOSCALING_EWMA_ALPHA;
        if (randomBoolean()) {
            ewmaAlpha = randomDoubleBetween(0.0, 1.0, true);
            settings = Settings.builder().put(WRITE_THREAD_POOLS_EWMA_ALPHA_SETTING.getKey(), ewmaAlpha).build();
        }
        var nodeName = internalCluster().startNode(settings);
        var threadPool = internalCluster().getInstance(ThreadPool.class, nodeName);
        for (var name : List.of(ThreadPool.Names.WRITE, ThreadPool.Names.SYSTEM_WRITE, ThreadPool.Names.SYSTEM_CRITICAL_WRITE)) {
            assertThat(threadPool.executor(name), instanceOf(TaskExecutionTimeTrackingEsThreadPoolExecutor.class));
            final var executor = (TaskExecutionTimeTrackingEsThreadPoolExecutor) threadPool.executor(name);
            assertThat(Double.compare(executor.getEwmaAlpha(), ewmaAlpha), CoreMatchers.equalTo(0));
        }
    }
}
