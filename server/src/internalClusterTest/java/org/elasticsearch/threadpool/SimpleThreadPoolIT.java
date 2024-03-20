/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.threadpool;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;

import static java.util.function.Function.identity;
import static org.elasticsearch.common.util.Maps.toUnmodifiableSortedMap;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.in;
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

    // temporarily re-enable to gather
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
        ThreadPoolStats tps = tp.stats();
        plugin.collect();
        ArrayList<String> registeredMetrics = plugin.getRegisteredMetrics(InstrumentType.LONG_GAUGE);
        registeredMetrics.addAll(plugin.getRegisteredMetrics(InstrumentType.LONG_ASYNC_COUNTER));

        tps.forEach(stats -> {
            Map<String, Long> threadPoolStats = List.of(
                Map.entry(ThreadPool.THREAD_POOL_METRIC_NAME_COMPLETED, stats.completed()),
                Map.entry(ThreadPool.THREAD_POOL_METRIC_NAME_ACTIVE, (long) stats.active()),
                Map.entry(ThreadPool.THREAD_POOL_METRIC_NAME_CURRENT, (long) stats.threads()),
                Map.entry(ThreadPool.THREAD_POOL_METRIC_NAME_LARGEST, (long) stats.largest()),
                Map.entry(ThreadPool.THREAD_POOL_METRIC_NAME_QUEUE, (long) stats.queue())
            ).stream().collect(toUnmodifiableSortedMap(Entry::getKey, Entry::getValue));

            Function<String, List<Long>> measurementExtractor = name -> {
                String metricName = ThreadPool.THREAD_POOL_METRIC_PREFIX + stats.name() + name;
                assertThat(metricName, in(registeredMetrics));

                List<Measurement> measurements = name.equals(ThreadPool.THREAD_POOL_METRIC_NAME_COMPLETED)
                    ? plugin.getLongAsyncCounterMeasurement(metricName)
                    : plugin.getLongGaugeMeasurement(metricName);
                return measurements.stream().map(Measurement::getLong).toList();
            };

            Map<String, List<Long>> measurements = threadPoolStats.keySet()
                .stream()
                .collect(toUnmodifiableSortedMap(identity(), measurementExtractor));

            logger.info("Stats of `{}`: {}", stats.name(), threadPoolStats);
            logger.info("Measurements of `{}`: {}", stats.name(), measurements);

            threadPoolStats.forEach(
                (metric, value) -> assertThat(measurements, hasEntry(equalTo(metric), contains(greaterThanOrEqualTo(value))))
            );
        });
    }

}
