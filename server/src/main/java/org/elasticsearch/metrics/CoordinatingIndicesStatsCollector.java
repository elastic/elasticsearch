package org.elasticsearch.metrics;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author kyra.wkh
 */
public class CoordinatingIndicesStatsCollector {

    /**
     * indexName -- collector
     */
    private final Map<String, StatsCollector> collectorMap = new ConcurrentHashMap<>();

    public void register(String metricsName, MetricsConstant.MetricsType metricsType, String indexName) {
        assert indexName != null;

        StatsCollector collector = collectorMap.get(indexName);
        if (collector == null) {
            Map<String, CounterMetric> counterMetricMap = new ConcurrentHashMap<>();
            Map<String, MeanMetric> meanMetricMap = new ConcurrentHashMap<>();
            StatsCollector statsCollector = new StatsCollector(counterMetricMap, meanMetricMap);
            collectorMap.put(indexName, statsCollector);
            collector = statsCollector;
        }

        collector.register(metricsName, metricsType);
    }

    public void removeIndex(String indexName) {
        if (Strings.isNullOrEmpty(indexName)) {
            return;
        }
        collectorMap.remove(indexName);
    }


    public void collectCount(String indexName, String metricsName, long value) {
        StatsCollector collector = findStatsCollector(indexName);
        if (collector == null) {
            return;
        }
        collector.collectCount(metricsName, value);
    }

    public void collectMean(String indexName, String metricsName, long value) {
        StatsCollector collector = findStatsCollector(indexName);
        if (collector == null) {
            return;
        }
        collector.collectMean(metricsName, value);
    }

    public Map<String, StatsCollector> getCollectorMap() {
        return collectorMap;
    }

    private StatsCollector findStatsCollector(String indexName) {
        return collectorMap.get(indexName);
    }

    public class StatsCollector {

        public final Map<String, CounterMetric> counterMetricMap;
        public final Map<String, MeanMetric> meanMetricMap;

        public StatsCollector(Map<String, CounterMetric> counterMetricMap, Map<String, MeanMetric> meanMetricMap) {
            this.counterMetricMap = counterMetricMap;
            this.meanMetricMap = meanMetricMap;
        }

        public void collectCount(String metricsName, long value) {
            if (Strings.isNullOrEmpty(metricsName)) {
                return;
            }
            CounterMetric counterMetric = counterMetricMap.get(metricsName);
            if (counterMetric == null) {
                return;
            }
            counterMetric.inc(value);
        }

        public void collectMean(String metricsName, long value) {
            if (Strings.isNullOrEmpty(metricsName)) {
                return;
            }
            MeanMetric meanMetric = meanMetricMap.get(metricsName);
            if (meanMetric == null) {
                return;
            }
            meanMetric.inc(value);
        }

        public void register(String metricsName, MetricsConstant.MetricsType metricsType) {
            switch (metricsType) {
                case COUNTER:
                    counterMetricMap.putIfAbsent(metricsName, new CounterMetric());
                    break;
                case MEAN:
                    meanMetricMap.putIfAbsent(metricsName, new MeanMetric());
                    break;
                default:
                    break;
            }
        }
    }
}
