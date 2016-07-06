/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.benchmark.metrics;

import org.apache.commons.math3.stat.StatUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Statistics {
    @SafeVarargs
    public static List<StatisticsRecord> calculate(Collection<MetricsRecord>... metrics) {
        Map<String, List<MetricsRecord>> metricsPerOperation = new HashMap<>();


        for (Collection<MetricsRecord> metricCollection : metrics) {
            for (MetricsRecord metricsRecord : metricCollection) {
                if (!metricsPerOperation.containsKey(metricsRecord.getOperation())) {
                    metricsPerOperation.put(metricsRecord.getOperation(), new ArrayList<>());
                }
                metricsPerOperation.get(metricsRecord.getOperation()).add(metricsRecord);
            }
        }

        List<StatisticsRecord> stats = new ArrayList<>();

        for (Map.Entry<String, List<MetricsRecord>> operationAndMetrics : metricsPerOperation.entrySet()) {
            List<MetricsRecord> m = operationAndMetrics.getValue();
            double[] serviceTimes = new double[m.size()];
            int it = 0;
            long firstStart = Long.MAX_VALUE;
            long latestEnd = Long.MIN_VALUE;
            for (MetricsRecord metricsRecord : m) {
                // very primitive throughput calculation (does not consider warmup!)
                firstStart = Math.min(metricsRecord.getStartTimestamp(), firstStart);
                latestEnd = Math.max(metricsRecord.getStopTimestamp(), latestEnd);
                serviceTimes[it++] = TimeUnit.MILLISECONDS.convert(metricsRecord.getServiceTime(), TimeUnit.NANOSECONDS);
            }

            stats.add(new StatisticsRecord(operationAndMetrics.getKey(),
                m.stream().filter((r) -> r.isSuccess()).count(),
                m.stream().filter((r) -> !r.isSuccess()).count(),
                1.0d / TimeUnit.SECONDS.convert(latestEnd - firstStart, TimeUnit.NANOSECONDS),
                StatUtils.percentile(serviceTimes, 90.0d),
                StatUtils.percentile(serviceTimes, 95.0d),
                StatUtils.percentile(serviceTimes, 99.0d),
                StatUtils.percentile(serviceTimes, 99.9d)
            ));
        }
        return stats;
    }
}
