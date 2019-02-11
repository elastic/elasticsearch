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

public final class MetricsCalculator {
    public static List<Metrics> calculate(Collection<Sample> samples) {
        Map<String, List<Sample>> samplesPerOperation = groupByOperation(samples);
        return calculateMetricsPerOperation(samplesPerOperation);
    }

    private static Map<String, List<Sample>> groupByOperation(Collection<Sample> samples) {
        Map<String, List<Sample>> samplesPerOperation = new HashMap<>();

        for (Sample sample : samples) {
            if (!samplesPerOperation.containsKey(sample.getOperation())) {
                samplesPerOperation.put(sample.getOperation(), new ArrayList<>());
            }
            samplesPerOperation.get(sample.getOperation()).add(sample);
        }
        return samplesPerOperation;
    }

    private static List<Metrics> calculateMetricsPerOperation(Map<String, List<Sample>> samplesPerOperation) {
        List<Metrics> metrics = new ArrayList<>();
        for (Map.Entry<String, List<Sample>> operationAndMetrics : samplesPerOperation.entrySet()) {
            List<Sample> samples = operationAndMetrics.getValue();
            double[] serviceTimes = new double[samples.size()];
            double[] latencies = new double[samples.size()];
            int it = 0;
            long firstStart = Long.MAX_VALUE;
            long latestEnd = Long.MIN_VALUE;
            for (Sample sample : samples) {
                firstStart = Math.min(sample.getStartTimestamp(), firstStart);
                latestEnd = Math.max(sample.getStopTimestamp(), latestEnd);
                serviceTimes[it] = sample.getServiceTime();
                latencies[it] = sample.getLatency();
                it++;
            }

            metrics.add(new Metrics(operationAndMetrics.getKey(),
                samples.stream().filter((r) -> r.isSuccess()).count(),
                samples.stream().filter((r) -> !r.isSuccess()).count(),
                // throughput calculation is based on the total (Wall clock) time it took to generate all samples
                calculateThroughput(samples.size(), latestEnd - firstStart),
                // convert ns -> ms without losing precision
                StatUtils.percentile(serviceTimes, 50.0d) / TimeUnit.MILLISECONDS.toNanos(1L),
                StatUtils.percentile(serviceTimes, 90.0d) / TimeUnit.MILLISECONDS.toNanos(1L),
                StatUtils.percentile(serviceTimes, 95.0d) / TimeUnit.MILLISECONDS.toNanos(1L),
                StatUtils.percentile(serviceTimes, 99.0d) / TimeUnit.MILLISECONDS.toNanos(1L),
                StatUtils.percentile(serviceTimes, 99.9d) / TimeUnit.MILLISECONDS.toNanos(1L),
                StatUtils.percentile(serviceTimes, 99.99d) / TimeUnit.MILLISECONDS.toNanos(1L),
                StatUtils.percentile(latencies, 50.0d) / TimeUnit.MILLISECONDS.toNanos(1L),
                StatUtils.percentile(latencies, 90.0d) / TimeUnit.MILLISECONDS.toNanos(1L),
                StatUtils.percentile(latencies, 95.0d) / TimeUnit.MILLISECONDS.toNanos(1L),
                StatUtils.percentile(latencies, 99.0d) / TimeUnit.MILLISECONDS.toNanos(1L),
                StatUtils.percentile(latencies, 99.9d) / TimeUnit.MILLISECONDS.toNanos(1L),
                StatUtils.percentile(latencies, 99.99d) / TimeUnit.MILLISECONDS.toNanos(1L)));
        }
        return metrics;
    }

    private static double calculateThroughput(int sampleSize, double duration) {
        return sampleSize * (TimeUnit.SECONDS.toNanos(1L) / duration);
    }
}
