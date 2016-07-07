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
    public static List<Metrics> calculate(Collection<Sample> samples, int operationsPerIteration) {
        Map<String, List<Sample>> samplesPerOperation = new HashMap<>();

        for (Sample sample : samples) {
            if (!samplesPerOperation.containsKey(sample.getOperation())) {
                samplesPerOperation.put(sample.getOperation(), new ArrayList<>());
            }
            samplesPerOperation.get(sample.getOperation()).add(sample);
        }

        List<Metrics> metrics = new ArrayList<>();

        for (Map.Entry<String, List<Sample>> operationAndMetrics : samplesPerOperation.entrySet()) {
            List<Sample> m = operationAndMetrics.getValue();
            double[] serviceTimes = new double[m.size()];
            int it = 0;
            long firstStart = Long.MAX_VALUE;
            long latestEnd = Long.MIN_VALUE;
            for (Sample sample : m) {
                firstStart = Math.min(sample.getStartTimestamp(), firstStart);
                latestEnd = Math.max(sample.getStopTimestamp(), latestEnd);
                serviceTimes[it++] = TimeUnit.MILLISECONDS.convert(sample.getServiceTime(), TimeUnit.NANOSECONDS);
            }

            metrics.add(new Metrics(operationAndMetrics.getKey(),
                m.stream().filter((r) -> r.isSuccess()).count(),
                m.stream().filter((r) -> !r.isSuccess()).count(),
                // throughput calculation is based on the total (Wall clock) time it took to generate all samples
                samples.size() / TimeUnit.SECONDS.convert(latestEnd - firstStart, TimeUnit.NANOSECONDS),
                StatUtils.percentile(serviceTimes, 90.0d),
                StatUtils.percentile(serviceTimes, 95.0d),
                StatUtils.percentile(serviceTimes, 99.0d),
                StatUtils.percentile(serviceTimes, 99.9d)
            ));
        }
        return metrics;
    }
}
