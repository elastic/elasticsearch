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
package org.elasticsearch.client.rollup;

import org.elasticsearch.client.rollup.job.config.DateHistogramGroupConfig;
import org.elasticsearch.client.rollup.job.config.GroupConfig;
import org.elasticsearch.client.rollup.job.config.HistogramGroupConfig;
import org.elasticsearch.client.rollup.job.config.MetricConfig;
import org.elasticsearch.client.rollup.job.config.RollupJobConfig;
import org.elasticsearch.client.rollup.job.config.RollupJobConfigTests;
import org.elasticsearch.client.rollup.job.config.TermsGroupConfig;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

abstract class RollupCapsResponseTestCase<T> extends ESTestCase {

    protected Map<String, RollableIndexCaps> indices;

    protected abstract T createTestInstance();

    protected abstract void toXContent(T response, XContentBuilder builder) throws IOException;

    protected abstract T fromXContent(XContentParser parser) throws IOException;

    protected Predicate<String> randomFieldsExcludeFilter() {
        return field -> false;
    }

    protected String[] shuffleFieldsExceptions() {
        return Strings.EMPTY_ARRAY;
    }

    public void testFromXContent() throws IOException {
        xContentTester(
            this::createParser,
            this::createTestInstance,
            this::toXContent,
            this::fromXContent)
            .supportsUnknownFields(true)
            .randomFieldsExcludeFilter(randomFieldsExcludeFilter())
            .shuffleFieldsExceptions(shuffleFieldsExceptions())
            .test();
    }

    @Before
    private void setupIndices() throws IOException {
        int numIndices = randomIntBetween(1,5);
        indices = new HashMap<>(numIndices);
        for (int i = 0; i < numIndices; i++) {
            String indexName = "index_" + randomAlphaOfLength(10);
            int numJobs = randomIntBetween(1,5);
            List<RollupJobCaps> jobs = new ArrayList<>(numJobs);
            for (int j = 0; j < numJobs; j++) {
                RollupJobConfig config = RollupJobConfigTests.randomRollupJobConfig(randomAlphaOfLength(10));
                jobs.add(new RollupJobCaps(config.getId(), config.getIndexPattern(),
                    config.getRollupIndex(), createRollupFieldCaps(config)));
            }
            RollableIndexCaps cap = new RollableIndexCaps(indexName, jobs);
            indices.put(indexName, cap);
        }
    }

    /**
     * Lifted from core's RollupJobCaps, so that we can test without having to include this actual logic in the request
     */
    private static Map<String, RollupJobCaps.RollupFieldCaps> createRollupFieldCaps(final RollupJobConfig rollupJobConfig) {
        final Map<String, List<Map<String, Object>>> tempFieldCaps = new HashMap<>();

        final GroupConfig groupConfig = rollupJobConfig.getGroupConfig();
        if (groupConfig != null) {
            // Create RollupFieldCaps for the date histogram
            final DateHistogramGroupConfig dateHistogram = groupConfig.getDateHistogram();
            final Map<String, Object> dateHistogramAggCap = new HashMap<>();
            dateHistogramAggCap.put("agg", DateHistogramAggregationBuilder.NAME);
            dateHistogramAggCap.put("interval", dateHistogram.getInterval().toString());
            if (dateHistogram.getDelay() != null) {
                dateHistogramAggCap.put("delay", dateHistogram.getDelay().toString());
            }
            dateHistogramAggCap.put("time_zone", dateHistogram.getTimeZone());

            List<Map<String, Object>> dateAggCaps = tempFieldCaps.getOrDefault(dateHistogram.getField(), new ArrayList<>());
            dateAggCaps.add(dateHistogramAggCap);
            tempFieldCaps.put(dateHistogram.getField(), dateAggCaps);

            // Create RollupFieldCaps for the histogram
            final HistogramGroupConfig histogram = groupConfig.getHistogram();
            if (histogram != null) {
                final Map<String, Object> histogramAggCap = new HashMap<>();
                histogramAggCap.put("agg", HistogramAggregationBuilder.NAME);
                histogramAggCap.put("interval", histogram.getInterval());
                Arrays.stream(rollupJobConfig.getGroupConfig().getHistogram().getFields()).forEach(field -> {
                    List<Map<String, Object>> caps = tempFieldCaps.getOrDefault(field, new ArrayList<>());
                    caps.add(histogramAggCap);
                    tempFieldCaps.put(field, caps);
                });
            }

            // Create RollupFieldCaps for the term
            final TermsGroupConfig terms = groupConfig.getTerms();
            if (terms != null) {
                final Map<String, Object> termsAggCap = singletonMap("agg", TermsAggregationBuilder.NAME);
                Arrays.stream(rollupJobConfig.getGroupConfig().getTerms().getFields()).forEach(field -> {
                    List<Map<String, Object>> caps = tempFieldCaps.getOrDefault(field, new ArrayList<>());
                    caps.add(termsAggCap);
                    tempFieldCaps.put(field, caps);
                });
            }
        }

        // Create RollupFieldCaps for the metrics
        final List<MetricConfig> metricsConfig = rollupJobConfig.getMetricsConfig();
        if (metricsConfig.size() > 0) {
            rollupJobConfig.getMetricsConfig().forEach(metricConfig -> {
                final List<Map<String, Object>> metrics = metricConfig.getMetrics().stream()
                    .map(metric -> singletonMap("agg", (Object) metric))
                    .collect(Collectors.toList());
                metrics.forEach(m -> {
                    List<Map<String, Object>> caps = tempFieldCaps
                        .getOrDefault(metricConfig.getField(), new ArrayList<>());
                    caps.add(m);
                    tempFieldCaps.put(metricConfig.getField(), caps);
                });
            });
        }

        return Collections.unmodifiableMap(tempFieldCaps.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey,
                e -> new RollupJobCaps.RollupFieldCaps(e.getValue()))));
    }
}
