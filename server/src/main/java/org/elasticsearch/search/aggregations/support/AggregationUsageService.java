/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.node.ReportingService;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

public class AggregationUsageService implements ReportingService<AggregationInfo> {
    private static final String ES_SEARCH_QUERY_AGGREGATIONS_TOTAL_COUNT = "es.search.query.aggregations.total";
    private final LongCounter aggregationsUsageCounter;
    private final Map<String, Map<String, LongAdder>> aggs;
    private final AggregationInfo info;

    public static final String OTHER_SUBTYPE = "other";

    public static class Builder {
        private final Map<String, Map<String, LongAdder>> aggs;
        private final MeterRegistry meterRegistry;

        public Builder() {
            this(MeterRegistry.NOOP);
        }

        public Builder(MeterRegistry meterRegistry) {
            aggs = new HashMap<>();
            assert meterRegistry != null;
            this.meterRegistry = meterRegistry;
        }

        public void registerAggregationUsage(String aggregationName) {
            registerAggregationUsage(aggregationName, OTHER_SUBTYPE);
        }

        public void registerAggregationUsage(String aggregationName, String valuesSourceType) {
            Map<String, LongAdder> subAgg = aggs.computeIfAbsent(aggregationName, k -> new HashMap<>());
            if (subAgg.put(valuesSourceType, new LongAdder()) != null) {
                throw new IllegalArgumentException(
                    "stats for aggregation [" + aggregationName + "][" + valuesSourceType + "] already registered"
                );
            }
        }

        public AggregationUsageService build() {
            return new AggregationUsageService(this);
        }
    }

    // Attribute names for the metric

    private AggregationUsageService(Builder builder) {
        this.aggs = builder.aggs;
        info = new AggregationInfo(aggs);
        this.aggregationsUsageCounter = builder.meterRegistry.registerLongCounter(
            ES_SEARCH_QUERY_AGGREGATIONS_TOTAL_COUNT,
            "Aggregations usage",
            "count"
        );
    }

    public void incAggregationUsage(String aggregationName, String valuesSourceType) {
        Map<String, LongAdder> valuesSourceMap = aggs.get(aggregationName);
        // Not all aggs register their usage at the moment we also don't register them in test context
        if (valuesSourceMap != null) {
            LongAdder adder = valuesSourceMap.get(valuesSourceType);
            if (adder != null) {
                adder.increment();
            }
            assert adder != null : "Unknown subtype [" + aggregationName + "][" + valuesSourceType + "]";
        }
        assert valuesSourceMap != null : "Unknown aggregation [" + aggregationName + "][" + valuesSourceType + "]";
        // tests will have a no-op implementation here
        String VALUES_SOURCE_KEY = "values_source";
        String AGGREGATION_NAME_KEY = "aggregation_name";
        aggregationsUsageCounter.incrementBy(1, Map.of(AGGREGATION_NAME_KEY, aggregationName, VALUES_SOURCE_KEY, valuesSourceType));
    }

    public Map<String, Object> getUsageStats() {
        Map<String, Object> aggsUsageMap = new HashMap<>();
        aggs.forEach((name, agg) -> {
            Map<String, Long> aggUsageMap = new HashMap<>();
            agg.forEach((k, v) -> {
                long val = v.longValue();
                if (val > 0) {
                    aggUsageMap.put(k, val);
                }
            });
            if (aggUsageMap.isEmpty() == false) {
                aggsUsageMap.put(name, aggUsageMap);
            }
        });
        return aggsUsageMap;
    }

    @Override
    public AggregationInfo info() {
        return info;
    }
}
