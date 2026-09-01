/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;

import java.util.Map;
import java.util.Objects;

/**
 * Fleet-wide telemetry for per-search datafeed result volumes. Used to assess whether
 * ES|QL's default row cap ({@code esql.query.result_truncation_default_size}, typically 1000)
 * is compatible with typical scroll and aggregation workloads.
 */
public final class DatafeedSearchTelemetry {

    public static final DatafeedSearchTelemetry NOOP = new DatafeedSearchTelemetry(MeterRegistry.NOOP);

    public static final String RESULTS_METRIC = "es.ml.datafeeds.search.results.total";
    public static final String FULL_PAGE_METRIC = "es.ml.datafeeds.search.full_page.total";

    static final String EXTRACTOR_TYPE_ATTRIBUTE = "extractor_type";
    static final String RESULTS_BUCKET_ATTRIBUTE = "es_results_bucket";

    public enum ExtractorType {
        SCROLL("scroll"),
        AGGREGATION("aggregation"),
        COMPOSITE("composite");

        private final String attributeValue;

        ExtractorType(String attributeValue) {
            this.attributeValue = attributeValue;
        }

        public String attributeValue() {
            return attributeValue;
        }
    }

    private final LongCounter searchResultsCounter;
    private final LongCounter fullPageCounter;

    public DatafeedSearchTelemetry(MeterRegistry meterRegistry) {
        Objects.requireNonNull(meterRegistry);
        this.searchResultsCounter = meterRegistry.registerLongCounter(
            RESULTS_METRIC,
            "Count of datafeed searches by extractor type and result-size bucket.",
            "searches"
        );
        this.fullPageCounter = meterRegistry.registerLongCounter(
            FULL_PAGE_METRIC,
            "Count of datafeed searches that returned a full page of results (scroll/composite pagination pressure).",
            "searches"
        );
    }

    public static ExtractorType classifyExtractorType(DatafeedConfig config, NamedXContentRegistry xContentRegistry) {
        if (config.hasAggregations() == false) {
            return ExtractorType.SCROLL;
        }
        if (config.hasCompositeAgg(xContentRegistry)) {
            return ExtractorType.COMPOSITE;
        }
        return ExtractorType.AGGREGATION;
    }

    public void recordSearchResults(ExtractorType extractorType, long resultCount) {
        searchResultsCounter.incrementBy(
            1,
            Map.of(EXTRACTOR_TYPE_ATTRIBUTE, extractorType.attributeValue(), RESULTS_BUCKET_ATTRIBUTE, resultsBucket(resultCount))
        );
    }

    public void recordFullPage(ExtractorType extractorType) {
        fullPageCounter.incrementBy(1, Map.of(EXTRACTOR_TYPE_ATTRIBUTE, extractorType.attributeValue()));
    }

    static String resultsBucket(long resultCount) {
        if (resultCount <= 0) {
            return "0";
        }
        if (resultCount < 100) {
            return "1_99";
        }
        if (resultCount < 500) {
            return "100_499";
        }
        if (resultCount < 1000) {
            return "500_999";
        }
        if (resultCount == 1000) {
            return "1000";
        }
        return "gt_1000";
    }
}
