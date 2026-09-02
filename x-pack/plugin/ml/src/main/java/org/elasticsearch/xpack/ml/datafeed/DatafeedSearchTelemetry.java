/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Fleet-wide telemetry for per-search datafeed result volumes. Used to assess whether
 * ES|QL's row caps are compatible with typical scroll and aggregation workloads: the implicit
 * default {@code esql.query.result_truncation_default_size} (1000) and the hard ceiling
 * {@code esql.query.result_truncation_max_size} (10000 by default, raisable to 1000000). Result
 * and page-size buckets carry boundaries at 1000, 10000, and 1000000 so the fleet distribution can
 * tell apart workloads that fit the default, workloads that would fit a raised default (up to the
 * default ceiling), workloads that need the ceiling raised (up to its 1000000 maximum), and
 * workloads that exceed even that.
 */
public final class DatafeedSearchTelemetry {

    public static final DatafeedSearchTelemetry NOOP = new DatafeedSearchTelemetry(MeterRegistry.NOOP);

    public static final String RESPONSES_METRIC = "es.ml.datafeeds.search.responses.total";
    public static final String FULL_PAGE_METRIC = "es.ml.datafeeds.search.full_page.total";

    public static final String EXTRACTOR_TYPE_ATTRIBUTE = "es_extractor_type";
    public static final String RESULTS_BUCKET_ATTRIBUTE = "es_results_bucket";
    public static final String PAGE_SIZE_BUCKET_ATTRIBUTE = "es_page_size_bucket";

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

    public enum PageSizeBucket {
        LT_1000("lt_1000"),
        EQ_1000("eq_1000"),
        GT_1000_LT_10000("1001_9999"),
        EQ_10000("eq_10000"),
        GT_10000_LE_1000000("10001_1000000"),
        GT_1000000("gt_1000000");

        private final String attributeValue;

        PageSizeBucket(String attributeValue) {
            this.attributeValue = attributeValue;
        }

        public String attributeValue() {
            return attributeValue;
        }
    }

    private final LongCounter searchResponsesCounter;
    private final LongCounter fullPageCounter;

    public DatafeedSearchTelemetry(MeterRegistry meterRegistry) {
        Objects.requireNonNull(meterRegistry);
        this.searchResponsesCounter = meterRegistry.registerLongCounter(
            RESPONSES_METRIC,
            "Count of datafeed search responses by extractor type, result-size bucket, and page-size bucket.",
            "responses"
        );
        this.fullPageCounter = meterRegistry.registerLongCounter(
            FULL_PAGE_METRIC,
            "Count of datafeed search responses that filled a page and were followed by another non-empty page.",
            "responses"
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

    public static PageSizeBucket pageSizeBucket(int pageSize) {
        if (pageSize < 1000) {
            return PageSizeBucket.LT_1000;
        }
        if (pageSize == 1000) {
            return PageSizeBucket.EQ_1000;
        }
        if (pageSize < 10000) {
            return PageSizeBucket.GT_1000_LT_10000;
        }
        if (pageSize == 10000) {
            return PageSizeBucket.EQ_10000;
        }
        if (pageSize <= 1000000) {
            return PageSizeBucket.GT_10000_LE_1000000;
        }
        return PageSizeBucket.GT_1000000;
    }

    public void recordSearchResults(ExtractorType extractorType, long resultCount, @Nullable PageSizeBucket pageSizeBucket) {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(EXTRACTOR_TYPE_ATTRIBUTE, extractorType.attributeValue());
        attributes.put(RESULTS_BUCKET_ATTRIBUTE, resultsBucket(resultCount));
        if (pageSizeBucket != null) {
            attributes.put(PAGE_SIZE_BUCKET_ATTRIBUTE, pageSizeBucket.attributeValue());
        }
        searchResponsesCounter.incrementBy(1, attributes);
    }

    public void recordFullPage(ExtractorType extractorType, PageSizeBucket pageSizeBucket) {
        fullPageCounter.incrementBy(
            1,
            Map.of(EXTRACTOR_TYPE_ATTRIBUTE, extractorType.attributeValue(), PAGE_SIZE_BUCKET_ATTRIBUTE, pageSizeBucket.attributeValue())
        );
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
        if (resultCount < 10000) {
            return "1001_9999";
        }
        if (resultCount == 10000) {
            return "10000";
        }
        if (resultCount <= 1000000) {
            return "10001_1000000";
        }
        return "gt_1000000";
    }
}
