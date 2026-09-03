/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Fleet-wide telemetry for per-search datafeed result volumes. Used to assess whether
 * ES|QL's row caps are compatible with typical scroll and aggregation workloads: the implicit
 * default {@code esql.query.result_truncation_default_size} (1000) and the hard ceiling
 * {@code esql.query.result_truncation_max_size} (10000 by default, raisable to 1000000).
 * Result-count and page-size histograms share explicit bucket boundaries at 1000, 10000, and
 * 1000000 so the fleet distribution can tell apart workloads that fit the default, workloads
 * that would fit a raised default (up to the default ceiling), workloads that need the ceiling
 * raised (up to its 1000000 maximum), and workloads that exceed even that.
 */
public final class DatafeedSearchTelemetry {

    public static final DatafeedSearchTelemetry NOOP = new DatafeedSearchTelemetry(MeterRegistry.NOOP);

    public static final String RESULT_COUNT_METRIC = "es.ml.datafeeds.search.result_count.histogram";
    public static final String PAGE_SIZE_METRIC = "es.ml.datafeeds.search.page_size.histogram";
    public static final String FULL_PAGE_METRIC = "es.ml.datafeeds.search.full_page.total";

    public static final String EXTRACTOR_TYPE_ATTRIBUTE = "es_extractor_type";

    /**
     * Upper-inclusive bucket boundaries aligned with ES|QL row-cap decision points (1000, 10000, 1000000).
     */
    public static final List<Long> SEARCH_VOLUME_BUCKET_BOUNDARIES = List.of(0L, 99L, 499L, 999L, 1000L, 9999L, 10000L, 1000000L);

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

    private final LongHistogram resultCountHistogram;
    private final LongHistogram pageSizeHistogram;
    private final LongCounter fullPageCounter;

    public DatafeedSearchTelemetry(MeterRegistry meterRegistry) {
        Objects.requireNonNull(meterRegistry);
        this.resultCountHistogram = meterRegistry.registerLongHistogram(
            RESULT_COUNT_METRIC,
            "Per-response result count distribution for datafeed searches, by extractor type.",
            "documents",
            SEARCH_VOLUME_BUCKET_BOUNDARIES
        );
        this.pageSizeHistogram = meterRegistry.registerLongHistogram(
            PAGE_SIZE_METRIC,
            "Per-response configured page size distribution for scroll and composite datafeed searches, by extractor type.",
            "documents",
            SEARCH_VOLUME_BUCKET_BOUNDARIES
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

    public void recordSearchResults(ExtractorType extractorType, long resultCount, @Nullable Integer pageSize) {
        Map<String, Object> attributes = Map.of(EXTRACTOR_TYPE_ATTRIBUTE, extractorType.attributeValue());
        resultCountHistogram.record(resultCount, attributes);
        if (pageSize != null) {
            pageSizeHistogram.record(pageSize, attributes);
        }
    }

    public void recordFullPage(ExtractorType extractorType) {
        fullPageCounter.incrementBy(1, Map.of(EXTRACTOR_TYPE_ATTRIBUTE, extractorType.attributeValue()));
    }
}
