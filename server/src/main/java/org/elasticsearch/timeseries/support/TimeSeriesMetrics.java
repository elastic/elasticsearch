/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.timeseries.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.InternalComposite;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.metrics.InternalTopHits;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.time.temporal.TemporalAccessor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 * Reads data in a time series style way.
 */
public class TimeSeriesMetrics {
    private static final Logger logger = LogManager.getLogger();

    private final int bucketBatchSize;
    private final int docBatchSize;
    private final Client client;
    private final String[] indices;
    private final List<String> dimensionFieldNames;

    TimeSeriesMetrics(int bucketBatchSize, int docBatchSize, Client client, String[] indices, List<String> dimensionFieldNames) {
        this.bucketBatchSize = bucketBatchSize;
        this.docBatchSize = docBatchSize;
        this.client = client;
        this.indices = indices;
        this.dimensionFieldNames = dimensionFieldNames;
    }

    /**
     * Called when metric data is available.
     */
    interface MetricsCallback {
        /**
         * Called when starting a new time series.
         */
        void onTimeSeriesStart(Map<String, Object> dimensions);

        /**
         * Called for each metric returned.
         * @param time the {@code @timestamp} recorded in the metric
         * @param value the metric value
         */
        void onMetric(long time, double value);

        /**
         * Called when all requested metrics have been returned.
         */
        void onSuccess();

        /**
         * Called when there is any error fetching metrics. No more results
         * will be returned.
         */
        void onError(Exception e);
    }

    // TODO selector
    /**
     * Get the latest value for all time series in the range.
     */
    public void latestInRange(String metric, TemporalAccessor from, TemporalAccessor to, MetricsCallback callback) {
        latestInRanage(metric, from, to, null, null, null, callback);
    }

    // TODO selector
    /**
     * Get the latest value for all time series in many ranges.
     */
    public void latestInRanges(
        String metric,
        TemporalAccessor from,
        TemporalAccessor to,
        DateHistogramInterval step,
        MetricsCallback callback
    ) {
        latestInRanage(metric, from, to, step, null, null, callback);
    }

    /**
     * Get the latest value for all time series in one or many ranges.
     * @param step null if reading from a single range, the length of the range otherwise.
     */
    private void latestInRanage(
        String metric,
        TemporalAccessor from,
        TemporalAccessor to,
        @Nullable DateHistogramInterval step,
        @Nullable Map<String, Object> afterKey,
        @Nullable Map<String, Object> previousTimeSeries,
        MetricsCallback callback
    ) {
        // TODO test asserting forking
        SearchRequest search = searchInRange(from, to);
        search.source().size(0);
        search.source().trackTotalHits(false);
        search.source().aggregation(timeSeriesComposite(step, afterKey).subAggregation(latestMetric(metric)));
        logger.debug("Requesting batch of latest {}", search);
        client.search(
            search,
            ActionListener.wrap(
                new LatestInRangeResponseHandler(metric, callback, from, to, step, search, previousTimeSeries),
                callback::onError
            )
        );
    }

    private SearchRequest searchInRange(TemporalAccessor from, TemporalAccessor to) {
        SearchRequest search = new SearchRequest(indices);
        search.source()
            .query(
                new RangeQueryBuilder("@timestamp").format(DateFieldMapper.DEFAULT_DATE_TIME_NANOS_FORMATTER.pattern())
                    .gt(DateFieldMapper.DEFAULT_DATE_TIME_NANOS_FORMATTER.format(from))
                    .lte(DateFieldMapper.DEFAULT_DATE_TIME_NANOS_FORMATTER.format(to))
            );
        return search;
    }

    private CompositeAggregationBuilder timeSeriesComposite(@Nullable DateHistogramInterval step, @Nullable Map<String, Object> afterKey) {
        Stream<CompositeValuesSourceBuilder<?>> sources = dimensionFieldNames.stream()
            .map(d -> new TermsValuesSourceBuilder(d).field(d).missingBucket(true));
        if (step != null) {
            sources = Stream.concat(
                sources,
                /*
                 * offset(1) *includes* that last milli of the range and excludes
                 * the first milli of the range - effectively shifting us from a
                 * closed/open range to an open/closed range.
                 */
                Stream.of(new DateHistogramValuesSourceBuilder("@timestamp").field("@timestamp").fixedInterval(step).offset(1))
            );
        }
        return new CompositeAggregationBuilder("time_series", sources.collect(toList())).aggregateAfter(afterKey).size(bucketBatchSize);
    }

    private TopHitsAggregationBuilder latestMetric(String metric) {
        // TODO top metrics would almost certainly be better here but its in analytics.
        return new TopHitsAggregationBuilder("latest").sort(new FieldSortBuilder("@timestamp").order(SortOrder.DESC))
            .fetchField(metric)
            .fetchField(new FieldAndFormat("@timestamp", "epoch_millis"))
            .size(1);
    }

    /**
     * Handler for each page of results from {@link TimeSeriesMetrics#latestInRanage}.
     */
    private class LatestInRangeResponseHandler implements CheckedConsumer<SearchResponse, RuntimeException> {
        private final String metric;
        private final MetricsCallback callback;
        private final TemporalAccessor from;
        private final TemporalAccessor to;
        @Nullable
        private final DateHistogramInterval step;
        private final SearchRequest search;
        private Map<String, Object> previousDimensions;

        LatestInRangeResponseHandler(
            String metric,
            MetricsCallback callback,
            TemporalAccessor from,
            TemporalAccessor to,
            @Nullable DateHistogramInterval step,
            SearchRequest search,
            @Nullable Map<String, Object> previousDimensions
        ) {
            this.metric = metric;
            this.callback = callback;
            this.from = from;
            this.to = to;
            this.step = step;
            this.search = search;
            this.previousDimensions = previousDimensions;
        }

        @Override
        public void accept(SearchResponse response) {
            // TODO shard error handling
            InternalComposite composite = response.getAggregations().get("time_series");
            logger.debug("Received batch of latest {} with {} buckets", search, composite.getBuckets().size());
            for (InternalComposite.InternalBucket bucket : composite.getBuckets()) {
                Map<String, Object> dimensions = bucket.getKey()
                    .entrySet()
                    .stream()
                    .filter(e -> false == e.getKey().equals("@timestamp") && e.getValue() != null)
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
                if (false == Objects.equals(previousDimensions, dimensions)) {
                    previousDimensions = dimensions;
                    callback.onTimeSeriesStart(dimensions);
                }
                InternalTopHits latest = bucket.getAggregations().get("latest");
                SearchHit[] hits = latest.getHits().getHits();
                if (hits.length == 0) {
                    continue;
                }
                DocumentField metricField = hits[0].field(metric);
                if (metricField == null) {
                    // TODO skip in query?
                    continue;
                }
                long time = Long.parseLong((String) hits[0].field("@timestamp").getValue());
                double value = ((Number) metricField.getValue()).doubleValue();
                callback.onMetric(time, value);
            }
            if (composite.afterKey() == null) {
                callback.onSuccess();
            } else {
                latestInRanage(metric, from, to, step, composite.afterKey(), previousDimensions, callback);
            }
        }
    }

    // TODO selector
    /**
     * Return all values for all time series in a range.
     */
    public void valuesInRange(String metric, TemporalAccessor from, TemporalAccessor to, MetricsCallback listener) {
        valuesInRange(metric, from, to, null, null, listener);
    }

    /**
     * Search for a page of values for all time series in a range.
     */
    private void valuesInRange(
        String metric,
        TemporalAccessor from,
        TemporalAccessor to,
        Object[] searchAfter,
        Map<String, Object> previousTimeSeries,
        MetricsCallback callback
    ) {
        SearchRequest search = searchInRange(from, to);
        search.source().size(docBatchSize);
        search.source().trackTotalHits(false);
        List<SortBuilder<?>> sorts = Stream.concat(
            dimensionFieldNames.stream().map(d -> new FieldSortBuilder(d).order(SortOrder.ASC)),
            Stream.of(new FieldSortBuilder("@timestamp").order(SortOrder.ASC).setFormat("epoch_millis"))
        ).collect(toList());
        search.source().sort(sorts);
        if (searchAfter != null) {
            search.source().searchAfter(searchAfter);
        }
        search.source().fetchField(metric);
        client.search(
            search,
            ActionListener.wrap(new ValuesInRangeResponseHandler(metric, callback, from, to, search, previousTimeSeries), callback::onError)
        );
    }

    /**
     * Handler for {@link TimeSeriesMetrics#valuesInRange}.
     */
    private class ValuesInRangeResponseHandler implements CheckedConsumer<SearchResponse, RuntimeException> {
        private final String metric;
        private final MetricsCallback callback;
        private final TemporalAccessor from;
        private final TemporalAccessor to;
        private final SearchRequest search;
        private Map<String, Object> previousDimensions;

        ValuesInRangeResponseHandler(
            String metric,
            MetricsCallback callback,
            TemporalAccessor from,
            TemporalAccessor to,
            SearchRequest search,
            @Nullable Map<String, Object> previousDimensions
        ) {
            this.metric = metric;
            this.callback = callback;
            this.from = from;
            this.to = to;
            this.search = search;
            this.previousDimensions = previousDimensions;
        }

        @Override
        public void accept(SearchResponse response) {
            // TODO shard error handling
            logger.debug("Received batch of values {} with {} docs", search, response.getHits().getHits().length);
            SearchHit[] hits = response.getHits().getHits();
            for (SearchHit hit : hits) {
                /*
                 * Read the dimensions out of the sort. This is useful because
                 * we already need the sort so we can do proper pagination but
                 * it also converts numeric dimension into a Long which is nice
                 * and consistent.
                 */
                Map<String, Object> dimensions = new HashMap<>();
                for (int d = 0; d < dimensionFieldNames.size(); d++) {
                    Object dimensionValue = hit.getSortValues()[d];
                    if (dimensionValue != null) {
                        dimensions.put(dimensionFieldNames.get(d), dimensionValue);
                    }
                }
                if (false == Objects.equals(previousDimensions, dimensions)) {
                    previousDimensions = dimensions;
                    callback.onTimeSeriesStart(dimensions);
                }
                DocumentField metricField = hit.field(metric);
                if (metricField == null) {
                    // TODO skip in query?
                    continue;
                }
                long time = Long.parseLong((String) hit.getSortValues()[dimensionFieldNames.size()]);
                double value = ((Number) metricField.getValue()).doubleValue();
                callback.onMetric(time, value);
            }

            if (hits.length < docBatchSize) {
                callback.onSuccess();
            } else {
                valuesInRange(metric, from, to, hits[hits.length - 1].getSortValues(), previousDimensions, callback);
            }
        }
    }
}
