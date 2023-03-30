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
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.RegexpQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.InternalComposite;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.metrics.InternalTopHits;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.index.IndexSettings.MAX_INNER_RESULT_WINDOW_SETTING;

/**
 * Reads data in a time series style way.
 */
public class TimeSeriesMetrics {
    private static final Logger logger = LogManager.getLogger(TimeSeriesMetrics.class);

    private final int bucketBatchSize;
    private final int docBatchSize;
    private final TimeValue staleness;
    private final Client client;
    private final String[] indices;
    private final List<String> dimensionFieldNames;
    private final Map<String, TimeSeriesParams.MetricType> metricFieldNames;

    TimeSeriesMetrics(
        int bucketBatchSize,
        int docBatchSize,
        TimeValue staleness,
        Client client,
        String[] indices,
        List<String> dimensionFieldNames,
        Map<String, TimeSeriesParams.MetricType> metricFieldNames
    ) {
        this.bucketBatchSize = bucketBatchSize;
        this.docBatchSize = docBatchSize;
        this.staleness = staleness;
        this.client = client;
        this.indices = indices;
        this.dimensionFieldNames = dimensionFieldNames;
        this.metricFieldNames = metricFieldNames;
    }

    /**
     * Called when metric data is available.
     */
    interface MetricsCallback {
        /**
         * Called when starting a new time series.
         */
        void onTimeSeriesStart(String metric, Map<String, Object> dimensions);

        /**
         * Called for each metric returned.
         *
         * @param time  the {@code @timestamp} recorded in the metric
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

    public enum TimeSeriesSelectorMatcher {
        EQUAL {
            @Override
            protected Predicate<String> matcher(String expression) {
                return expression::equals;
            }

            @Override
            protected QueryBuilder asQuery(String name, String value) {
                return new TermQueryBuilder(name, value);
            }
        },

        NOT_EQUAL {
            @Override
            protected Predicate<String> matcher(String expression) {
                return Predicate.not(expression::equals);
            }

            @Override
            protected QueryBuilder asQuery(String name, String value) {
                return new BoolQueryBuilder().mustNot(EQUAL.asQuery(name, value));
            }
        },

        RE_EQUAL {
            @Override
            protected Predicate<String> matcher(String expression) {
                return Pattern.compile(expression).asMatchPredicate();
            }

            @Override
            protected QueryBuilder asQuery(String name, String value) {
                return new RegexpQueryBuilder(name, value);
            }
        },

        RE_NOT_EQUAL {
            @Override
            protected Predicate<String> matcher(String expression) {
                return Predicate.not(RE_EQUAL.matcher(expression));
            }

            @Override
            protected QueryBuilder asQuery(String name, String value) {
                return new BoolQueryBuilder().mustNot(RE_EQUAL.asQuery(name, value));
            }
        };

        protected abstract Predicate<String> matcher(String expression);

        protected abstract QueryBuilder asQuery(String name, String value);
    }

    public static class TimeSeriesMetricSelector {
        private final TimeSeriesSelectorMatcher matcher;
        private final String value;

        public TimeSeriesMetricSelector(TimeSeriesSelectorMatcher matcher, String value) {
            this.matcher = matcher;
            this.value = value;
        }

        public Predicate<String> asPredicate() {
            return matcher.matcher(value);
        }
    }

    public static class TimeSeriesDimensionSelector {
        private final TimeSeriesSelectorMatcher matcher;
        private final String name;
        private final String value;

        public TimeSeriesDimensionSelector(String name, TimeSeriesSelectorMatcher matcher, String value) {
            this.name = name;
            this.matcher = matcher;
            this.value = value;
        }

        public QueryBuilder asQuery() {
            return matcher.asQuery(name, value);
        }
    }

    /**
     * Return the latest metrics before time within staleness period.
     *
     * @param metrics metrics selectors (ANDed together)
     * @param dimensions dimension selectors (ANDed together)
     * @param time the time before which the latest metrics are returned
     * @param callback callback used to return the metrics
     */
    public void latest(
        List<TimeSeriesMetricSelector> metrics,
        List<TimeSeriesDimensionSelector> dimensions,
        long time,
        MetricsCallback callback
    ) {
        retrieve(metrics, dimensions, time, null, null, callback);
    }

    /**
     * Return all metrics with range time period just before and including the time specified by the time parameter
     *
     * @param metrics metrics selectors (ANDed together)
     * @param dimensions dimension selectors (ANDed together)
     * @param time the time before which the results are returned
     * @param range range within which the results are returned
     * @param callback callback used to return the metrics
     */
    public void range(
        List<TimeSeriesMetricSelector> metrics,
        List<TimeSeriesDimensionSelector> dimensions,
        long time,
        TimeValue range,
        MetricsCallback callback
    ) {
        retrieve(metrics, dimensions, time, range, null, callback);
    }

    /**
     * Return all metrics with range time period just before and including the time specified by the time parameter
     *
     * @param metrics metrics selectors (ANDed together)
     * @param dimensions dimension selectors (ANDed together)
     * @param time the time before which the results are returned
     * @param range range within which the results are returned
     * @param step if not null, it makes this method equivalent to running the {@link #latest(List, List, long, MetricsCallback)} method
     *             several times while changing time from time to (time - range) with the step interval
     * @param callback callback used to return the metrics
     */
    public void range(
        List<TimeSeriesMetricSelector> metrics,
        List<TimeSeriesDimensionSelector> dimensions,
        long time,
        TimeValue range,
        TimeValue step,
        MetricsCallback callback
    ) {
        retrieve(metrics, dimensions, time, range, step, callback);
    }

    private void retrieve(
        List<TimeSeriesMetricSelector> metrics,
        List<TimeSeriesDimensionSelector> dimensions,
        long time,
        @Nullable TimeValue range,
        @Nullable TimeValue step,
        MetricsCallback callback
    ) {
        List<String> resolvedMetrics = resolveMetrics(metrics);
        final long from;
        final int size;
        if (range != null) {
            if (step != null) {
                from = time - range.getMillis() - staleness.getMillis();
                size = 0;
            } else {
                from = time - range.getMillis();
                size = docBatchSize;
            }
        } else {
            if (step != null) {
                throw new IllegalArgumentException("Cannot specify non-null step if range is null");
            } else {
                from = time - staleness.getMillis();
                size = 0;
            }
        }
        SearchRequest search = searchInRange(resolvedMetrics, dimensions, from, time, size);
        if (size > 0) {
            client.search(search, ActionListener.wrap(new SearchResponseHandler(resolvedMetrics, callback, search), callback::onError));
        } else {
            CompositeAggregationBuilder timeSeries = timeSeriesComposite();
            for (String metric : resolvedMetrics) {
                timeSeries.subAggregation(latestMetric(metric, time, range, step));
            }
            search.source().aggregation(timeSeries);
            logger.debug("Requesting batch of latest {}", search);
            client.search(
                search,
                ActionListener.wrap(new AggsResponseHandler(resolvedMetrics, callback, search, timeSeries), callback::onError)
            );
        }
    }

    /**
     * Handler for extracting results from {@link TimeSeriesMetrics#retrieve} from aggregations.
     */
    private class AggsResponseHandler implements CheckedConsumer<SearchResponse, RuntimeException> {
        private final List<String> resolvedMetrics;
        Map<String, Object> previousDimensions;
        private final MetricsCallback callback;
        private final SearchRequest search;
        private final CompositeAggregationBuilder timeSeries;

        AggsResponseHandler(
            List<String> resolvedMetrics,
            MetricsCallback callback,
            SearchRequest search,
            CompositeAggregationBuilder timeSeries
        ) {
            this.resolvedMetrics = resolvedMetrics;
            this.previousDimensions = null;
            this.callback = callback;
            this.search = search;
            this.timeSeries = timeSeries;
        }

        @Override
        public void accept(SearchResponse response) {
            // TODO shard error handling
            InternalComposite composite = response.getAggregations().get("time_series");
            for (String metric : resolvedMetrics) {
                previousDimensions = null;
                for (InternalComposite.InternalBucket bucket : composite.getBuckets()) {
                    Map<String, Object> dimensions = bucket.getKey()
                        .entrySet()
                        .stream()
                        .filter(e -> false == e.getKey().equals("@timestamp") && e.getValue() != null)
                        .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

                    if (false == Objects.equals(previousDimensions, dimensions)) {
                        previousDimensions = dimensions;
                        callback.onTimeSeriesStart(metric, dimensions);
                    }

                    InternalFilter filter = bucket.getAggregations().get(metric);
                    List<Aggregation> aggs = new ArrayList<>(filter.getAggregations().asList());
                    aggs.sort(Comparator.comparingLong(o -> Long.parseLong(o.getName())));
                    for (Aggregation agg : aggs) {
                        InternalFilter timeFilter = (InternalFilter) agg;
                        Long time;
                        if ("use_timestamp".equals(timeFilter.getName())) {
                            time = null;
                        } else {
                            time = Long.parseLong(timeFilter.getName());
                        }
                        InternalTopHits latest = timeFilter.getAggregations().get("results");
                        for (SearchHit hit : latest.getHits().getHits()) {
                            DocumentField metricField = hit.field(metric);
                            if (metricField == null) {
                                throw new IllegalStateException(
                                    "Cannot retrieve metric field [" + metric + "][" + bucket + "] from [" + response + "]"
                                );
                            }
                            long effectiveTime = time == null ? Long.parseLong(hit.field("@timestamp").getValue()) : time;
                            double value = ((Number) metricField.getValue()).doubleValue();
                            callback.onMetric(effectiveTime, value);
                        }
                    }
                }
            }
            if (composite.afterKey() == null) {
                callback.onSuccess();
            } else {
                timeSeries.aggregateAfter(composite.afterKey());
                client.search(search, ActionListener.wrap(this, callback::onError));
            }
        }
    }

    /**
     * Handler for extracting results from {@link TimeSeriesMetrics#retrieve} from search hits.
     */
    private class SearchResponseHandler implements CheckedConsumer<SearchResponse, RuntimeException> {
        private final List<String> resolvedMetrics;
        Map<String, Object> previousDimensions;
        private final MetricsCallback callback;
        private final SearchRequest search;

        SearchResponseHandler(List<String> resolvedMetrics, MetricsCallback callback, SearchRequest search) {
            this.resolvedMetrics = resolvedMetrics;
            this.previousDimensions = null;
            this.callback = callback;
            this.search = search;
        }

        @Override
        public void accept(SearchResponse response) {
            // TODO shard error handling
            SearchHit[] hits = response.getHits().getHits();
            for (String metric : resolvedMetrics) {
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
                    DocumentField metricField = hit.field(metric);
                    if (metricField == null) {
                        continue;
                    }
                    if (false == Objects.equals(previousDimensions, dimensions)) {
                        previousDimensions = dimensions;
                        callback.onTimeSeriesStart(metric, dimensions);
                    }
                    long time = Long.parseLong((String) hit.getSortValues()[dimensionFieldNames.size()]);
                    double value = ((Number) metricField.getValue()).doubleValue();
                    callback.onMetric(time, value);
                }
            }

            if (hits.length < docBatchSize) {
                callback.onSuccess();
            } else {
                search.source().searchAfter(hits[hits.length - 1].getSortValues());
                client.search(search, ActionListener.wrap(this, callback::onError));
            }
        }
    }

    private List<String> resolveMetrics(List<TimeSeriesMetricSelector> selectors) {
        Stream<String> metrics = metricFieldNames.keySet().stream();
        for (TimeSeriesMetricSelector selector : selectors) {
            metrics = metrics.filter(selector.asPredicate());
        }
        return metrics.toList();
    }

    private SearchRequest searchInRange(List<String> metrics, List<TimeSeriesDimensionSelector> dimensions, long from, long to, int size) {
        SearchRequest search = new SearchRequest(indices);
        BoolQueryBuilder builder = new BoolQueryBuilder();
        builder.must(
            new RangeQueryBuilder("@timestamp").format(DateFieldMapper.DEFAULT_DATE_TIME_NANOS_FORMATTER.pattern()).gt(from).lte(to)
        );
        for (String metric : metrics) {
            builder.should(new ExistsQueryBuilder(metric));
        }
        for (TimeSeriesDimensionSelector dimension : dimensions) {
            builder.must(dimension.asQuery());
        }
        search.source().query(builder);
        if (size > 0) {
            List<SortBuilder<?>> sorts = Stream.concat(
                dimensionFieldNames.stream().map(d -> new FieldSortBuilder(d).order(SortOrder.ASC)),
                Stream.of(new FieldSortBuilder("@timestamp").order(SortOrder.ASC).setFormat("epoch_millis"))
            ).collect(toList());
            search.source().sort(sorts);
            for (String metric : metrics) {
                search.source().fetchField(metric);
            }
        }
        search.source().size(size);
        search.source().trackTotalHits(false);
        return search;
    }

    private CompositeAggregationBuilder timeSeriesComposite(@Nullable Map<String, Object> afterKey) {
        Stream<CompositeValuesSourceBuilder<?>> sources = dimensionFieldNames.stream()
            .map(d -> new TermsValuesSourceBuilder(d).field(d).missingBucket(true));
        return new CompositeAggregationBuilder("time_series", sources.collect(toList())).aggregateAfter(afterKey).size(bucketBatchSize);
    }

    private CompositeAggregationBuilder timeSeriesComposite() {
        Stream<CompositeValuesSourceBuilder<?>> sources = dimensionFieldNames.stream()
            .map(d -> new TermsValuesSourceBuilder(d).field(d).missingBucket(true));
        return new CompositeAggregationBuilder("time_series", sources.collect(toList())).size(bucketBatchSize);
    }

    private AggregationBuilder latestMetric(String metric, long time, TimeValue range, TimeValue step) {
        if (step == null) {
            String aggKey = range == null ? Long.toString(time) : "use_timestamp";
            return new FilterAggregationBuilder(metric, new ExistsQueryBuilder(metric)).subAggregation(
                new FilterAggregationBuilder(aggKey, new MatchAllQueryBuilder()).subAggregation(
                    new TopHitsAggregationBuilder("results").sort(
                        new FieldSortBuilder("@timestamp").order(range == null ? SortOrder.DESC : SortOrder.ASC)
                    )
                        .fetchField(metric)
                        .fetchField(new FieldAndFormat("@timestamp", "epoch_millis"))
                        .size(range == null ? 1 : MAX_INNER_RESULT_WINDOW_SETTING.getDefault(Settings.EMPTY))
                )
            );
        } else {
            FilterAggregationBuilder metricAgg = new FilterAggregationBuilder(metric, new ExistsQueryBuilder(metric));
            long stepMillis = step.getMillis();
            long from = time - range.getMillis();
            long first = from / stepMillis;
            long last = time / stepMillis;
            for (int i = 0; i < (int) (last - first); i++) {
                metricAgg.subAggregation(
                    new FilterAggregationBuilder(
                        Long.toString((first + i + 1) * stepMillis),
                        new BoolQueryBuilder().filter(
                            new RangeQueryBuilder("@timestamp").format(DateFieldMapper.DEFAULT_DATE_TIME_NANOS_FORMATTER.pattern())
                                .gt((first + i + 1) * stepMillis - staleness.getMillis())
                                .lte((first + i + 1) * stepMillis)
                        ).filter(new ExistsQueryBuilder(metric))
                    ).subAggregation(
                        new TopHitsAggregationBuilder("results").sort(new FieldSortBuilder("@timestamp").order(SortOrder.DESC))
                            .fetchField(metric)
                            .fetchField(new FieldAndFormat("@timestamp", "epoch_millis"))
                            .size(1)
                    )
                );
            }
            return metricAgg;
        }
    }
}
