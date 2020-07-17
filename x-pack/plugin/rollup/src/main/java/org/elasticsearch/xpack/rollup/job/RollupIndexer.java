/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.job;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.HistogramValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.indexing.AsyncTwoPhaseIndexer;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.indexing.IterationResult;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.core.rollup.job.DateHistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.GroupConfig;
import org.elasticsearch.xpack.core.rollup.job.HistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;
import org.elasticsearch.xpack.core.rollup.job.RollupIndexerJobStats;
import org.elasticsearch.xpack.core.rollup.job.RollupJob;
import org.elasticsearch.xpack.core.rollup.job.RollupJobConfig;
import org.elasticsearch.xpack.core.rollup.job.TermsGroupConfig;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.core.rollup.RollupField.formatFieldName;

/**
 * An abstract implementation of {@link AsyncTwoPhaseIndexer} that builds a rollup index incrementally.
 */
public abstract class RollupIndexer extends AsyncTwoPhaseIndexer<Map<String, Object>, RollupIndexerJobStats> {
    static final String AGGREGATION_NAME = RollupField.NAME;

    private final RollupJob job;
    private final CompositeAggregationBuilder compositeBuilder;
    private long maxBoundary;

    /**
     * Ctr
     * @param threadPool ThreadPool to use to fire the first request of a background job.
     * @param executorName Name of the executor to use to fire the first request of a background job.
     * @param job The rollup job
     * @param initialState Initial state for the indexer
     * @param initialPosition The last indexed bucket of the task
     */
    RollupIndexer(ThreadPool threadPool, String executorName, RollupJob job, AtomicReference<IndexerState> initialState,
                  Map<String, Object> initialPosition) {
        this(threadPool, executorName, job, initialState, initialPosition, new RollupIndexerJobStats());
    }

    /**
     * Ctr
     * @param threadPool ThreadPool to use to fire the first request of a background job.
     * @param executorName Name of the executor to use to fire the first request of a background job.
     * @param job The rollup job
     * @param initialState Initial state for the indexer
     * @param initialPosition The last indexed bucket of the task
     * @param jobStats jobstats instance for collecting stats
     */
    RollupIndexer(ThreadPool threadPool, String executorName, RollupJob job, AtomicReference<IndexerState> initialState,
                  Map<String, Object> initialPosition, RollupIndexerJobStats jobStats) {
        super(threadPool, executorName, initialState, initialPosition, jobStats);
        this.job = job;
        this.compositeBuilder = createCompositeBuilder(job.getConfig());
    }

    @Override
    protected String getJobId() {
        return job.getConfig().getId();
    }

    @Override
    protected void onStart(long now, ActionListener<Boolean> listener) {
        try {
            // this is needed to exclude buckets that can still receive new documents
            DateHistogramGroupConfig dateHisto = job.getConfig().getGroupConfig().getDateHistogram();
            // if the job has a delay we filter all documents that appear before it
            long delay = dateHisto.getDelay() != null ?
                TimeValue.parseTimeValue(dateHisto.getDelay().toString(), "").millis() : 0;
            maxBoundary = dateHisto.createRounding().round(now - delay);
            listener.onResponse(true);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    protected SearchRequest buildSearchRequest(long waitTimeInNanos) {
        final Map<String, Object> position = getPosition();
        SearchSourceBuilder searchSource = new SearchSourceBuilder()
                .size(0)
                .trackTotalHits(false)
                // make sure we always compute complete buckets that appears before the configured delay
                .query(createBoundaryQuery(position))
                .aggregation(compositeBuilder.aggregateAfter(position));
        return new SearchRequest(job.getConfig().getIndexPattern()).source(searchSource);
    }

    @Override
    protected IterationResult<Map<String, Object>> doProcess(SearchResponse searchResponse) {
        final CompositeAggregation response = searchResponse.getAggregations().get(AGGREGATION_NAME);

        if (response.getBuckets().isEmpty()) {
            // do not reset the position as we want to continue from where we stopped
            return new IterationResult<>(Collections.emptyList(), getPosition(), true);
        }

        return new IterationResult<>(
                IndexerUtils.processBuckets(response, job.getConfig().getRollupIndex(), getStats(),
                        job.getConfig().getGroupConfig(), job.getConfig().getId()),
                response.afterKey(), response.getBuckets().isEmpty());
    }

    /**
     * Creates a skeleton {@link CompositeAggregationBuilder} from the provided job config.
     * @param config The config for the job.
     * @return The composite aggregation that creates the rollup buckets
     */
    private CompositeAggregationBuilder createCompositeBuilder(RollupJobConfig config) {
        final GroupConfig groupConfig = config.getGroupConfig();
        List<CompositeValuesSourceBuilder<?>> builders = createValueSourceBuilders(groupConfig);

        CompositeAggregationBuilder composite = new CompositeAggregationBuilder(AGGREGATION_NAME, builders);

        List<AggregationBuilder> aggregations = createAggregationBuilders(config.getMetricsConfig());
        aggregations.forEach(composite::subAggregation);

        final Map<String, Object> metadata = createMetadata(groupConfig);
        if (metadata.isEmpty() == false) {
            composite.setMetadata(metadata);
        }
        composite.size(config.getPageSize());

        return composite;
    }

    /**
     * Creates the range query that limits the search to documents that appear before the maximum allowed time
     * (see {@link #maxBoundary}
     * and on or after the last processed time.
     * @param position The current position of the pagination
     * @return The range query to execute
     */
    private QueryBuilder createBoundaryQuery(Map<String, Object> position) {
        assert maxBoundary < Long.MAX_VALUE;
        DateHistogramGroupConfig dateHisto = job.getConfig().getGroupConfig().getDateHistogram();
        String fieldName = dateHisto.getField();
        String rollupFieldName = fieldName + "."  + DateHistogramAggregationBuilder.NAME;
        long lowerBound = 0L;
        if (position != null) {
            Number value = (Number) position.get(rollupFieldName);
            lowerBound = value.longValue();
        }
        assert lowerBound <= maxBoundary;
        final RangeQueryBuilder query = new RangeQueryBuilder(fieldName)
                .gte(lowerBound)
                .lt(maxBoundary)
                .format("epoch_millis");
        return query;
    }

    static Map<String, Object> createMetadata(final GroupConfig groupConfig) {
        final Map<String, Object> metadata = new HashMap<>();
        if (groupConfig != null) {
            // Add all the metadata in order: date_histo -> histo
            final DateHistogramGroupConfig dateHistogram = groupConfig.getDateHistogram();
            metadata.put(RollupField.formatMetaField(RollupField.INTERVAL), dateHistogram.getInterval().toString());

            final HistogramGroupConfig histogram = groupConfig.getHistogram();
            if (histogram != null) {
                metadata.put(RollupField.formatMetaField(RollupField.INTERVAL), histogram.getInterval());
            }
        }
        return metadata;
    }

    public static List<CompositeValuesSourceBuilder<?>> createValueSourceBuilders(final GroupConfig groupConfig) {
        final List<CompositeValuesSourceBuilder<?>> builders = new ArrayList<>();
        // Add all the agg builders to our request in order: date_histo -> histo -> terms
        if (groupConfig != null) {
            final DateHistogramGroupConfig dateHistogram = groupConfig.getDateHistogram();
            builders.addAll(createValueSourceBuilders(dateHistogram));

            final HistogramGroupConfig histogram = groupConfig.getHistogram();
            builders.addAll(createValueSourceBuilders(histogram));

            final TermsGroupConfig terms = groupConfig.getTerms();
            builders.addAll(createValueSourceBuilders(terms));
        }
        return Collections.unmodifiableList(builders);
    }

    public static List<CompositeValuesSourceBuilder<?>> createValueSourceBuilders(final DateHistogramGroupConfig dateHistogram) {
        final String dateHistogramField = dateHistogram.getField();
        final String dateHistogramName = RollupField.formatIndexerAggName(dateHistogramField, DateHistogramAggregationBuilder.NAME);
        final DateHistogramValuesSourceBuilder dateHistogramBuilder = new DateHistogramValuesSourceBuilder(dateHistogramName);
        if (dateHistogram instanceof DateHistogramGroupConfig.FixedInterval) {
            dateHistogramBuilder.fixedInterval(dateHistogram.getInterval());
        } else if (dateHistogram instanceof DateHistogramGroupConfig.CalendarInterval) {
            dateHistogramBuilder.calendarInterval(dateHistogram.getInterval());
        } else {
            dateHistogramBuilder.dateHistogramInterval(dateHistogram.getInterval());
        }
        dateHistogramBuilder.field(dateHistogramField);
        dateHistogramBuilder.timeZone(ZoneId.of(dateHistogram.getTimeZone()));
        return Collections.singletonList(dateHistogramBuilder);
    }

    public static List<CompositeValuesSourceBuilder<?>> createValueSourceBuilders(final HistogramGroupConfig histogram) {
        final List<CompositeValuesSourceBuilder<?>> builders = new ArrayList<>();
        if (histogram != null) {
            for (String field : histogram.getFields()) {
                final String histogramName = RollupField.formatIndexerAggName(field, HistogramAggregationBuilder.NAME);
                final HistogramValuesSourceBuilder histogramBuilder = new HistogramValuesSourceBuilder(histogramName);
                histogramBuilder.interval(histogram.getInterval());
                histogramBuilder.field(field);
                histogramBuilder.missingBucket(true);
                builders.add(histogramBuilder);
            }
        }
        return Collections.unmodifiableList(builders);
    }

    public static List<CompositeValuesSourceBuilder<?>> createValueSourceBuilders(final TermsGroupConfig terms) {
        final List<CompositeValuesSourceBuilder<?>> builders = new ArrayList<>();
        if (terms != null) {
            for (String field : terms.getFields()) {
                final String termsName = RollupField.formatIndexerAggName(field, TermsAggregationBuilder.NAME);
                final TermsValuesSourceBuilder termsBuilder = new TermsValuesSourceBuilder(termsName);
                termsBuilder.field(field);
                termsBuilder.missingBucket(true);
                builders.add(termsBuilder);
            }
        }
        return Collections.unmodifiableList(builders);
    }

    /**
     * This returns a set of aggregation builders which represent the configured
     * set of metrics. Used to iterate over historical data.
     */
    static List<AggregationBuilder> createAggregationBuilders(final List<MetricConfig> metricsConfigs) {
        final List<AggregationBuilder> builders = new ArrayList<>();
        if (metricsConfigs != null) {
            for (MetricConfig metricConfig : metricsConfigs) {
                final List<String> metrics = metricConfig.getMetrics();
                if (metrics.isEmpty() == false) {
                    final String field = metricConfig.getField();
                    for (String metric : metrics) {
                        ValuesSourceAggregationBuilder.LeafOnly<? extends ValuesSource, ? extends AggregationBuilder> newBuilder;
                        if (metric.equals(MetricConfig.MIN.getPreferredName())) {
                            newBuilder = new MinAggregationBuilder(formatFieldName(field, MinAggregationBuilder.NAME, RollupField.VALUE));
                        } else if (metric.equals(MetricConfig.MAX.getPreferredName())) {
                            newBuilder = new MaxAggregationBuilder(formatFieldName(field, MaxAggregationBuilder.NAME, RollupField.VALUE));
                        } else if (metric.equals(MetricConfig.AVG.getPreferredName())) {
                            // Avgs are sum + count
                            newBuilder = new SumAggregationBuilder(formatFieldName(field, AvgAggregationBuilder.NAME, RollupField.VALUE));
                            ValuesSourceAggregationBuilder.LeafOnly<ValuesSource, ValueCountAggregationBuilder> countBuilder
                                = new ValueCountAggregationBuilder(
                                formatFieldName(field, AvgAggregationBuilder.NAME, RollupField.COUNT_FIELD));
                            countBuilder.field(field);
                            builders.add(countBuilder);
                        } else if (metric.equals(MetricConfig.SUM.getPreferredName())) {
                            newBuilder = new SumAggregationBuilder(formatFieldName(field, SumAggregationBuilder.NAME, RollupField.VALUE));
                        } else if (metric.equals(MetricConfig.VALUE_COUNT.getPreferredName())) {
                            // TODO allow non-numeric value_counts.
                            // I removed the hard coding of NUMERIC as part of cleaning up targetValueType, but I don't think  that resolves
                            // the above to do note -- Tozzi 2019-12-06
                            newBuilder = new ValueCountAggregationBuilder(
                                formatFieldName(field, ValueCountAggregationBuilder.NAME, RollupField.VALUE));
                        } else {
                            throw new IllegalArgumentException("Unsupported metric type [" + metric + "]");
                        }
                        newBuilder.field(field);
                        builders.add(newBuilder);
                    }
                }
            }
        }
        return Collections.unmodifiableList(builders);
    }
}

