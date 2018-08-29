/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.job;

import org.apache.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
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
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.core.rollup.job.DateHistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.GroupConfig;
import org.elasticsearch.xpack.core.rollup.job.HistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.IndexerState;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;
import org.elasticsearch.xpack.core.rollup.job.RollupJob;
import org.elasticsearch.xpack.core.rollup.job.RollupJobConfig;
import org.elasticsearch.xpack.core.rollup.job.RollupJobStats;
import org.elasticsearch.xpack.core.rollup.job.TermsGroupConfig;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.xpack.core.rollup.RollupField.formatFieldName;

/**
 * An abstract class that builds a rollup index incrementally. A background job can be launched using {@link #maybeTriggerAsyncJob(long)},
 * it will create the rollup index from the source index up to the last complete bucket that is allowed to be built (based on the current
 * time and the delay set on the rollup job). Only one background job can run simultaneously and {@link #onFinish()} is called when the job
 * finishes. {@link #onFailure(Exception)} is called if the job fails with an exception and {@link #onAbort()} is called if the indexer is
 * aborted while a job is running. The indexer must be started ({@link #start()} to allow a background job to run when
 * {@link #maybeTriggerAsyncJob(long)} is called. {@link #stop()} can be used to stop the background job without aborting the indexer.
 */
public abstract class RollupIndexer {
    private static final Logger logger = Logger.getLogger(RollupIndexer.class.getName());

    static final String AGGREGATION_NAME = RollupField.NAME;

    private final RollupJob job;
    private final RollupJobStats stats;
    private final AtomicReference<IndexerState> state;
    private final AtomicReference<Map<String, Object>> position;
    private final Executor executor;
    protected final AtomicBoolean upgradedDocumentID;

    private final CompositeAggregationBuilder compositeBuilder;
    private long maxBoundary;

    /**
     * Ctr
     * @param executor Executor to use to fire the first request of a background job.
     * @param job The rollup job
     * @param initialState Initial state for the indexer
     * @param initialPosition The last indexed bucket of the task
     */
    RollupIndexer(Executor executor, RollupJob job, AtomicReference<IndexerState> initialState,
                  Map<String, Object> initialPosition, AtomicBoolean upgradedDocumentID) {
        this.executor = executor;
        this.job = job;
        this.stats = new RollupJobStats();
        this.state = initialState;
        this.position = new AtomicReference<>(initialPosition);
        this.compositeBuilder = createCompositeBuilder(job.getConfig());
        this.upgradedDocumentID = upgradedDocumentID;
    }

    /**
     * Executes the {@link SearchRequest} and calls <code>nextPhase</code> with the response
     * or the exception if an error occurs.
     *
     * @param request The search request to execute
     * @param nextPhase Listener for the next phase
     */
    protected abstract void doNextSearch(SearchRequest request, ActionListener<SearchResponse> nextPhase);

    /**
     * Executes the {@link BulkRequest} and calls <code>nextPhase</code> with the response
     * or the exception if an error occurs.
     *
     * @param request The bulk request to execute
     * @param nextPhase Listener for the next phase
     */
    protected abstract void doNextBulk(BulkRequest request, ActionListener<BulkResponse> nextPhase);

    /**
     * Called periodically during the execution of a background job. Implementation should
     * persists the state somewhere and continue the execution asynchronously using <code>next</code>.
     *
     * @param state The current state of the indexer
     * @param position The current position of the indexer
     * @param next Runnable for the next phase
     */
    protected abstract void doSaveState(IndexerState state, Map<String, Object> position, Runnable next);

    /**
     * Called when a failure occurs in an async job causing the execution to stop.
     * @param exc The exception
     */
    protected abstract void onFailure(Exception exc);

    /**
     * Called when a background job finishes.
     */
    protected abstract void onFinish();

    /**
     * Called when a background job detects that the indexer is aborted causing the async execution
     * to stop.
     */
    protected abstract void onAbort();

    /**
     * Get the current state of the indexer.
     */
    public IndexerState getState() {
        return state.get();
    }

    /**
     * Get the current position of the indexer.
     */
    public Map<String, Object> getPosition() {
        return position.get();
    }

    /**
     * Get the stats of this indexer.
     */
    public RollupJobStats getStats() {
        return stats;
    }

    /**
     * Returns if this job has upgraded it's ID scheme yet or not
     */
    public boolean isUpgradedDocumentID() {
        return upgradedDocumentID.get();
    }

    /**
     * Sets the internal state to {@link IndexerState#STARTED} if the previous state was {@link IndexerState#STOPPED}. Setting the state to
     * STARTED allows a job to run in the background when {@link #maybeTriggerAsyncJob(long)} is called.
     * @return The new state for the indexer (STARTED, INDEXING or ABORTING if the job was already aborted).
     */
    public synchronized IndexerState start() {
        state.compareAndSet(IndexerState.STOPPED, IndexerState.STARTED);
        return state.get();
    }

    /**
     * Sets the internal state to {@link IndexerState#STOPPING} if an async job is running in the background and in such case
     * {@link #onFinish()} will be called as soon as the background job detects that the indexer is stopped. If there is no job running when
     * this function is called, the state is directly set to {@link IndexerState#STOPPED} and {@link #onFinish()} will never be called.
     * @return The new state for the indexer (STOPPED, STOPPING or ABORTING if the job was already aborted).
     */
    public synchronized IndexerState stop() {
        IndexerState currentState = state.updateAndGet(previousState -> {
            if (previousState == IndexerState.INDEXING) {
                return IndexerState.STOPPING;
            } else if (previousState == IndexerState.STARTED) {
                return IndexerState.STOPPED;
            } else {
                return previousState;
            }
        });
        return currentState;
    }

    /**
     * Sets the internal state to {@link IndexerState#ABORTING}. It returns false if an async job is running in the background and in such
     * case {@link #onAbort} will be called as soon as the background job detects that the indexer is aborted. If there is no job running
     * when this function is called, it returns true and {@link #onAbort()} will never be called.
     * @return true if the indexer is aborted, false if a background job is running and abort is delayed.
     */
    public synchronized boolean abort() {
        IndexerState prevState = state.getAndUpdate((prev) -> IndexerState.ABORTING);
        return prevState == IndexerState.STOPPED || prevState == IndexerState.STARTED;
    }

    /**
     * Triggers a background job that builds the rollup index asynchronously iff there is no other job that runs
     * and the indexer is started ({@link IndexerState#STARTED}.
     *
     * @param now The current time in milliseconds (used to limit the job to complete buckets)
     * @return true if a job has been triggered, false otherwise
     */
    public synchronized boolean maybeTriggerAsyncJob(long now) {
        final IndexerState currentState = state.get();
        switch (currentState) {
            case INDEXING:
            case STOPPING:
            case ABORTING:
                logger.warn("Schedule was triggered for rollup job [" + job.getConfig().getId() + "], but prior indexer is still running.");
                return false;

            case STOPPED:
                logger.debug("Schedule was triggered for rollup job [" + job.getConfig().getId()
                        + "] but job is stopped.  Ignoring trigger.");
                return false;

            case STARTED:
                logger.debug("Schedule was triggered for rollup job [" + job.getConfig().getId() + "], state: [" + currentState + "]");
                // Only valid time to start indexing is when we are STARTED but not currently INDEXING.
                stats.incrementNumInvocations(1);

                // rounds the current time to its current bucket based on the date histogram interval.
                // this is needed to exclude buckets that can still receive new documents.
                DateHistogramGroupConfig dateHisto = job.getConfig().getGroupConfig().getDateHistogram();
                long rounded = dateHisto.createRounding().round(now);
                if (dateHisto.getDelay() != null) {
                    // if the job has a delay we filter all documents that appear before it.
                    maxBoundary = rounded - TimeValue.parseTimeValue(dateHisto.getDelay().toString(), "").millis();
                } else {
                    maxBoundary = rounded;
                }

                if (state.compareAndSet(IndexerState.STARTED, IndexerState.INDEXING)) {
                    // fire off the search.  Note this is async, the method will return from here
                    executor.execute(() -> doNextSearch(buildSearchRequest(),
                            ActionListener.wrap(this::onSearchResponse, exc -> finishWithFailure(exc))));
                    logger.debug("Beginning to rollup [" + job.getConfig().getId() + "], state: [" + currentState + "]");
                    return true;
                } else {
                    logger.debug("Could not move from STARTED to INDEXING state because current state is [" + state.get() + "]");
                    return false;
                }

            default:
                logger.warn("Encountered unexpected state [" + currentState + "] while indexing");
                throw new IllegalStateException("Rollup job encountered an illegal state [" + currentState + "]");
        }
    }

    /**
     * Checks the {@link IndexerState} and returns false if the execution
     * should be stopped.
     */
    private boolean checkState(IndexerState currentState) {
        switch (currentState) {
            case INDEXING:
                // normal state;
                return true;

            case STOPPING:
                logger.info("Rollup job encountered [" + IndexerState.STOPPING + "] state, halting indexer.");
                doSaveState(finishAndSetState(), getPosition(), () -> {});
                return false;

            case STOPPED:
                return false;

            case ABORTING:
                logger.info("Requested shutdown of indexer for job [" + job.getConfig().getId() + "]");
                onAbort();
                return false;

            default:
                // Anything other than indexing, aborting or stopping is unanticipated
                logger.warn("Encountered unexpected state [" + currentState + "] while indexing");
                throw new IllegalStateException("Rollup job encountered an illegal state [" + currentState + "]");
        }
    }

    private void onBulkResponse(BulkResponse response, Map<String, Object> after) {
        // TODO we should check items in the response and move after accordingly to resume the failing buckets ?
        stats.incrementNumRollups(response.getItems().length);
        if (response.hasFailures()) {
            logger.warn("Error while attempting to bulk index rollup documents: " + response.buildFailureMessage());
        }
        try {
            if (checkState(getState()) == false) {
                return ;
            }
            position.set(after);
            ActionListener<SearchResponse> listener = ActionListener.wrap(this::onSearchResponse, this::finishWithFailure);
            // TODO probably something more intelligent than every-50 is needed
            if (stats.getNumPages() > 0 && stats.getNumPages() % 50 == 0) {
                doSaveState(IndexerState.INDEXING, after, () -> doNextSearch(buildSearchRequest(), listener));
            } else {
                doNextSearch(buildSearchRequest(), listener);
            }
        } catch (Exception e) {
            finishWithFailure(e);
        }
    }

    private void onSearchResponse(SearchResponse searchResponse) {
        try {
            if (checkState(getState()) == false) {
                return ;
            }
            if (searchResponse.getShardFailures().length != 0) {
                throw new RuntimeException("Shard failures encountered while running indexer for rollup job ["
                        + job.getConfig().getId() + "]: " + Arrays.toString(searchResponse.getShardFailures()));
            }
            final CompositeAggregation response = searchResponse.getAggregations().get(AGGREGATION_NAME);
            if (response == null) {
                throw new IllegalStateException("Missing composite response for query: " + compositeBuilder.toString());
            }
            stats.incrementNumPages(1);
            if (response.getBuckets().isEmpty()) {
                // this is the end...
                logger.debug("Finished indexing for job [" + job.getConfig().getId() + "], saving state and shutting down.");

                // Change state first, then try to persist.  This prevents in-progress STOPPING/ABORTING from
                // being persisted as STARTED but then stop the job
                doSaveState(finishAndSetState(), position.get(), this::onFinish);
                return;
            }

            final BulkRequest bulkRequest = new BulkRequest();
            // Indexer is single-threaded, and only place that the ID scheme can get upgraded is doSaveState(), so
            // we can pass down the boolean value rather than the atomic here
            final List<IndexRequest> docs = IndexerUtils.processBuckets(response, job.getConfig().getRollupIndex(),
                    stats, job.getConfig().getGroupConfig(), job.getConfig().getId(), upgradedDocumentID.get());
            docs.forEach(bulkRequest::add);
            assert bulkRequest.requests().size() > 0;
            doNextBulk(bulkRequest,
                    ActionListener.wrap(
                            bulkResponse -> onBulkResponse(bulkResponse, response.afterKey()),
                            exc -> finishWithFailure(exc)
                    )
            );
        } catch(Exception e) {
            finishWithFailure(e);
        }
    }

    private void finishWithFailure(Exception exc) {
        doSaveState(finishAndSetState(), position.get(), () -> onFailure(exc));
    }

    private IndexerState finishAndSetState() {
        return state.updateAndGet(
                prev -> {
                    switch (prev) {
                        case INDEXING:
                            // ready for another job
                            return IndexerState.STARTED;

                        case STOPPING:
                            // must be started again
                            return IndexerState.STOPPED;

                        case ABORTING:
                            // abort and exit
                            onAbort();
                            return IndexerState.ABORTING;  // This shouldn't matter, since onAbort() will kill the task first

                        case STOPPED:
                            // No-op.  Shouldn't really be possible to get here (should have to go through STOPPING
                            // first which will be handled) but is harmless to no-op and we don't want to throw exception here
                            return IndexerState.STOPPED;

                        default:
                            // any other state is unanticipated at this point
                            throw new IllegalStateException("Rollup job encountered an illegal state [" + prev + "]");
                    }
                });
    }

    private SearchRequest buildSearchRequest() {
        final Map<String, Object> position = getPosition();
        SearchSourceBuilder searchSource = new SearchSourceBuilder()
                .size(0)
                .trackTotalHits(false)
                // make sure we always compute complete buckets that appears before the configured delay
                .query(createBoundaryQuery(position))
                .aggregation(compositeBuilder.aggregateAfter(position));
        return new SearchRequest(job.getConfig().getIndexPattern()).source(searchSource);
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
            composite.setMetaData(metadata);
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
        return unmodifiableList(builders);
    }

    public static List<CompositeValuesSourceBuilder<?>> createValueSourceBuilders(final DateHistogramGroupConfig dateHistogram) {
        final String dateHistogramField = dateHistogram.getField();
        final String dateHistogramName = RollupField.formatIndexerAggName(dateHistogramField, DateHistogramAggregationBuilder.NAME);
        final DateHistogramValuesSourceBuilder dateHistogramBuilder = new DateHistogramValuesSourceBuilder(dateHistogramName);
        dateHistogramBuilder.dateHistogramInterval(dateHistogram.getInterval());
        dateHistogramBuilder.field(dateHistogramField);
        dateHistogramBuilder.timeZone(toDateTimeZone(dateHistogram.getTimeZone()));
        return singletonList(dateHistogramBuilder);
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
        return unmodifiableList(builders);
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
        return unmodifiableList(builders);
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
                        ValuesSourceAggregationBuilder.LeafOnly newBuilder;
                        if (metric.equals(MetricConfig.MIN.getPreferredName())) {
                            newBuilder = new MinAggregationBuilder(formatFieldName(field, MinAggregationBuilder.NAME, RollupField.VALUE));
                        } else if (metric.equals(MetricConfig.MAX.getPreferredName())) {
                            newBuilder = new MaxAggregationBuilder(formatFieldName(field, MaxAggregationBuilder.NAME, RollupField.VALUE));
                        } else if (metric.equals(MetricConfig.AVG.getPreferredName())) {
                            // Avgs are sum + count
                            newBuilder = new SumAggregationBuilder(formatFieldName(field, AvgAggregationBuilder.NAME, RollupField.VALUE));
                            ValuesSourceAggregationBuilder.LeafOnly countBuilder
                                = new ValueCountAggregationBuilder(
                                formatFieldName(field, AvgAggregationBuilder.NAME, RollupField.COUNT_FIELD), ValueType.NUMERIC);
                            countBuilder.field(field);
                            builders.add(countBuilder);
                        } else if (metric.equals(MetricConfig.SUM.getPreferredName())) {
                            newBuilder = new SumAggregationBuilder(formatFieldName(field, SumAggregationBuilder.NAME, RollupField.VALUE));
                        } else if (metric.equals(MetricConfig.VALUE_COUNT.getPreferredName())) {
                            // TODO allow non-numeric value_counts.
                            // Hardcoding this is fine for now since the job validation guarantees that all metric fields are numerics
                            newBuilder = new ValueCountAggregationBuilder(
                                formatFieldName(field, ValueCountAggregationBuilder.NAME, RollupField.VALUE), ValueType.NUMERIC);
                        } else {
                            throw new IllegalArgumentException("Unsupported metric type [" + metric + "]");
                        }
                        newBuilder.field(field);
                        builders.add(newBuilder);
                    }
                }
            }
        }
        return unmodifiableList(builders);
    }

    private static DateTimeZone toDateTimeZone(final String timezone) {
        try {
            return DateTimeZone.forOffsetHours(Integer.parseInt(timezone));
        } catch (NumberFormatException e) {
            return DateTimeZone.forID(timezone);
        }
    }
}

