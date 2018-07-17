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
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.core.rollup.job.DateHistoGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.GroupConfig;
import org.elasticsearch.xpack.core.rollup.job.IndexerState;
import org.elasticsearch.xpack.core.rollup.job.RollupJob;
import org.elasticsearch.xpack.core.rollup.job.RollupJobConfig;
import org.elasticsearch.xpack.core.rollup.job.RollupJobStats;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

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

    private final CompositeAggregationBuilder compositeBuilder;
    private long maxBoundary;

    /**
     * Ctr
     * @param executor Executor to use to fire the first request of a background job.
     * @param job The rollup job
     * @param initialState Initial state for the indexer
     * @param initialPosition The last indexed bucket of the task
     */
    RollupIndexer(Executor executor, RollupJob job, AtomicReference<IndexerState> initialState, Map<String, Object> initialPosition) {
        this.executor = executor;
        this.job = job;
        this.stats = new RollupJobStats();
        this.state = initialState;
        this.position = new AtomicReference<>(initialPosition);
        this.compositeBuilder = createCompositeBuilder(job.getConfig());
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
                DateHistoGroupConfig dateHisto = job.getConfig().getGroupConfig().getDateHisto();
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
            final List<IndexRequest> docs = IndexerUtils.processBuckets(response, job.getConfig().getRollupIndex(),
                    stats, job.getConfig().getGroupConfig(), job.getConfig().getId());
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
        List<CompositeValuesSourceBuilder<?>> builders = new ArrayList<>();
        Map<String, Object> metadata = new HashMap<>();

        // Add all the agg builders to our request in order: date_histo -> histo -> terms
        if (groupConfig != null) {
            builders.addAll(groupConfig.getDateHisto().toBuilders());
            metadata.putAll(groupConfig.getDateHisto().getMetadata());
            if (groupConfig.getHisto() != null) {
                builders.addAll(groupConfig.getHisto().toBuilders());
                metadata.putAll(groupConfig.getHisto().getMetadata());
            }
            if (groupConfig.getTerms() != null) {
                builders.addAll(groupConfig.getTerms().toBuilders());
                metadata.putAll(groupConfig.getTerms().getMetadata());
            }
        }

        CompositeAggregationBuilder composite = new CompositeAggregationBuilder(AGGREGATION_NAME, builders);
        config.getMetricsConfig().forEach(m -> m.toBuilders().forEach(composite::subAggregation));
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
        DateHistoGroupConfig dateHisto = job.getConfig().getGroupConfig().getDateHisto();
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
                .lt(maxBoundary);
        return query;
    }
}

