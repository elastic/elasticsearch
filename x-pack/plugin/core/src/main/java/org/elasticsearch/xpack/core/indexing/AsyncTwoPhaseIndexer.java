/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An abstract class that builds an index incrementally. A background job can be launched using {@link #maybeTriggerAsyncJob(long)},
 * it will create the index from the source index up to the last complete bucket that is allowed to be built (based on job position).
 * Only one background job can run simultaneously and {@link #onFinish} is called when the job
 * finishes. {@link #onStop()} is called after the current search returns when the job is stopped early via a call
 * to {@link #stop()}. {@link #onFailure(Exception)} is called if the job fails with an exception and {@link #onAbort()}
 * is called if the indexer is aborted while a job is running. The indexer must be started ({@link #start()}
 * to allow a background job to run when {@link #maybeTriggerAsyncJob(long)} is called.
 * {@link #stop()} can be used to stop the background job without aborting the indexer.
 *
 * In a nutshell this is a 2 cycle engine: 1st it sends a query, 2nd it indexes documents based on the response, sends the next query,
 * indexes, queries, indexes, ... until a condition lets the engine pause until the source provides new input.
 *
 * @param <JobPosition> Type that defines a job position to be defined by the implementation.
 */
public abstract class AsyncTwoPhaseIndexer<JobPosition, JobStats extends IndexerJobStats> {
    private static final Logger logger = LogManager.getLogger(AsyncTwoPhaseIndexer.class.getName());

    private final JobStats stats;

    private final AtomicReference<IndexerState> state;
    private final AtomicReference<JobPosition> position;
    private final Executor executor;

    protected AsyncTwoPhaseIndexer(Executor executor, AtomicReference<IndexerState> initialState,
                                   JobPosition initialPosition, JobStats jobStats) {
        this.executor = executor;
        this.state = initialState;
        this.position = new AtomicReference<>(initialPosition);
        this.stats = jobStats;
    }

    /**
     * Get the current state of the indexer.
     */
    public IndexerState getState() {
        return state.get();
    }

    /**
     * Get the current position of the indexer.
     */
    public JobPosition getPosition() {
        return position.get();
    }

    /**
     * Get the stats of this indexer.
     */
    public JobStats getStats() {
        return stats;
    }

    /**
     * Sets the internal state to {@link IndexerState#STARTED} if the previous state
     * was {@link IndexerState#STOPPED}. Setting the state to STARTED allows a job
     * to run in the background when {@link #maybeTriggerAsyncJob(long)} is called.
     *
     * @return The new state for the indexer (STARTED, INDEXING or ABORTING if the
     *         job was already aborted).
     */
    public synchronized IndexerState start() {
        state.compareAndSet(IndexerState.STOPPED, IndexerState.STARTED);
        return state.get();
    }

    /**
     * Sets the internal state to {@link IndexerState#STOPPING} if an async job is
     * running in the background, {@link #onStop()} will be called when the background job
     * detects that the indexer is stopped.
     * If there is no job running when this function is called the returned
     * state is {@link IndexerState#STOPPED} and {@link #onStop()} will not be called.
     *
     * @return The new state for the indexer (STOPPED, STOPPING or ABORTING if the job was already aborted).
     */
    public synchronized IndexerState stop() {
        return state.updateAndGet(previousState -> {
            if (previousState == IndexerState.INDEXING) {
                return IndexerState.STOPPING;
            } else if (previousState == IndexerState.STARTED) {
                return IndexerState.STOPPED;
            } else {
                return previousState;
            }
        });
    }

    /**
     * Sets the internal state to {@link IndexerState#ABORTING}. It returns false if
     * an async job is running in the background and in such case {@link #onAbort}
     * will be called as soon as the background job detects that the indexer is
     * aborted. If there is no job running when this function is called, it returns
     * true and {@link #onAbort()} will never be called.
     *
     * @return true if the indexer is aborted, false if a background job is running
     *         and abort is delayed.
     */
    public synchronized boolean abort() {
        IndexerState prevState = state.getAndUpdate((prev) -> IndexerState.ABORTING);
        return prevState == IndexerState.STOPPED || prevState == IndexerState.STARTED;
    }

    /**
     * Triggers a background job that builds the index asynchronously iff
     * there is no other job that runs and the indexer is started
     * ({@link IndexerState#STARTED}.
     *
     * @param now
     *            The current time in milliseconds (used to limit the job to
     *            complete buckets)
     * @return true if a job has been triggered, false otherwise
     */
    public synchronized boolean maybeTriggerAsyncJob(long now) {
        final IndexerState currentState = state.get();
        switch (currentState) {
        case INDEXING:
        case STOPPING:
        case ABORTING:
            logger.warn("Schedule was triggered for job [" + getJobId() + "], but prior indexer is still running " +
                "(with state [" + currentState + "]");
            return false;

        case STOPPED:
            logger.debug("Schedule was triggered for job [" + getJobId() + "] but job is stopped.  Ignoring trigger.");
            return false;

        case STARTED:
            logger.debug("Schedule was triggered for job [" + getJobId() + "], state: [" + currentState + "]");
            stats.incrementNumInvocations(1);

            if (state.compareAndSet(IndexerState.STARTED, IndexerState.INDEXING)) {
                // fire off the search. Note this is async, the method will return from here
                executor.execute(() -> {
                    onStart(now, ActionListener.wrap(r -> {
                        assert r != null;
                        if (r) {
                            nextSearch(ActionListener.wrap(this::onSearchResponse, this::finishWithSearchFailure));
                        } else {
                            onFinish(ActionListener.wrap(
                                onFinishResponse -> doSaveState(finishAndSetState(), position.get(), () -> {}),
                                onFinishFailure -> doSaveState(finishAndSetState(), position.get(), () -> {})));
                        }
                    },
                    this::finishWithFailure));
                });
                logger.debug("Beginning to index [" + getJobId() + "], state: [" + currentState + "]");
                return true;
            } else {
                logger.debug("Could not move from STARTED to INDEXING state because current state is [" + state.get() + "]");
                return false;
            }

        default:
            logger.warn("Encountered unexpected state [" + currentState + "] while indexing");
            throw new IllegalStateException("Job encountered an illegal state [" + currentState + "]");
        }
    }

    /**
     * Called to get the Id of the job, used for logging.
     *
     * @return a string with the id of the job
     */
    protected abstract String getJobId();

    /**
     * Called to process a response from the 1 search request in order to turn it into a {@link IterationResult}.
     *
     * @param searchResponse response from the search phase.
     * @return Iteration object to be passed to indexing phase.
     */
    protected abstract IterationResult<JobPosition> doProcess(SearchResponse searchResponse);

    /**
     * Called to build the next search request.
     *
     * @return SearchRequest to be passed to the search phase.
     */
    protected abstract SearchRequest buildSearchRequest();

    /**
     * Called at startup after job has been triggered using {@link #maybeTriggerAsyncJob(long)} and the
     * internal state is {@link IndexerState#STARTED}.
     *
     * @param now The current time in milliseconds passed through from {@link #maybeTriggerAsyncJob(long)}
     * @param listener listener to call after done. The argument passed to the listener indicates if the indexer should continue or not
     *                 true: continue execution as normal
     *                 false: cease execution. This does NOT call onFinish
     */
    protected abstract void onStart(long now, ActionListener<Boolean> listener);

    /**
     * Executes the {@link SearchRequest} and calls <code>nextPhase</code> with the
     * response or the exception if an error occurs.
     *
     * @param request
     *            The search request to execute
     * @param nextPhase
     *            Listener for the next phase
     */
    protected abstract void doNextSearch(SearchRequest request, ActionListener<SearchResponse> nextPhase);

    /**
     * Executes the {@link BulkRequest} and calls <code>nextPhase</code> with the
     * response or the exception if an error occurs.
     *
     * @param request
     *            The bulk request to execute
     * @param nextPhase
     *            Listener for the next phase
     */
    protected abstract void doNextBulk(BulkRequest request, ActionListener<BulkResponse> nextPhase);

    /**
     * Called periodically during the execution of a background job. Implementation
     * should persists the state somewhere and continue the execution asynchronously
     * using <code>next</code>.
     *
     * @param state
     *            The current state of the indexer
     * @param position
     *            The current position of the indexer
     * @param next
     *            Runnable for the next phase
     */
    protected abstract void doSaveState(IndexerState state, JobPosition position, Runnable next);

    /**
     * Called when a failure occurs in an async job causing the execution to stop.
     *
     * This is called before the internal state changes from the state in which the failure occurred.
     *
     * @param exc The exception
     */
    protected abstract void onFailure(Exception exc);

    /**
     * Called when a background job finishes before the internal state changes from {@link IndexerState#INDEXING} back to
     * {@link IndexerState#STARTED}.
     *
     * @param listener listener to call after done
     */
    protected abstract void onFinish(ActionListener<Void> listener);

    /**
     * Called when the indexer is stopped. This is only called when the indexer is stopped
     * via {@link #stop()} as opposed to {@link #onFinish(ActionListener)} which is called
     * when the indexer's work is done.
     */
    protected void onStop() {
    }

    /**
     * Called when a background job detects that the indexer is aborted causing the
     * async execution to stop.
     */
    protected abstract void onAbort();

    private void finishWithSearchFailure(Exception exc) {
        stats.incrementSearchFailures();
        onFailure(exc);
        doSaveState(finishAndSetState(), position.get(), () -> {});
    }

    private void finishWithIndexingFailure(Exception exc) {
        stats.incrementIndexingFailures();
        onFailure(exc);
        doSaveState(finishAndSetState(), position.get(), () -> {});
    }

    private void finishWithFailure(Exception exc) {
        onFailure(exc);
        finishAndSetState();
    }

    private IndexerState finishAndSetState() {
        AtomicBoolean callOnStop = new AtomicBoolean(false);
        AtomicBoolean callOnAbort = new AtomicBoolean(false);
        IndexerState updatedState = state.updateAndGet(prev -> {
            callOnAbort.set(false);
            callOnStop.set(false);
            switch (prev) {
            case INDEXING:
                // ready for another job
                return IndexerState.STARTED;

            case STOPPING:
                callOnStop.set(true);
                // must be started again
                return IndexerState.STOPPED;

            case ABORTING:
                callOnAbort.set(true);
                // abort and exit
                return IndexerState.ABORTING; // This shouldn't matter, since onAbort() will kill the task first

            case STOPPED:
                // No-op. Shouldn't really be possible to get here (should have to go through
                // STOPPING
                // first which will be handled) but is harmless to no-op and we don't want to
                // throw exception here
                return IndexerState.STOPPED;

            default:
                // any other state is unanticipated at this point
                throw new IllegalStateException("Indexer job encountered an illegal state [" + prev + "]");
            }
        });

        if (callOnStop.get()) {
            onStop();
        } else if (callOnAbort.get()) {
            onAbort();
        }

        return updatedState;
    }

    private void onSearchResponse(SearchResponse searchResponse) {
        stats.markEndSearch();
        try {
            if (checkState(getState()) == false) {
                return;
            }

            // allowPartialSearchResults is set to false, so we should never see shard failures here
            assert (searchResponse.getShardFailures().length == 0);

            stats.incrementNumPages(1);
            IterationResult<JobPosition> iterationResult = doProcess(searchResponse);

            if (iterationResult.isDone()) {
                logger.debug("Finished indexing for job [" + getJobId() + "], saving state and shutting down.");

                position.set(iterationResult.getPosition());
                // execute finishing tasks
                onFinish(ActionListener.wrap(
                        r -> doSaveState(finishAndSetState(), position.get(), () -> {}),
                        e -> doSaveState(finishAndSetState(), position.get(), () -> {})));

                return;
            }

            final List<IndexRequest> docs = iterationResult.getToIndex();

            // an iteration result might return an empty set of documents to be indexed
            if (docs.isEmpty() == false) {
                final BulkRequest bulkRequest = new BulkRequest();
                docs.forEach(bulkRequest::add);

                stats.markStartIndexing();
                doNextBulk(bulkRequest, ActionListener.wrap(bulkResponse -> {
                    // TODO we should check items in the response and move after accordingly to
                    // resume the failing buckets ?
                    if (bulkResponse.hasFailures()) {
                        logger.warn("Error while attempting to bulk index documents: " + bulkResponse.buildFailureMessage());
                    }
                    stats.incrementNumOutputDocuments(bulkResponse.getItems().length);

                    // check if indexer has been asked to stop, state {@link IndexerState#STOPPING}
                    if (checkState(getState()) == false) {
                        return;
                    }

                    JobPosition newPosition = iterationResult.getPosition();
                    position.set(newPosition);

                    onBulkResponse(bulkResponse, newPosition);
                }, this::finishWithIndexingFailure));
            } else {
                // no documents need to be indexed, continue with search
                try {
                    JobPosition newPosition = iterationResult.getPosition();
                    position.set(newPosition);

                    ActionListener<SearchResponse> listener = ActionListener.wrap(this::onSearchResponse, this::finishWithSearchFailure);
                    nextSearch(listener);
                } catch (Exception e) {
                    finishWithFailure(e);
                }
            }
        } catch (Exception e) {
            finishWithSearchFailure(e);
        }
    }

    private void onBulkResponse(BulkResponse response, JobPosition position) {
        stats.markEndIndexing();
        try {
            ActionListener<SearchResponse> listener = ActionListener.wrap(this::onSearchResponse, this::finishWithSearchFailure);
            // TODO probably something more intelligent than every-50 is needed
            if (stats.getNumPages() > 0 && stats.getNumPages() % 50 == 0) {
                doSaveState(IndexerState.INDEXING, position, () -> {
                    nextSearch(listener);
                });
            } else {
                nextSearch(listener);
            }
        } catch (Exception e) {
            finishWithIndexingFailure(e);
        }
    }

    private void nextSearch(ActionListener<SearchResponse> listener) {
        stats.markStartSearch();
        // ensure that partial results are not accepted and cause a search failure
        SearchRequest searchRequest = buildSearchRequest().allowPartialSearchResults(false);
        doNextSearch(searchRequest, listener);
    }

    /**
     * Checks the {@link IndexerState} and returns false if the execution should be
     * stopped.
     */
    private boolean checkState(IndexerState currentState) {
        switch (currentState) {
        case INDEXING:
            // normal state;
            return true;

        case STOPPING:
            logger.info("Indexer job encountered [" + IndexerState.STOPPING + "] state, halting indexer.");
            doSaveState(finishAndSetState(), getPosition(), () -> {});
            return false;

        case STOPPED:
            return false;

        case ABORTING:
            logger.info("Requested shutdown of indexer for job [" + getJobId() + "]");
            onAbort();
            return false;

        default:
            // Anything other than indexing, aborting or stopping is unanticipated
            logger.warn("Encountered unexpected state [" + currentState + "] while indexing");
            throw new IllegalStateException("Indexer job encountered an illegal state [" + currentState + "]");
        }
    }

}
