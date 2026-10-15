/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.chunked;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.SearchInterval;
import org.elasticsearch.xpack.ml.datafeed.LinkedClusterState;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorUtils;
import org.elasticsearch.xpack.ml.datafeed.extractor.esql.EsqlDataExtractor;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

/**
 * A wrapper {@link DataExtractor} that can be used with other extractors in order to perform
 * searches in smaller chunks of the time range.
 *
 * <p> The chunk span can be either specified or not. When not specified,
 * a heuristic is employed (see {@link #setUpChunkedSearch()}) to automatically determine the chunk span.
 * The search is set up by querying a data summary for the given time range
 * that includes the number of total hits and the earliest/latest times. Those are then used to determine the chunk span,
 * when necessary, and to jump the search forward to the time where the earliest data can be found.
 * If a search for a chunk returns empty, the set up is performed again for the remaining time.
 *
 * <p> Cancellation's behaviour depends on the delegate extractor.
 *
 * <p> Note that this class is NOT thread-safe.
 */
public class ChunkedDataExtractor implements DataExtractor {

    private static final Logger LOGGER = LogManager.getLogger(ChunkedDataExtractor.class);

    /** Let us set a minimum chunk span of 1 minute */
    private static final long MIN_CHUNK_SPAN = 60000L;

    /**
     * Target row count per chunk for ESQL datafeeds, used both to size the initial chunk span (when it is not specified)
     * and as the target when a truncated chunk is shrunk. It is deliberately far below
     * {@link EsqlDataExtractor#INJECTED_ROW_LIMIT}, so that non-uniform (bursty) data distributions still leave headroom
     * before a result is truncated.
     */
    private static final long DEFAULT_ESQL_CHUNK_DOCS = 500L;

    /**
     * Maximum number of times one ES|QL chunk is re-queried with a smaller interval after its result was truncated at
     * {@link EsqlDataExtractor#INJECTED_ROW_LIMIT}. The counter restarts with every new chunk. Each attempt cuts the
     * interval roughly by the ratio of {@link #DEFAULT_ESQL_CHUNK_DOCS} to the row limit, and shrinking never goes below
     * one grouping interval.
     */
    static final int MAX_ESQL_SHRINK_ATTEMPTS = 5;

    private final DataExtractorFactory dataExtractorFactory;
    private final ChunkedDataExtractorContext context;
    private long currentStart;
    private long currentEnd;
    private long chunkSpan;
    private boolean isCancelled;
    private DataExtractor currentExtractor;
    private List<LinkedClusterState> lastLinkedClusterStates = List.of();
    private SearchInterval incompleteSearchInterval;
    private int esqlShrinkAttempts;

    ChunkedDataExtractor(DataExtractorFactory dataExtractorFactory, ChunkedDataExtractorContext context) {
        this.dataExtractorFactory = Objects.requireNonNull(dataExtractorFactory);
        this.context = Objects.requireNonNull(context);
        this.currentStart = context.start();
        this.currentEnd = context.start();
        this.isCancelled = false;
    }

    @Override
    public DataSummary getSummary() {
        return null;
    }

    @Override
    public boolean hasNext() {
        boolean currentHasNext = currentExtractor != null && currentExtractor.hasNext();
        if (isCancelled()) {
            return currentHasNext;
        }
        return currentHasNext || currentEnd < context.end();
    }

    @Override
    public Result next() throws IOException {
        if (hasNext() == false) {
            throw new NoSuchElementException();
        }

        if (currentExtractor == null) {
            // This is the first time next is called
            setUpChunkedSearch();
        }

        return getNextStream();
    }

    private void setUpChunkedSearch() {
        // Keep a reference so that if getSummary() throws (e.g. because a remote cluster was skipped)
        // we can still recover the cluster states it observed and expose them via getLinkedClusterStates().
        DataExtractor summaryExtractor = dataExtractorFactory.newExtractor(currentStart, context.end());
        DataSummary dataSummary;
        try {
            dataSummary = summaryExtractor.getSummary();
        } catch (ResourceNotFoundException e) {
            List<LinkedClusterState> failedStates = summaryExtractor.getLinkedClusterStates();
            if (failedStates.isEmpty() == false) {
                lastLinkedClusterStates = DataExtractorUtils.preferRicherLinkedClusterStates(lastLinkedClusterStates, failedStates);
            }
            throw e;
        }
        if (dataSummary.hasData()) {
            long earliestTime = context.timeAligner().alignToFloor(dataSummary.earliestTime());
            // For ESQL datafeeds the query may transform the time field (e.g. DATE_TRUNC), so the
            // summary's MIN(??timeField) can be earlier than the window we just queried -- clamp so we
            // never rewind currentStart. Unlike DSL, this must still jump currentStart *forward* to
            // earliestTime: without it, an unbounded preview/first real _start walks the chunked extractor
            // one chunk at a time across the *entire* configured range (e.g. epoch to "now") instead of
            // straight to where the data actually starts, which is what made ES|QL datafeed unbounded
            // preview ~750x slower than the DSL equivalent (elastic-workspace-g2sz.1) -- verified by
            // DEBUG-logging ChunkedDataExtractor during the repro: ~5000 resync round trips instead of ~3.
            currentStart = context.hasEsqlQuery() ? Math.max(currentStart, earliestTime) : earliestTime;
            currentEnd = currentStart;

            if (context.chunkSpan() != null) {
                chunkSpan = context.chunkSpan().getMillis();
            } else if (context.hasAggregations()) {
                // This heuristic is a direct copy of the manual chunking config auto-creation done in {@link DatafeedConfig}
                chunkSpan = DatafeedConfig.DEFAULT_AGGREGATION_CHUNKING_BUCKETS * context.histogramInterval();
            } else if (context.hasEsqlQuery()) {
                long timeSpread = dataSummary.latestTime() - dataSummary.earliestTime();
                if (timeSpread <= 0) {
                    chunkSpan = context.end() - currentEnd;
                } else {
                    // Target roughly DEFAULT_ESQL_CHUNK_DOCS rows per chunk assuming data is evenly distributed over time.
                    chunkSpan = Math.max(MIN_CHUNK_SPAN, DEFAULT_ESQL_CHUNK_DOCS * timeSpread / dataSummary.totalHits());
                }
            } else {
                long timeSpread = dataSummary.latestTime() - dataSummary.earliestTime();
                if (timeSpread <= 0) {
                    chunkSpan = context.end() - currentEnd;
                } else {
                    // The heuristic here is that we want a time interval where we expect roughly scrollSize documents
                    // (assuming data are uniformly spread over time).
                    // We have totalHits documents over dataTimeSpread (latestTime - earliestTime), we want scrollSize documents over chunk.
                    // Thus, the interval would be (scrollSize * dataTimeSpread) / totalHits.
                    // However, assuming this as the chunk span may often lead to half-filled pages or empty searches.
                    // It is beneficial to take a multiple of that. Based on benchmarking, we set this to 10x.
                    chunkSpan = Math.max(MIN_CHUNK_SPAN, 10 * (context.scrollSize() * timeSpread) / dataSummary.totalHits());
                }
            }

            chunkSpan = context.timeAligner().alignToCeil(chunkSpan);
            LOGGER.debug("[{}] Chunked search configured: chunk span = {} ms", context.jobId(), chunkSpan);
        } else {
            // search is over
            currentEnd = context.end();
            LOGGER.debug("[{}] Chunked search configured: no data found", context.jobId());
        }
    }

    private Result getNextStream() throws IOException {
        SearchInterval lastSearchInterval = new SearchInterval(context.start(), context.end());
        while (hasNext()) {
            boolean isNewSearch = false;

            if (currentExtractor == null || currentExtractor.hasNext() == false) {
                // First search or the current search finished; we can advance to the next search
                advanceTime();
                isNewSearch = true;
            }

            Result result;
            try {
                result = currentExtractor.next();
            } catch (IOException | RuntimeException e) {
                // Capture states from the inner extractor before rethrowing so that
                // getLinkedClusterStates() gives accurate data to DatafeedJob's catch block.
                List<LinkedClusterState> innerStates = currentExtractor.getLinkedClusterStates();
                if (innerStates.isEmpty() == false) {
                    lastLinkedClusterStates = DataExtractorUtils.preferRicherLinkedClusterStates(lastLinkedClusterStates, innerStates);
                }
                throw e;
            }
            lastSearchInterval = result.searchInterval();
            if (result.linkedClusterStates().isEmpty() == false) {
                lastLinkedClusterStates = DataExtractorUtils.preferRicherLinkedClusterStates(
                    lastLinkedClusterStates,
                    result.linkedClusterStates()
                );
            }
            if (shouldRetryWithSmallerEsqlChunk(result)) {
                if (result.data().isPresent()) {
                    result.data().get().close();
                }
                shrinkCurrentEsqlChunk(result.rowCount());
                continue;
            }
            if (incompleteSearchInterval == null && isIncompleteEsqlChunk(result)) {
                incompleteSearchInterval = result.searchInterval();
            }
            if (result.data().isPresent()) {
                return result;
            }

            if (isNewSearch && hasNext()) {
                // If it was a new search it means it returned 0 results. Thus,
                // we reconfigure and jump to the next time interval where there are data.
                // In theory, if everything is consistent, it would be sufficient to call
                // setUpChunkedSearch() here. However, the way that works is to take the
                // query from the datafeed config and add on some simple aggregations.
                // These aggregations are completely separate from any that might be defined
                // in the datafeed config. It is possible that the aggregations in the
                // datafeed config rather than the query are responsible for no data being
                // found. For example, "filter" or "bucket_selector" aggregations can do this.
                // Originally we thought this situation would never happen, with the query
                // selecting data and the aggregations just grouping it, but recently we've
                // seen cases of users filtering in the aggregations. Therefore, we
                // unconditionally advance the start time by one chunk here. setUpChunkedSearch()
                // might then advance substantially further, but in the pathological cases
                // where setUpChunkedSearch() thinks data exists at the current start time
                // while the datafeed's own aggregation doesn't, at least we'll step forward
                // a little bit rather than go into an infinite loop.
                currentStart += chunkSpan;
                setUpChunkedSearch();
            }
        }
        return new Result(lastSearchInterval, Optional.empty(), lastLinkedClusterStates);
    }

    /**
     * An ES|QL result is truncated only when it reached the row limit {@link EsqlDataExtractor} injects into the query
     * ({@link EsqlDataExtractor#INJECTED_ROW_LIMIT}); anything below it is complete, however many rows it holds.
     */
    private boolean isTruncatedEsqlResult(Result result) {
        return context.hasEsqlQuery() && result.rowCount() >= EsqlDataExtractor.INJECTED_ROW_LIMIT;
    }

    private boolean canShrinkEsqlChunk(Result result) {
        long intervalLength = result.searchInterval().endMs() - result.searchInterval().startMs();
        return esqlShrinkAttempts < MAX_ESQL_SHRINK_ATTEMPTS && intervalLength > context.timeAligner().alignToCeil(1L);
    }

    /**
     * Whether the chunk is still truncated after the allowed shrinks, or can no longer be shrunk (one grouping interval).
     */
    private boolean isIncompleteEsqlChunk(Result result) {
        return isTruncatedEsqlResult(result) && canShrinkEsqlChunk(result) == false;
    }

    private boolean shouldRetryWithSmallerEsqlChunk(Result result) {
        return isTruncatedEsqlResult(result) && canShrinkEsqlChunk(result);
    }

    private void shrinkCurrentEsqlChunk(long rowCount) {
        currentExtractor.destroy();
        long intervalLength = currentEnd - currentStart;
        long targetLength = Math.max(
            1L,
            intervalLength / rowCount * DEFAULT_ESQL_CHUNK_DOCS + intervalLength % rowCount * DEFAULT_ESQL_CHUNK_DOCS / rowCount
        );
        long shrunkEnd = context.timeAligner().alignToFloor(currentStart + targetLength);
        if (shrunkEnd <= currentStart) {
            shrunkEnd = currentStart + context.timeAligner().alignToCeil(1L);
        }
        currentEnd = Math.min(shrunkEnd, currentEnd);
        currentExtractor = dataExtractorFactory.newExtractor(currentStart, currentEnd);
        esqlShrinkAttempts++;
        LOGGER.debug("[{}] shrinks ES|QL chunk to [{}, {}) after receiving [{}] rows", context.jobId(), currentStart, currentEnd, rowCount);
    }

    private void advanceTime() {
        // Destroy the previous extractor to clean up any scroll contexts before creating a new one
        if (currentExtractor != null) {
            currentExtractor.destroy();
        }
        currentStart = currentEnd;
        currentEnd = Math.min(currentStart + chunkSpan, context.end());
        currentExtractor = dataExtractorFactory.newExtractor(currentStart, currentEnd);
        esqlShrinkAttempts = 0;
        LOGGER.debug("[{}] advances time to [{}, {})", context.jobId(), currentStart, currentEnd);
    }

    @Override
    public boolean isCancelled() {
        return isCancelled;
    }

    @Override
    public void cancel() {
        if (currentExtractor != null) {
            currentExtractor.cancel();
        }
        isCancelled = true;
    }

    @Override
    public void destroy() {
        cancel();
        if (currentExtractor != null) {
            currentExtractor.destroy();
        }
    }

    @Override
    public long getEndTime() {
        return context.end();
    }

    @Override
    public List<LinkedClusterState> getLinkedClusterStates() {
        return lastLinkedClusterStates;
    }

    @Override
    public Optional<SearchInterval> getIncompleteSearchInterval() {
        return Optional.ofNullable(incompleteSearchInterval);
    }

    ChunkedDataExtractorContext getContext() {
        return context;
    }
}
