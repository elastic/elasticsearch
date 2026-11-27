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

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

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

    private final DataExtractorFactory dataExtractorFactory;
    private final ChunkedDataExtractorContext context;
    private final ChunkedDataExtractorEsqlSizing esqlSizing;
    private long currentStart;
    private long currentEnd;
    private long chunkSpan;
    private volatile boolean isCancelled;
    private volatile DataExtractor currentExtractor;
    private List<LinkedClusterState> lastLinkedClusterStates = List.of();

    ChunkedDataExtractor(DataExtractorFactory dataExtractorFactory, ChunkedDataExtractorContext context) {
        this.dataExtractorFactory = Objects.requireNonNull(dataExtractorFactory);
        this.context = Objects.requireNonNull(context);
        this.esqlSizing = new ChunkedDataExtractorEsqlSizing(context);
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
        if (context.hasEsqlQuery() && context.end() - currentStart <= context.singleChunkSpan()) {
            // The whole window is one chunk: query it directly instead of paying for a summary. If it is truncated, the
            // existing shrink path splits it.
            // Realtime costs one query per cycle only in the steady state, where window == frequency == singleChunkSpan. The first
            // cycle after lookback and late cycles have window > frequency, take the summary path below and cost two queries.
            long window = context.end() - currentStart;
            chunkSpan = context.chunkSpan() != null
                ? Math.min(window, context.timeAligner().alignToCeil(context.chunkSpan().getMillis()))
                : window;
            currentEnd = currentStart;
            esqlSizing.captureInitialChunkSpan(chunkSpan);
            LOGGER.debug("[{}] Chunked search configured: single chunk of {} ms, no summary", context.jobId(), chunkSpan);
            return;
        }
        // Keep a reference so that if getSummary() throws (e.g. because a remote cluster was skipped)
        // we can still recover the cluster states it observed and expose them via getLinkedClusterStates().
        DataExtractor summaryExtractor = dataExtractorFactory.newExtractor(currentStart, context.end());
        DataSummary dataSummary;
        try {
            dataSummary = summaryExtractor.getSummary();
        } catch (ResourceNotFoundException e) {
            recordFailedExtractorStates(summaryExtractor);
            throw e;
        }
        if (context.hasEsqlQuery()) {
            // A cycle that only runs the summary (no data) returns these states, so a recovered remote is not reported as skipped.
            lastLinkedClusterStates = DataExtractorUtils.preferRicherLinkedClusterStates(
                lastLinkedClusterStates,
                summaryExtractor.getLinkedClusterStates()
            );
        }
        if (dataSummary.hasData()) {
            long earliestTime = context.timeAligner().alignToFloor(dataSummary.earliestTime());
            // ES|QL summary MIN may precede the queried window (e.g. DATE_TRUNC). Jump forward so an
            // unbounded range skips empty chunks.
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
                    // Same uniform-density heuristic as DSL below: totalHits over timeSpread, target DEFAULT_ESQL_CHUNK_DOCS rows.
                    chunkSpan = Math.max(
                        MIN_CHUNK_SPAN,
                        ChunkedDataExtractorEsqlSizing.DEFAULT_ESQL_CHUNK_DOCS * timeSpread / dataSummary.totalHits()
                    );
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
            if (context.hasEsqlQuery()) {
                esqlSizing.captureInitialChunkSpan(chunkSpan);
            }
            LOGGER.debug("[{}] Chunked search configured: chunk span = {} ms", context.jobId(), chunkSpan);
        } else {
            // search is over
            currentEnd = context.end();
            LOGGER.debug("[{}] Chunked search configured: no data found", context.jobId());
        }
    }

    /**
     * Remembers the cluster states of an extractor whose query failed. For ES|QL the failed query's states replace the earlier ones, so
     * a remote that was available in a previous chunk but skipped in the failing one is still reported as skipped. The richer-state
     * preference stays for other extractors, where it keeps a newly linked remote visible.
     */
    private void recordFailedExtractorStates(DataExtractor failed) {
        List<LinkedClusterState> failedStates = failed.getLinkedClusterStates();
        if (failedStates.isEmpty()) {
            return;
        }
        lastLinkedClusterStates = context.hasEsqlQuery()
            ? List.copyOf(failedStates)
            : DataExtractorUtils.preferRicherLinkedClusterStates(lastLinkedClusterStates, failedStates);
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
                if (currentExtractor != null) {
                    recordFailedExtractorStates(currentExtractor);
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
            if (handleTruncatedEsqlResult(result)) {
                continue;
            }
            if (context.hasEsqlQuery() && result.data().isPresent()) {
                chunkSpan = esqlSizing.regrownSpan(chunkSpan, result.rowCount());
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

    private boolean handleTruncatedEsqlResult(Result result) throws IOException {
        if (context.hasEsqlQuery() == false || esqlSizing.isTruncated(result.rowCount()) == false) {
            return false;
        }
        OptionalLong shrunkEnd = esqlSizing.onTruncated(currentStart, currentEnd, isCancelled(), result.searchInterval());
        if (shrunkEnd.isEmpty()) {
            return false;
        }
        long newEnd = shrunkEnd.getAsLong();
        if (result.data().isPresent()) {
            result.data().get().close();
        }
        currentExtractor.destroy();
        currentEnd = newEnd;
        chunkSpan = esqlSizing.alignChunkSpanAfterShrink(chunkSpan, currentStart, currentEnd);
        installExtractor(dataExtractorFactory.newExtractor(currentStart, currentEnd));
        LOGGER.debug(
            "[{}] shrinks ES|QL chunk to [{}, {}) after receiving [{}] rows",
            context.jobId(),
            currentStart,
            currentEnd,
            result.rowCount()
        );
        return true;
    }

    /** Installs a new delegate extractor for the current chunk window. */
    private void installExtractor(DataExtractor newExtractor) {
        currentExtractor = newExtractor;
        // cancel() sets the flag before cancelling the delegate; re-check after install so a cancel during replacement still reaches it.
        if (isCancelled) {
            newExtractor.cancel();
        }
    }

    private void advanceTime() {
        // Destroy the previous extractor to clean up any scroll contexts before creating a new one
        if (currentExtractor != null) {
            currentExtractor.destroy();
        }
        currentStart = currentEnd;
        currentEnd = Math.min(currentStart + chunkSpan, context.end());
        installExtractor(dataExtractorFactory.newExtractor(currentStart, currentEnd));
        esqlSizing.onNewChunk();
        LOGGER.debug("[{}] advances time to [{}, {})", context.jobId(), currentStart, currentEnd);
    }

    @Override
    public boolean isCancelled() {
        return isCancelled;
    }

    @Override
    public void cancel() {
        isCancelled = true;
        DataExtractor delegate = currentExtractor;
        if (delegate != null) {
            delegate.cancel();
        }
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

    /** Span covering every truncated chunk; complete chunks between them may fall inside it. */
    @Override
    public Optional<SearchInterval> getIncompleteSearchInterval() {
        return esqlSizing.incompleteInterval();
    }

    ChunkedDataExtractorContext getContext() {
        return context;
    }
}
