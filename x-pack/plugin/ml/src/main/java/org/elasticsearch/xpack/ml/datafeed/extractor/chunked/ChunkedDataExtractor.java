/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.chunked;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.core.ml.datafeed.extractor.ExtractorUtils;
import org.elasticsearch.xpack.core.rollup.action.RollupSearchAction;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;
import org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.RollupDataExtractorFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

/**
 * A wrapper {@link DataExtractor} that can be used with other extractors in order to perform
 * searches in smaller chunks of the time range.
 *
 * <p> The chunk span can be either specified or not. When not specified,
 * a heuristic is employed (see {@link DataSummary#estimateChunk()}) to automatically determine the chunk span.
 * The search is set up (see {@link #setUpChunkedSearch()} by querying a data summary for the given time range
 * that includes the number of total hits and the earliest/latest times. Those are then used to determine the chunk span,
 * when necessary, and to jump the search forward to the time where the earliest data can be found.
 * If a search for a chunk returns empty, the set up is performed again for the remaining time.
 *
 * <p> Cancellation's behaviour depends on the delegate extractor.
 *
 * <p> Note that this class is NOT thread-safe.
 */
public class ChunkedDataExtractor implements DataExtractor {

    interface DataSummary {
        long estimateChunk();
        boolean hasData();
        long earliestTime();
        long getDataTimeSpread();
    }

    private static final Logger LOGGER = LogManager.getLogger(ChunkedDataExtractor.class);

    private static final String EARLIEST_TIME = "earliest_time";
    private static final String LATEST_TIME = "latest_time";

    /** Let us set a minimum chunk span of 1 minute */
    private static final long MIN_CHUNK_SPAN = 60000L;

    private final Client client;
    private final DataExtractorFactory dataExtractorFactory;
    private final ChunkedDataExtractorContext context;
    private final DataSummaryFactory dataSummaryFactory;
    private final DatafeedTimingStatsReporter timingStatsReporter;
    private long currentStart;
    private long currentEnd;
    private long chunkSpan;
    private boolean isCancelled;
    private DataExtractor currentExtractor;

    public ChunkedDataExtractor(
            Client client,
            DataExtractorFactory dataExtractorFactory,
            ChunkedDataExtractorContext context,
            DatafeedTimingStatsReporter timingStatsReporter) {
        this.client = Objects.requireNonNull(client);
        this.dataExtractorFactory = Objects.requireNonNull(dataExtractorFactory);
        this.context = Objects.requireNonNull(context);
        this.timingStatsReporter = Objects.requireNonNull(timingStatsReporter);
        this.currentStart = context.start;
        this.currentEnd = context.start;
        this.isCancelled = false;
        this.dataSummaryFactory = new DataSummaryFactory();
    }

    @Override
    public boolean hasNext() {
        boolean currentHasNext = currentExtractor != null && currentExtractor.hasNext();
        if (isCancelled()) {
            return currentHasNext;
        }
        return currentHasNext ||  currentEnd < context.end;
    }

    @Override
    public Optional<InputStream> next() throws IOException {
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
        DataSummary dataSummary = dataSummaryFactory.buildDataSummary();
        if (dataSummary.hasData()) {
            currentStart = context.timeAligner.alignToFloor(dataSummary.earliestTime());
            currentEnd = currentStart;
            chunkSpan = context.chunkSpan == null ? dataSummary.estimateChunk() : context.chunkSpan.getMillis();
            chunkSpan = context.timeAligner.alignToCeil(chunkSpan);
            LOGGER.debug("[{}] Chunked search configured: kind = {}, dataTimeSpread = {} ms, chunk span = {} ms",
                    context.jobId, dataSummary.getClass().getSimpleName(), dataSummary.getDataTimeSpread(), chunkSpan);
        } else {
            // search is over
            currentEnd = context.end;
        }
    }

    protected SearchResponse executeSearchRequest(ActionRequestBuilder<SearchRequest, SearchResponse> searchRequestBuilder) {
        return ClientHelper.executeWithHeaders(context.headers, ClientHelper.ML_ORIGIN, client, searchRequestBuilder::get);
    }

    private Optional<InputStream> getNextStream() throws IOException {
        while (hasNext()) {
            boolean isNewSearch = false;

            if (currentExtractor == null || currentExtractor.hasNext() == false) {
                // First search or the current search finished; we can advance to the next search
                advanceTime();
                isNewSearch = true;
            }

            Optional<InputStream> nextStream = currentExtractor.next();
            if (nextStream.isPresent()) {
                return nextStream;
            }

            if (isNewSearch && hasNext()) {
                // If it was a new search it means it returned 0 results. Thus,
                // we reconfigure and jump to the next time interval where there are data.
                setUpChunkedSearch();
            }
        }
        return Optional.empty();
    }

    private void advanceTime() {
        currentStart = currentEnd;
        currentEnd = Math.min(currentStart + chunkSpan, context.end);
        currentExtractor = dataExtractorFactory.newExtractor(currentStart, currentEnd);
        LOGGER.trace("[{}] advances time to [{}, {})", context.jobId, currentStart, currentEnd);
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
    public long getEndTime() {
        return context.end;
    }

    ChunkedDataExtractorContext getContext() {
        return context;
    }

    private class DataSummaryFactory {

        /**
         * If there are aggregations, an AggregatedDataSummary object is created. It returns a ScrollingDataSummary otherwise.
         *
         * By default a DatafeedConfig with aggregations, should already have a manual ChunkingConfig created.
         * However, the end user could have specifically set the ChunkingConfig to AUTO, which would not really work for aggregations.
         * So, if we need to gather an appropriate chunked time for aggregations, we can utilize the AggregatedDataSummary
         *
         * @return DataSummary object
         */
        private DataSummary buildDataSummary() {
            return context.hasAggregations ? newAggregatedDataSummary() : newScrolledDataSummary();
        }

        private DataSummary newScrolledDataSummary() {
            SearchRequestBuilder searchRequestBuilder = rangeSearchRequest();

            SearchResponse searchResponse = executeSearchRequest(searchRequestBuilder);
            LOGGER.debug("[{}] Scrolling Data summary response was obtained", context.jobId);
            timingStatsReporter.reportSearchDuration(searchResponse.getTook());

            long earliestTime = 0;
            long latestTime = 0;
            long totalHits = searchResponse.getHits().getTotalHits().value;
            if (totalHits > 0) {
                Aggregations aggregations = searchResponse.getAggregations();
                Min min = aggregations.get(EARLIEST_TIME);
                earliestTime = (long) min.getValue();
                Max max = aggregations.get(LATEST_TIME);
                latestTime = (long) max.getValue();
            }
            return new ScrolledDataSummary(earliestTime, latestTime, totalHits);
        }

        private DataSummary newAggregatedDataSummary() {
            // TODO: once RollupSearchAction is changed from indices:admin* to indices:data/read/* this branch is not needed
            ActionRequestBuilder<SearchRequest, SearchResponse> searchRequestBuilder =
                dataExtractorFactory instanceof RollupDataExtractorFactory ? rollupRangeSearchRequest() : rangeSearchRequest();
            SearchResponse searchResponse = executeSearchRequest(searchRequestBuilder);
            LOGGER.debug("[{}] Aggregating Data summary response was obtained", context.jobId);
            timingStatsReporter.reportSearchDuration(searchResponse.getTook());

            long totalHits = searchResponse.getHits().getTotalHits().value;
            if (totalHits == 0) {
                // This can happen if all the indices the datafeed is searching are deleted after it started
                return AggregatedDataSummary.noDataSummary(context.histogramInterval);
            }
            Aggregations aggregations = searchResponse.getAggregations();
            Min min = aggregations.get(EARLIEST_TIME);
            Max max = aggregations.get(LATEST_TIME);
            return new AggregatedDataSummary(min.getValue(), max.getValue(), context.histogramInterval);
        }

        private SearchSourceBuilder rangeSearchBuilder() {
            return new SearchSourceBuilder()
                .size(0)
                .query(ExtractorUtils.wrapInTimeRangeQuery(context.query, context.timeField, currentStart, context.end))
                .runtimeMappings(context.runtimeMappings)
                .aggregation(AggregationBuilders.min(EARLIEST_TIME).field(context.timeField))
                .aggregation(AggregationBuilders.max(LATEST_TIME).field(context.timeField));
        }

        private SearchRequestBuilder rangeSearchRequest() {
            return new SearchRequestBuilder(client, SearchAction.INSTANCE)
                .setIndices(context.indices)
                .setIndicesOptions(context.indicesOptions)
                .setSource(rangeSearchBuilder())
                .setAllowPartialSearchResults(false)
                .setTrackTotalHits(true);
        }

        private RollupSearchAction.RequestBuilder rollupRangeSearchRequest() {
            SearchRequest searchRequest = new SearchRequest().indices(context.indices)
                .indicesOptions(context.indicesOptions)
                .allowPartialSearchResults(false)
                .source(rangeSearchBuilder());
            return new RollupSearchAction.RequestBuilder(client, searchRequest);
        }
    }

    private class ScrolledDataSummary implements DataSummary {

        private final long earliestTime;
        private final long latestTime;
        private final long totalHits;

        private ScrolledDataSummary(long earliestTime, long latestTime, long totalHits) {
            this.earliestTime = earliestTime;
            this.latestTime = latestTime;
            this.totalHits = totalHits;
        }

        @Override
        public long earliestTime() {
            return earliestTime;
        }

        @Override
        public long getDataTimeSpread() {
            return latestTime - earliestTime;
        }

        /**
         *  The heuristic here is that we want a time interval where we expect roughly scrollSize documents
         * (assuming data are uniformly spread over time).
         * We have totalHits documents over dataTimeSpread (latestTime - earliestTime), we want scrollSize documents over chunk.
         * Thus, the interval would be (scrollSize * dataTimeSpread) / totalHits.
         * However, assuming this as the chunk span may often lead to half-filled pages or empty searches.
         * It is beneficial to take a multiple of that. Based on benchmarking, we set this to 10x.
         */
        @Override
        public long estimateChunk() {
            long dataTimeSpread = getDataTimeSpread();
            if (totalHits <= 0 || dataTimeSpread <= 0) {
                return context.end - currentEnd;
            }
            long estimatedChunk = 10 * (context.scrollSize * getDataTimeSpread()) / totalHits;
            return Math.max(estimatedChunk, MIN_CHUNK_SPAN);
        }

        @Override
        public boolean hasData() {
            return totalHits > 0;
        }
    }

    static class AggregatedDataSummary implements DataSummary {

        private final double earliestTime;
        private final double latestTime;
        private final long histogramIntervalMillis;

        static AggregatedDataSummary noDataSummary(long histogramInterval) {
            // hasData() uses infinity to mean no data
            return new AggregatedDataSummary(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, histogramInterval);
        }

        AggregatedDataSummary(double earliestTime, double latestTime, long histogramInterval) {
            this.earliestTime = earliestTime;
            this.latestTime = latestTime;
            this.histogramIntervalMillis = histogramInterval;
        }

        /**
         * This heuristic is a direct copy of the manual chunking config auto-creation done in {@link DatafeedConfig}
         */
        @Override
        public long estimateChunk() {
            return DatafeedConfig.DEFAULT_AGGREGATION_CHUNKING_BUCKETS * histogramIntervalMillis;
        }

        @Override
        public boolean hasData() {
            return (Double.isInfinite(earliestTime) || Double.isInfinite(latestTime)) == false;
        }

        @Override
        public long earliestTime() {
            return (long)earliestTime;
        }

        @Override
        public long getDataTimeSpread() {
            return (long)latestTime - (long)earliestTime;
        }
    }
}
