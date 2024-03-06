/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.aggregation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfigUtils;
import org.elasticsearch.xpack.core.ml.datafeed.SearchInterval;
import org.elasticsearch.xpack.core.ml.utils.Intervals;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.core.Strings.format;

/**
 * An implementation that extracts data from elasticsearch using search with composite aggregations on a client.
 * The first time {@link #next()} is called, the search is executed. All the aggregated buckets from the composite agg are then
 * returned. Subsequent calls to {@link #next()} execute additional searches, moving forward the composite agg `afterKey`.
 *
 * It's like scroll, but with aggs.
 *
 * It is not thread-safe, we reuse underlying objects without synchronization (like a pre-constructed composite agg object)
 */
class CompositeAggregationDataExtractor implements DataExtractor {

    private static final Logger LOGGER = LogManager.getLogger(CompositeAggregationDataExtractor.class);

    private volatile Map<String, Object> afterKey = null;
    private final CompositeAggregationBuilder compositeAggregationBuilder;
    private final Client client;
    private final CompositeAggregationDataExtractorContext context;
    private final DatafeedTimingStatsReporter timingStatsReporter;
    private final AggregatedSearchRequestBuilder requestBuilder;
    private final long interval;
    private volatile boolean isCancelled;
    private volatile long nextBucketOnCancel;
    private boolean hasNext;

    CompositeAggregationDataExtractor(
        CompositeAggregationBuilder compositeAggregationBuilder,
        Client client,
        CompositeAggregationDataExtractorContext dataExtractorContext,
        DatafeedTimingStatsReporter timingStatsReporter,
        AggregatedSearchRequestBuilder requestBuilder
    ) {
        this.compositeAggregationBuilder = Objects.requireNonNull(compositeAggregationBuilder);
        this.client = Objects.requireNonNull(client);
        this.context = Objects.requireNonNull(dataExtractorContext);
        this.timingStatsReporter = Objects.requireNonNull(timingStatsReporter);
        this.requestBuilder = Objects.requireNonNull(requestBuilder);
        this.interval = DatafeedConfigUtils.getHistogramIntervalMillis(compositeAggregationBuilder);
        this.hasNext = true;
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public boolean isCancelled() {
        return isCancelled;
    }

    @Override
    public void cancel() {
        LOGGER.debug("[{}] Data extractor received cancel request", context.jobId);
        isCancelled = true;
    }

    @Override
    public void destroy() {
        cancel();
    }

    @Override
    public long getEndTime() {
        return context.queryContext.end;
    }

    @Override
    public Result next() throws IOException {
        if (hasNext() == false) {
            throw new NoSuchElementException();
        }

        SearchInterval searchInterval = new SearchInterval(context.queryContext.start, context.queryContext.end);
        InternalAggregations aggs = search();
        if (aggs == null) {
            LOGGER.trace("[{}] extraction finished", context.jobId);
            hasNext = false;
            afterKey = null;
            return new Result(searchInterval, Optional.empty());
        }
        return new Result(searchInterval, Optional.of(processAggs(aggs)));
    }

    private InternalAggregations search() {
        // Compare to the normal aggregation implementation, this search does not search for the previous bucket's data.
        // For composite aggs, since it is scrolling, it is not really possible to know the previous pages results in the current page.
        // Aggregations like derivative cannot work within composite aggs, for now.
        // Also, it doesn't make sense to have a derivative when grouping by time AND by some other criteria.

        LOGGER.trace(
            () -> format(
                "[%s] Executing composite aggregated search from [%s] to [%s]",
                context.jobId,
                context.queryContext.start,
                context.queryContext.end
            )
        );
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().size(0)
            .query(
                DataExtractorUtils.wrapInTimeRangeQuery(
                    context.queryContext.query,
                    context.queryContext.timeField,
                    context.queryContext.start,
                    context.queryContext.end
                )
            );

        if (context.queryContext.runtimeMappings.isEmpty() == false) {
            searchSourceBuilder.runtimeMappings(context.queryContext.runtimeMappings);
        }
        if (afterKey != null) {
            compositeAggregationBuilder.aggregateAfter(afterKey);
        }
        searchSourceBuilder.aggregation(compositeAggregationBuilder);
        ActionRequestBuilder<SearchRequest, SearchResponse> searchRequest = requestBuilder.build(searchSourceBuilder);
        SearchResponse searchResponse = AbstractAggregationDataExtractor.executeSearchRequest(client, context.queryContext, searchRequest);
        try {
            LOGGER.trace("[{}] Search composite response was obtained", context.jobId);
            timingStatsReporter.reportSearchDuration(searchResponse.getTook());
            InternalAggregations aggregations = searchResponse.getAggregations();
            if (aggregations == null) {
                return null;
            }
            CompositeAggregation compositeAgg = aggregations.get(compositeAggregationBuilder.getName());
            if (compositeAgg == null || compositeAgg.getBuckets().isEmpty()) {
                return null;
            }
            return aggregations;
        } finally {
            searchResponse.decRef();
        }
    }

    private InputStream processAggs(InternalAggregations aggs) throws IOException {
        AggregationToJsonProcessor aggregationToJsonProcessor = new AggregationToJsonProcessor(
            context.queryContext.timeField,
            context.fields,
            context.includeDocCount,
            context.queryContext.start,
            context.compositeAggDateHistogramGroupSourceName
        );
        LOGGER.trace(
            () -> format(
                "[%s] got [%s] composite buckets",
                context.jobId,
                ((CompositeAggregation) aggs.get(compositeAggregationBuilder.getName())).getBuckets().size()
            )
        );
        aggregationToJsonProcessor.process(aggs);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final Long afterKeyTimeBucket = afterKey != null ? (Long) afterKey.get(context.compositeAggDateHistogramGroupSourceName) : null;
        boolean cancellable = aggregationToJsonProcessor.writeAllDocsCancellable(timestamp -> {
            if (isCancelled) {
                // If we have not processed a single composite agg page yet and we are cancelled
                // We should not process anything
                if (afterKeyTimeBucket == null) {
                    return true;
                }
                // We want to stop processing once a timestamp enters the next time bucket.
                // This could occur in any page. One benefit we have is that even though the paging order is not sorted
                // by max timestamp, our iteration of the page results is. So, once we cross over to the next bucket within
                // a given page, we know the previous bucket has been exhausted.
                if (nextBucketOnCancel == 0L) {
                    // This simple equation handles two unique scenarios:
                    // If the timestamp is the current floor, this means we need to keep processing until the next timebucket
                    // If we are not matching the current bucket floor, then this simply aligns to the next bucket
                    nextBucketOnCancel = Intervals.alignToFloor(timestamp + interval, interval);
                    LOGGER.debug(
                        () -> format(
                            "[%s] set future timestamp cancel to [%s] via timestamp [%s]",
                            context.jobId,
                            nextBucketOnCancel,
                            timestamp
                        )
                    );
                }
                return timestamp >= nextBucketOnCancel;
            }
            return false;
        }, outputStream);
        // If the process is canceled and cancelable, then we can indicate that there are no more buckets to process.
        if (isCancelled && cancellable) {
            LOGGER.debug(
                () -> format(
                    "[%s] cancelled before bucket [%s] on date_histogram page [%s]",
                    context.jobId,
                    nextBucketOnCancel,
                    afterKeyTimeBucket != null ? afterKeyTimeBucket : "__null__"
                )
            );
            hasNext = false;
        }
        // Only set the after key once we have processed the search, allows us to cancel on the first page
        CompositeAggregation compositeAgg = aggs.get(compositeAggregationBuilder.getName());
        afterKey = compositeAgg.afterKey();

        return new ByteArrayInputStream(outputStream.toByteArray());
    }

    @Override
    public DataSummary getSummary() {
        ActionRequestBuilder<SearchRequest, SearchResponse> searchRequestBuilder = DataExtractorUtils.getSearchRequestBuilderForSummary(
            client,
            context.queryContext
        );
        SearchResponse searchResponse = AbstractAggregationDataExtractor.executeSearchRequest(
            client,
            context.queryContext,
            searchRequestBuilder
        );
        try {
            LOGGER.debug("[{}] Aggregating Data summary response was obtained", context.jobId);
            timingStatsReporter.reportSearchDuration(searchResponse.getTook());
            return DataExtractorUtils.getDataSummary(searchResponse);
        } finally {
            searchResponse.decRef();
        }
    }
}
