/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.aggregation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.datafeed.SearchInterval;
import org.elasticsearch.xpack.core.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.core.ml.datafeed.extractor.ExtractorUtils;
import org.elasticsearch.xpack.core.ml.utils.Intervals;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

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
        this.interval = ExtractorUtils.getHistogramIntervalMillis(compositeAggregationBuilder);
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
        LOGGER.debug(() -> new ParameterizedMessage("[{}] Data extractor received cancel request", context.jobId));
        isCancelled = true;
    }

    @Override
    public long getEndTime() {
        return context.end;
    }

    @Override
    public Result next() throws IOException {
        if (hasNext() == false) {
            throw new NoSuchElementException();
        }

        SearchInterval searchInterval = new SearchInterval(context.start, context.end);
        Aggregations aggs = search();
        if (aggs == null) {
            LOGGER.trace(() -> new ParameterizedMessage("[{}] extraction finished", context.jobId));
            hasNext = false;
            afterKey = null;
            return new Result(searchInterval, Optional.empty());
        }
        return new Result(searchInterval, Optional.of(processAggs(aggs)));
    }

    private Aggregations search() {
        // Compare to the normal aggregation implementation, this search does not search for the previous bucket's data.
        // For composite aggs, since it is scrolling, it is not really possible to know the previous pages results in the current page.
        // Aggregations like derivative cannot work within composite aggs, for now.
        // Also, it doesn't make sense to have a derivative when grouping by time AND by some other criteria.

        LOGGER.trace(
            () -> new ParameterizedMessage(
                "[{}] Executing composite aggregated search from [{}] to [{}]",
                context.jobId,
                context.start,
                context.end
            )
        );
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().size(0)
            .query(ExtractorUtils.wrapInTimeRangeQuery(context.query, context.timeField, context.start, context.end));

        if (context.runtimeMappings.isEmpty() == false) {
            searchSourceBuilder.runtimeMappings(context.runtimeMappings);
        }
        if (afterKey != null) {
            compositeAggregationBuilder.aggregateAfter(afterKey);
        }
        searchSourceBuilder.aggregation(compositeAggregationBuilder);
        ActionRequestBuilder<SearchRequest, SearchResponse> searchRequest = requestBuilder.build(searchSourceBuilder);
        SearchResponse searchResponse = executeSearchRequest(searchRequest);
        LOGGER.trace(() -> new ParameterizedMessage("[{}] Search composite response was obtained", context.jobId));
        timingStatsReporter.reportSearchDuration(searchResponse.getTook());
        Aggregations aggregations = searchResponse.getAggregations();
        if (aggregations == null) {
            return null;
        }
        CompositeAggregation compositeAgg = aggregations.get(compositeAggregationBuilder.getName());
        if (compositeAgg == null || compositeAgg.getBuckets().isEmpty()) {
            return null;
        }
        return aggregations;
    }

    protected SearchResponse executeSearchRequest(ActionRequestBuilder<SearchRequest, SearchResponse> searchRequestBuilder) {
        SearchResponse searchResponse = ClientHelper.executeWithHeaders(
            context.headers,
            ClientHelper.ML_ORIGIN,
            client,
            searchRequestBuilder::get
        );
        checkForSkippedClusters(searchResponse);
        return searchResponse;
    }

    private InputStream processAggs(Aggregations aggs) throws IOException {
        AggregationToJsonProcessor aggregationToJsonProcessor = new AggregationToJsonProcessor(
            context.timeField,
            context.fields,
            context.includeDocCount,
            context.start,
            context.compositeAggDateHistogramGroupSourceName
        );
        LOGGER.trace(
            () -> new ParameterizedMessage(
                "[{}] got [{}] composite buckets",
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
                        () -> new ParameterizedMessage(
                            "[{}] set future timestamp cancel to [{}] via timestamp [{}]",
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
                () -> new ParameterizedMessage(
                    "[{}] cancelled before bucket [{}] on date_histogram page [{}]",
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

}
