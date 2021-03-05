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
import org.elasticsearch.client.Client;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.core.ml.datafeed.extractor.ExtractorUtils;
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
    private boolean hasNext;
    private boolean isCancelled;

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
        hasNext = false;
    }

    @Override
    public long getEndTime() {
        return context.end;
    }

    @Override
    public Optional<InputStream> next() throws IOException {
        if (hasNext() == false) {
            throw new NoSuchElementException();
        }

        Aggregations aggs = search();
        if (aggs == null) {
            LOGGER.trace(() -> new ParameterizedMessage("[{}] extraction finished", context.jobId));
            hasNext = false;
            afterKey = null;
            return Optional.empty();
        }
        return Optional.of(processAggs(aggs));
    }

    private Aggregations search() {
        long histogramSearchStartTime = Math.max(
            0,
            context.start - ExtractorUtils.getHistogramIntervalMillis(compositeAggregationBuilder)
        );
        LOGGER.trace(
            () -> new ParameterizedMessage(
                "[{}] Executing composite aggregated search from [{}] to [{}]",
                context.jobId,
                histogramSearchStartTime,
                context.end
            )
        );
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
            .size(0)
            .query(ExtractorUtils.wrapInTimeRangeQuery(context.query, context.timeField, histogramSearchStartTime, context.end));

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
        afterKey = compositeAgg.afterKey();

        return aggregations;
    }

    protected SearchResponse executeSearchRequest(ActionRequestBuilder<SearchRequest, SearchResponse> searchRequestBuilder) {
        return ClientHelper.executeWithHeaders(context.headers, ClientHelper.ML_ORIGIN, client, searchRequestBuilder::get);
    }

    private InputStream processAggs(Aggregations aggs) throws IOException {
        AggregationToJsonProcessor aggregationToJsonProcessor = new AggregationToJsonProcessor(
            context.timeField,
            context.fields,
            context.includeDocCount,
            context.start,
            context.compositeAggDateHistogramGroupSourceName
        );
        LOGGER.trace(() -> new ParameterizedMessage(
            "[{}] got [{}] composite buckets",
            context.jobId,
            ((CompositeAggregation)aggs.get(compositeAggregationBuilder.getName())).getBuckets().size()
        ));
        aggregationToJsonProcessor.process(aggs);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        boolean moreToWrite = aggregationToJsonProcessor.writeDocs(Integer.MAX_VALUE, outputStream);
        while (moreToWrite) {
            moreToWrite = aggregationToJsonProcessor.writeDocs(Integer.MAX_VALUE, outputStream);
        }
        return new ByteArrayInputStream(outputStream.toByteArray());
    }

}
