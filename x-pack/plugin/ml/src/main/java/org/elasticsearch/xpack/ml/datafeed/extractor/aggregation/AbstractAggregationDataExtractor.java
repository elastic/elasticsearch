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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfigUtils;
import org.elasticsearch.xpack.core.ml.datafeed.SearchInterval;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorQueryContext;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

/**
 * Abstract class for aggregated data extractors, e.g. {@link RollupDataExtractor}
 */
abstract class AbstractAggregationDataExtractor implements DataExtractor {

    private static final Logger LOGGER = LogManager.getLogger(AbstractAggregationDataExtractor.class);

    protected final Client client;
    protected final AggregationDataExtractorContext context;
    private final DatafeedTimingStatsReporter timingStatsReporter;
    private boolean hasNext;
    private volatile boolean isCancelled;
    private AggregationToJsonProcessor aggregationToJsonProcessor;
    private final ByteArrayOutputStream outputStream;

    AbstractAggregationDataExtractor(
        Client client,
        AggregationDataExtractorContext dataExtractorContext,
        DatafeedTimingStatsReporter timingStatsReporter
    ) {
        this.client = Objects.requireNonNull(client);
        this.context = Objects.requireNonNull(dataExtractorContext);
        this.timingStatsReporter = Objects.requireNonNull(timingStatsReporter);
        this.hasNext = true;
        this.isCancelled = false;
        this.outputStream = new ByteArrayOutputStream();
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
        hasNext = false;
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
        if (aggregationToJsonProcessor == null) {
            InternalAggregations aggs = search();
            if (aggs == null) {
                hasNext = false;
                return new Result(searchInterval, Optional.empty());
            }
            initAggregationProcessor(aggs);
        }

        outputStream.reset();
        // We can cancel immediately as we process whole date_histogram buckets at a time
        aggregationToJsonProcessor.writeAllDocsCancellable(_timestamp -> isCancelled, outputStream);
        // We process the whole search. So, if we are chunking or not, we have nothing more to process given the current query
        hasNext = false;

        return new Result(
            searchInterval,
            aggregationToJsonProcessor.getKeyValueCount() > 0
                ? Optional.of(new ByteArrayInputStream(outputStream.toByteArray()))
                : Optional.empty()
        );
    }

    private InternalAggregations search() {
        LOGGER.debug("[{}] Executing aggregated search", context.jobId);
        ActionRequestBuilder<SearchRequest, SearchResponse> searchRequest = buildSearchRequest(buildBaseSearchSource());
        assert searchRequest.request().allowPartialSearchResults() == false;
        SearchResponse searchResponse = executeSearchRequest(client, context.queryContext, searchRequest);
        try {
            LOGGER.debug("[{}] Search response was obtained", context.jobId);
            timingStatsReporter.reportSearchDuration(searchResponse.getTook());
            return validateAggs(searchResponse.getAggregations());
        } finally {
            searchResponse.decRef();
        }
    }

    private void initAggregationProcessor(InternalAggregations aggs) throws IOException {
        aggregationToJsonProcessor = new AggregationToJsonProcessor(
            context.queryContext.timeField,
            context.fields,
            context.includeDocCount,
            context.queryContext.start,
            null
        );
        aggregationToJsonProcessor.process(aggs);
    }

    static SearchResponse executeSearchRequest(
        Client client,
        DataExtractorQueryContext context,
        ActionRequestBuilder<SearchRequest, SearchResponse> searchRequestBuilder
    ) {
        SearchResponse searchResponse = ClientHelper.executeWithHeaders(
            context.headers,
            ClientHelper.ML_ORIGIN,
            client,
            searchRequestBuilder::get
        );
        boolean success = false;
        try {
            DataExtractorUtils.checkForSkippedClusters(searchResponse);
            success = true;
        } finally {
            if (success == false) {
                searchResponse.decRef();
            }
        }
        return searchResponse;
    }

    private SearchSourceBuilder buildBaseSearchSource() {
        // For derivative aggregations the first bucket will always be null
        // so query one extra histogram bucket back and hope there is data
        // in that bucket
        long histogramSearchStartTime = Math.max(
            0,
            context.queryContext.start - DatafeedConfigUtils.getHistogramIntervalMillis(context.aggs)
        );

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().size(0)
            .query(
                DataExtractorUtils.wrapInTimeRangeQuery(
                    context.queryContext.query,
                    context.queryContext.timeField,
                    histogramSearchStartTime,
                    context.queryContext.end
                )
            );

        if (context.queryContext.runtimeMappings.isEmpty() == false) {
            searchSourceBuilder.runtimeMappings(context.queryContext.runtimeMappings);
        }
        context.aggs.getAggregatorFactories().forEach(searchSourceBuilder::aggregation);
        context.aggs.getPipelineAggregatorFactories().forEach(searchSourceBuilder::aggregation);
        return searchSourceBuilder;
    }

    protected abstract ActionRequestBuilder<SearchRequest, SearchResponse> buildSearchRequest(SearchSourceBuilder searchRequestBuilder);

    private static InternalAggregations validateAggs(@Nullable InternalAggregations aggs) {
        if (aggs == null) {
            return null;
        }
        List<InternalAggregation> aggsAsList = aggs.asList();
        if (aggsAsList.isEmpty()) {
            return null;
        }
        if (aggsAsList.size() > 1) {
            throw new IllegalArgumentException(
                "Multiple top level aggregations not supported; found: " + aggsAsList.stream().map(Aggregation::getName).toList()
            );
        }

        return aggs;
    }

    public AggregationDataExtractorContext getContext() {
        return context;
    }

    @Override
    public DataSummary getSummary() {
        ActionRequestBuilder<SearchRequest, SearchResponse> searchRequestBuilder = buildSearchRequest(
            DataExtractorUtils.getSearchSourceBuilderForSummary(context.queryContext)
        );
        SearchResponse searchResponse = executeSearchRequest(client, context.queryContext, searchRequestBuilder);
        try {
            LOGGER.debug("[{}] Aggregating Data summary response was obtained", context.jobId);
            timingStatsReporter.reportSearchDuration(searchResponse.getTook());
            return DataExtractorUtils.getDataSummary(searchResponse);
        } finally {
            searchResponse.decRef();
        }
    }
}
