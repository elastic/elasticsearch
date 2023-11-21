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
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.datafeed.SearchInterval;
import org.elasticsearch.xpack.core.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.core.ml.datafeed.extractor.ExtractorUtils;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

/**
 * Abstract class for aggregated data extractors, e.g. {@link RollupDataExtractor}
 *
 * @param <T> The request builder type for getting data from ElasticSearch
 */
abstract class AbstractAggregationDataExtractor<T extends ActionRequestBuilder<SearchRequest, SearchResponse>> implements DataExtractor {

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
    public long getEndTime() {
        return context.end;
    }

    @Override
    public Result next() throws IOException {
        if (hasNext() == false) {
            throw new NoSuchElementException();
        }

        SearchInterval searchInterval = new SearchInterval(context.start, context.end);
        if (aggregationToJsonProcessor == null) {
            Aggregations aggs = search();
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

    private Aggregations search() {
        LOGGER.debug("[{}] Executing aggregated search", context.jobId);
        T searchRequest = buildSearchRequest(buildBaseSearchSource());
        assert searchRequest.request().allowPartialSearchResults() == false;
        SearchResponse searchResponse = executeSearchRequest(searchRequest);
        checkForSkippedClusters(searchResponse);
        LOGGER.debug("[{}] Search response was obtained", context.jobId);
        timingStatsReporter.reportSearchDuration(searchResponse.getTook());
        return validateAggs(searchResponse.getAggregations());
    }

    private void initAggregationProcessor(Aggregations aggs) throws IOException {
        aggregationToJsonProcessor = new AggregationToJsonProcessor(
            context.timeField,
            context.fields,
            context.includeDocCount,
            context.start,
            null
        );
        aggregationToJsonProcessor.process(aggs);
    }

    protected SearchResponse executeSearchRequest(T searchRequestBuilder) {
        return ClientHelper.executeWithHeaders(context.headers, ClientHelper.ML_ORIGIN, client, searchRequestBuilder::get);
    }

    private SearchSourceBuilder buildBaseSearchSource() {
        // For derivative aggregations the first bucket will always be null
        // so query one extra histogram bucket back and hope there is data
        // in that bucket
        long histogramSearchStartTime = Math.max(0, context.start - ExtractorUtils.getHistogramIntervalMillis(context.aggs));

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().size(0)
            .query(ExtractorUtils.wrapInTimeRangeQuery(context.query, context.timeField, histogramSearchStartTime, context.end));

        if (context.runtimeMappings.isEmpty() == false) {
            searchSourceBuilder.runtimeMappings(context.runtimeMappings);
        }
        context.aggs.getAggregatorFactories().forEach(searchSourceBuilder::aggregation);
        context.aggs.getPipelineAggregatorFactories().forEach(searchSourceBuilder::aggregation);
        return searchSourceBuilder;
    }

    protected abstract T buildSearchRequest(SearchSourceBuilder searchRequestBuilder);

    private static Aggregations validateAggs(@Nullable Aggregations aggs) {
        if (aggs == null) {
            return null;
        }
        List<Aggregation> aggsAsList = aggs.asList();
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

}
