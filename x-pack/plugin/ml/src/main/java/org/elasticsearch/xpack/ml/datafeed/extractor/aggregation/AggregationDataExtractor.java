/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.aggregation;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.core.ml.datafeed.extractor.ExtractorUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * An implementation that extracts data from elasticsearch using search with aggregations on a client.
 * The first time {@link #next()} is called, the search is executed. The result aggregations are
 * stored and they are then processed in batches. Cancellation is supported between batches.
 * Note that this class is NOT thread-safe.
 */
class AggregationDataExtractor implements DataExtractor {

    private static final Logger LOGGER = Loggers.getLogger(AggregationDataExtractor.class);

    /**
     * The number of key-value pairs written in each batch to process.
     * This has to be a number that is small enough to allow for responsive
     * cancelling and big enough to not cause overhead by calling the
     * post data action too often. The value of 1000 was determined via
     * such testing.
     */
    private static int BATCH_KEY_VALUE_PAIRS = 1000;

    private final Client client;
    private final AggregationDataExtractorContext context;
    private boolean hasNext;
    private boolean isCancelled;
    private AggregationToJsonProcessor aggregationToJsonProcessor;
    private ByteArrayOutputStream outputStream;

    AggregationDataExtractor(Client client, AggregationDataExtractorContext dataExtractorContext) {
        this.client = Objects.requireNonNull(client);
        context = Objects.requireNonNull(dataExtractorContext);
        hasNext = true;
        isCancelled = false;
        outputStream = new ByteArrayOutputStream();
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
        LOGGER.trace("[{}] Data extractor received cancel request", context.jobId);
        isCancelled = true;
        hasNext = false;
    }

    @Override
    public Optional<InputStream> next() throws IOException {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        if (aggregationToJsonProcessor == null) {
            Aggregations aggs = search();
            if (aggs == null) {
                hasNext = false;
                return Optional.empty();
            }
            initAggregationProcessor(aggs);
        }

        return Optional.ofNullable(processNextBatch());
    }

    private Aggregations search() throws IOException {
        LOGGER.debug("[{}] Executing aggregated search", context.jobId);
        SearchResponse searchResponse = executeSearchRequest(buildSearchRequest());
        LOGGER.debug("[{}] Search response was obtained", context.jobId);
        ExtractorUtils.checkSearchWasSuccessful(context.jobId, searchResponse);
        return validateAggs(searchResponse.getAggregations());
    }

    private void initAggregationProcessor(Aggregations aggs) throws IOException {
        aggregationToJsonProcessor = new AggregationToJsonProcessor(context.timeField, context.fields, context.includeDocCount,
                context.start);
        aggregationToJsonProcessor.process(aggs);
    }

    protected SearchResponse executeSearchRequest(SearchRequestBuilder searchRequestBuilder) {
        return ClientHelper.executeWithHeaders(context.headers, ClientHelper.ML_ORIGIN, client, searchRequestBuilder::get);
    }

    private SearchRequestBuilder buildSearchRequest() {
        // For derivative aggregations the first bucket will always be null
        // so query one extra histogram bucket back and hope there is data
        // in that bucket
        long histogramSearchStartTime = Math.max(0, context.start - getHistogramInterval());

        SearchRequestBuilder searchRequestBuilder = SearchAction.INSTANCE.newRequestBuilder(client)
                .setIndices(context.indices)
                .setTypes(context.types)
                .setSize(0)
                .setQuery(ExtractorUtils.wrapInTimeRangeQuery(context.query, context.timeField, histogramSearchStartTime, context.end));

        context.aggs.getAggregatorFactories().forEach(searchRequestBuilder::addAggregation);
        context.aggs.getPipelineAggregatorFactories().forEach(searchRequestBuilder::addAggregation);
        return searchRequestBuilder;
    }

    private Aggregations validateAggs(@Nullable Aggregations aggs) {
        if (aggs == null) {
            return null;
        }
        List<Aggregation> aggsAsList = aggs.asList();
        if (aggsAsList.isEmpty()) {
            return null;
        }
        if (aggsAsList.size() > 1) {
            throw new IllegalArgumentException("Multiple top level aggregations not supported; found: "
                    + aggsAsList.stream().map(Aggregation::getName).collect(Collectors.toList()));
        }

        return aggs;
    }

    private InputStream processNextBatch() throws IOException {
        outputStream.reset();

        hasNext = aggregationToJsonProcessor.writeDocs(BATCH_KEY_VALUE_PAIRS, outputStream);
        return new ByteArrayInputStream(outputStream.toByteArray());
    }

    private long getHistogramInterval() {
        return ExtractorUtils.getHistogramIntervalMillis(context.aggs);
    }

    AggregationDataExtractorContext getContext() {
        return context;
    }
}
