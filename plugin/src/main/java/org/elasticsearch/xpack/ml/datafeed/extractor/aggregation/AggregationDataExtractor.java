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
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.ml.datafeed.extractor.ExtractorUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.LinkedList;
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
    private LinkedList<Histogram.Bucket> histogramBuckets;

    AggregationDataExtractor(Client client, AggregationDataExtractorContext dataExtractorContext) {
        this.client = Objects.requireNonNull(client);
        this.context = Objects.requireNonNull(dataExtractorContext);
        this.hasNext = true;
        this.isCancelled = false;
        this.histogramBuckets = null;
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
        if (histogramBuckets == null) {
            histogramBuckets = search();
        }
        return Optional.ofNullable(processNextBatch());
    }

    private LinkedList<Histogram.Bucket> search() throws IOException {
        if (histogramBuckets != null) {
            throw new IllegalStateException("search should only be performed once");
        }
        LOGGER.debug("[{}] Executing aggregated search", context.jobId);
        SearchResponse searchResponse = executeSearchRequest(buildSearchRequest());
        ExtractorUtils.checkSearchWasSuccessful(context.jobId, searchResponse);
        return new LinkedList<>(getHistogramBuckets(searchResponse.getAggregations()));
    }

    protected SearchResponse executeSearchRequest(SearchRequestBuilder searchRequestBuilder) {
        return searchRequestBuilder.get();
    }

    private SearchRequestBuilder buildSearchRequest() {
        SearchRequestBuilder searchRequestBuilder = SearchAction.INSTANCE.newRequestBuilder(client)
                .setIndices(context.indexes)
                .setTypes(context.types)
                .setSize(0)
                .setQuery(ExtractorUtils.wrapInTimeRangeQuery(context.query, context.timeField, context.start, context.end));

        context.aggs.getAggregatorFactories().forEach(a -> searchRequestBuilder.addAggregation(a));
        context.aggs.getPipelineAggregatorFactories().forEach(a -> searchRequestBuilder.addAggregation(a));
        return searchRequestBuilder;
    }

    private List<Histogram.Bucket> getHistogramBuckets(@Nullable Aggregations aggs) {
        if (aggs == null) {
            return Collections.emptyList();
        }
        List<Aggregation> aggsAsList = aggs.asList();
        if (aggsAsList.isEmpty()) {
            return Collections.emptyList();
        }
        if (aggsAsList.size() > 1) {
            throw new IllegalArgumentException("Multiple top level aggregations not supported; found: "
                    + aggsAsList.stream().map(a -> a.getName()).collect(Collectors.toList()));
        }

        Aggregation topAgg = aggsAsList.get(0);
        if (topAgg instanceof Histogram) {
            return ((Histogram) topAgg).getBuckets();
        } else {
            throw new IllegalArgumentException("Top level aggregation should be [histogram]");
        }
    }

    private InputStream processNextBatch() throws IOException {
        if (histogramBuckets.isEmpty()) {
            hasNext = false;
            return null;
        }

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (AggregationToJsonProcessor processor = new AggregationToJsonProcessor(
                context.timeField, context.includeDocCount, outputStream)) {
            while (histogramBuckets.isEmpty() == false && processor.getKeyValueCount() < BATCH_KEY_VALUE_PAIRS) {
                processor.process(histogramBuckets.removeFirst());
            }
            if (histogramBuckets.isEmpty()) {
                hasNext = false;
            }
        }
        return new ByteArrayInputStream(outputStream.toByteArray());
    }
}
