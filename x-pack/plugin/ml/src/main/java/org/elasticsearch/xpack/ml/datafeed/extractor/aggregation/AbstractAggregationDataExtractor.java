/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.aggregation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.core.ml.datafeed.extractor.ExtractorUtils;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter;

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
 * Abstract class for aggregated data extractors, e.g. {@link RollupDataExtractor}
 *
 * @param <T> The request builder type for getting data from ElasticSearch
 */
abstract class AbstractAggregationDataExtractor<T extends ActionRequestBuilder<SearchRequest, SearchResponse>>
    implements DataExtractor {

    private static final Logger LOGGER = LogManager.getLogger(AbstractAggregationDataExtractor.class);

    /**
     * The number of key-value pairs written in each batch to process.
     * This has to be a number that is small enough to allow for responsive
     * cancelling and big enough to not cause overhead by calling the
     * post data action too often. The value of 1000 was determined via
     * such testing.
     */
    private static int BATCH_KEY_VALUE_PAIRS = 1000;

    protected final Client client;
    protected final AggregationDataExtractorContext context;
    private final DatafeedTimingStatsReporter timingStatsReporter;
    private boolean hasNext;
    private boolean isCancelled;
    private AggregationToJsonProcessor aggregationToJsonProcessor;
    private ByteArrayOutputStream outputStream;

    AbstractAggregationDataExtractor(
            Client client, AggregationDataExtractorContext dataExtractorContext, DatafeedTimingStatsReporter timingStatsReporter) {
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

    private Aggregations search() {
        LOGGER.debug("[{}] Executing aggregated search", context.jobId);
        T searchRequest = buildSearchRequest(buildBaseSearchSource());
        assert searchRequest.request().allowPartialSearchResults() == false;
        SearchResponse searchResponse = executeSearchRequest(searchRequest);
        LOGGER.debug("[{}] Search response was obtained", context.jobId);
        timingStatsReporter.reportSearchDuration(searchResponse.getTook());
        return validateAggs(searchResponse.getAggregations());
    }

    private void initAggregationProcessor(Aggregations aggs) throws IOException {
        aggregationToJsonProcessor = new AggregationToJsonProcessor(context.timeField, context.fields, context.includeDocCount,
            context.start);
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

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
            .size(0)
            .query(ExtractorUtils.wrapInTimeRangeQuery(context.query, context.timeField, histogramSearchStartTime, context.end));

        context.aggs.getAggregatorFactories().forEach(searchSourceBuilder::aggregation);
        context.aggs.getPipelineAggregatorFactories().forEach(searchSourceBuilder::aggregation);
        return searchSourceBuilder;
    }

    protected abstract T buildSearchRequest(SearchSourceBuilder searchRequestBuilder);

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

    public AggregationDataExtractorContext getContext() {
        return context;
    }

}
