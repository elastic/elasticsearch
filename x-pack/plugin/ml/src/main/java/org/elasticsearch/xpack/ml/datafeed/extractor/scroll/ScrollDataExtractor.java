/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.scroll;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.datafeed.SearchInterval;
import org.elasticsearch.xpack.core.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.core.ml.datafeed.extractor.ExtractorUtils;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter;
import org.elasticsearch.xpack.ml.extractor.ExtractedField;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * An implementation that extracts data from elasticsearch using search and scroll on a client.
 * It supports safe and responsive cancellation by continuing the scroll until a new timestamp
 * is seen.
 * Note that this class is NOT thread-safe.
 */
class ScrollDataExtractor implements DataExtractor {

    private static final TimeValue SCROLL_TIMEOUT = new TimeValue(30, TimeUnit.MINUTES);

    private static final Logger logger = LogManager.getLogger(ScrollDataExtractor.class);

    private final Client client;
    private final ScrollDataExtractorContext context;
    private final DatafeedTimingStatsReporter timingStatsReporter;
    private String scrollId;
    private boolean isCancelled;
    private boolean hasNext;
    private Long timestampOnCancel;
    protected Long lastTimestamp;
    private boolean searchHasShardFailure;

    ScrollDataExtractor(Client client, ScrollDataExtractorContext dataExtractorContext, DatafeedTimingStatsReporter timingStatsReporter) {
        this.client = Objects.requireNonNull(client);
        this.context = Objects.requireNonNull(dataExtractorContext);
        this.timingStatsReporter = Objects.requireNonNull(timingStatsReporter);
        hasNext = true;
        searchHasShardFailure = false;
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
        logger.trace("[{}] Data extractor received cancel request", context.jobId);
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
        Optional<InputStream> stream = tryNextStream();
        if (stream.isPresent() == false) {
            hasNext = false;
        }
        return new Result(new SearchInterval(context.start, context.end), stream);
    }

    private Optional<InputStream> tryNextStream() throws IOException {
        try {
            return scrollId == null ? Optional.ofNullable(initScroll(context.start)) : Optional.ofNullable(continueScroll());
        } catch (Exception e) {
            scrollId = null;
            if (searchHasShardFailure) {
                throw e;
            }
            logger.debug("[{}] Resetting scroll search after shard failure", context.jobId);
            markScrollAsErrored();
            return Optional.ofNullable(initScroll(lastTimestamp == null ? context.start : lastTimestamp));
        }
    }

    protected InputStream initScroll(long startTimestamp) throws IOException {
        logger.debug("[{}] Initializing scroll with start time [{}]", context.jobId, startTimestamp);
        SearchResponse searchResponse = executeSearchRequest(buildSearchRequest(startTimestamp));
        logger.debug("[{}] Search response was obtained", context.jobId);
        timingStatsReporter.reportSearchDuration(searchResponse.getTook());
        return processSearchResponse(searchResponse);
    }

    protected SearchResponse executeSearchRequest(SearchRequestBuilder searchRequestBuilder) {
        SearchResponse searchResponse = ClientHelper.executeWithHeaders(
            context.headers,
            ClientHelper.ML_ORIGIN,
            client,
            searchRequestBuilder::get
        );
        try {
            checkForSkippedClusters(searchResponse);
        } catch (ResourceNotFoundException e) {
            clearScrollLoggingExceptions(searchResponse.getScrollId());
            throw e;
        }
        return searchResponse;
    }

    private SearchRequestBuilder buildSearchRequest(long start) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().size(context.scrollSize)
            .sort(context.extractedFields.timeField(), SortOrder.ASC)
            .query(ExtractorUtils.wrapInTimeRangeQuery(context.query, context.extractedFields.timeField(), start, context.end))
            .runtimeMappings(context.runtimeMappings);

        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client, SearchAction.INSTANCE).setScroll(SCROLL_TIMEOUT)
            .setIndices(context.indices)
            .setIndicesOptions(context.indicesOptions)
            .setAllowPartialSearchResults(false)
            .setSource(searchSourceBuilder);

        for (ExtractedField docValueField : context.extractedFields.getDocValueFields()) {
            searchRequestBuilder.addDocValueField(docValueField.getSearchField(), docValueField.getDocValueFormat());
        }
        String[] sourceFields = context.extractedFields.getSourceFields();
        if (sourceFields.length == 0) {
            searchRequestBuilder.setFetchSource(false);
            searchRequestBuilder.storedFields(StoredFieldsContext._NONE_);
        } else {
            searchRequestBuilder.setFetchSource(sourceFields, null);
        }
        context.scriptFields.forEach(f -> searchRequestBuilder.addScriptField(f.fieldName(), f.script()));
        return searchRequestBuilder;
    }

    private InputStream processSearchResponse(SearchResponse searchResponse) throws IOException {

        scrollId = searchResponse.getScrollId();
        if (searchResponse.getHits().getHits().length == 0) {
            hasNext = false;
            clearScroll();
            return null;
        }

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (SearchHitToJsonProcessor hitProcessor = new SearchHitToJsonProcessor(context.extractedFields, outputStream)) {
            for (SearchHit hit : searchResponse.getHits().getHits()) {
                if (isCancelled) {
                    Long timestamp = context.extractedFields.timeFieldValue(hit);
                    if (timestamp != null) {
                        if (timestampOnCancel == null) {
                            timestampOnCancel = timestamp;
                        } else if (timestamp.equals(timestampOnCancel) == false) {
                            hasNext = false;
                            clearScroll();
                            break;
                        }
                    }
                }
                hitProcessor.process(hit);
            }
            SearchHit lastHit = searchResponse.getHits().getHits()[searchResponse.getHits().getHits().length - 1];
            lastTimestamp = context.extractedFields.timeFieldValue(lastHit);
        }
        return new ByteArrayInputStream(outputStream.toByteArray());
    }

    private InputStream continueScroll() throws IOException {
        logger.debug("[{}] Continuing scroll with id [{}]", context.jobId, scrollId);
        SearchResponse searchResponse;
        try {
            searchResponse = executeSearchScrollRequest(scrollId);
        } catch (SearchPhaseExecutionException searchExecutionException) {
            if (searchHasShardFailure) {
                throw searchExecutionException;
            }
            logger.debug("[{}] search failed due to SearchPhaseExecutionException. Will attempt again with new scroll", context.jobId);
            markScrollAsErrored();
            searchResponse = executeSearchRequest(buildSearchRequest(lastTimestamp == null ? context.start : lastTimestamp));
        }
        logger.debug("[{}] Search response was obtained", context.jobId);
        timingStatsReporter.reportSearchDuration(searchResponse.getTook());
        return processSearchResponse(searchResponse);
    }

    void markScrollAsErrored() {
        // This could be a transient error with the scroll Id.
        // Reinitialise the scroll and try again but only once.
        scrollId = null;
        if (lastTimestamp != null) {
            lastTimestamp++;
        }
        searchHasShardFailure = true;
    }

    @SuppressWarnings("HiddenField")
    protected SearchResponse executeSearchScrollRequest(String scrollId) {
        SearchResponse searchResponse = ClientHelper.executeWithHeaders(
            context.headers,
            ClientHelper.ML_ORIGIN,
            client,
            () -> new SearchScrollRequestBuilder(client, SearchScrollAction.INSTANCE).setScroll(SCROLL_TIMEOUT).setScrollId(scrollId).get()
        );
        try {
            checkForSkippedClusters(searchResponse);
        } catch (ResourceNotFoundException e) {
            clearScrollLoggingExceptions(searchResponse.getScrollId());
            throw e;
        }
        return searchResponse;
    }

    private void clearScroll() {
        innerClearScroll(scrollId);
        scrollId = null;
    }

    private void clearScrollLoggingExceptions(String scrollId) {
        try {
            innerClearScroll(scrollId);
        } catch (Exception e) {
            // This method is designed to be called from exception handlers, so just logs this exception
            // in the cleanup process so that the original exception can be propagated
            logger.error(new ParameterizedMessage("[{}] Failed to clear scroll", context.jobId), e);
        }
    }

    private void innerClearScroll(String scrollId) {
        if (scrollId != null) {
            ClearScrollRequest request = new ClearScrollRequest();
            request.addScrollId(scrollId);
            ClientHelper.executeWithHeaders(
                context.headers,
                ClientHelper.ML_ORIGIN,
                client,
                () -> client.execute(ClearScrollAction.INSTANCE, request).actionGet()
            );
        }
    }
}
