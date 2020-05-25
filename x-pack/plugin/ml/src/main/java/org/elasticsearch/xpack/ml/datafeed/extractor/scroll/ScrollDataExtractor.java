/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.scroll;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.core.ClientHelper;
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

    private static final Logger LOGGER = LogManager.getLogger(ScrollDataExtractor.class);
    private static final TimeValue SCROLL_TIMEOUT = new TimeValue(30, TimeUnit.MINUTES);

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
        LOGGER.trace("[{}] Data extractor received cancel request", context.jobId);
        isCancelled = true;
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
        Optional<InputStream> stream = tryNextStream();
        if (!stream.isPresent()) {
            hasNext = false;
        }
        return stream;
    }

    private Optional<InputStream> tryNextStream() throws IOException {
        try {
            return scrollId == null ?
                Optional.ofNullable(initScroll(context.start)) : Optional.ofNullable(continueScroll());
        } catch (Exception e) {
            scrollId = null;
            if (searchHasShardFailure) {
                throw e;
            }
            LOGGER.debug("[{}] Resetting scroll search after shard failure", context.jobId);
            markScrollAsErrored();
            return Optional.ofNullable(initScroll(lastTimestamp == null ? context.start : lastTimestamp));
        }
    }

    protected InputStream initScroll(long startTimestamp) throws IOException {
        LOGGER.debug("[{}] Initializing scroll", context.jobId);
        SearchResponse searchResponse = executeSearchRequest(buildSearchRequest(startTimestamp));
        LOGGER.debug("[{}] Search response was obtained", context.jobId);
        timingStatsReporter.reportSearchDuration(searchResponse.getTook());
        return processSearchResponse(searchResponse);
    }

    protected SearchResponse executeSearchRequest(SearchRequestBuilder searchRequestBuilder) {
        return ClientHelper.executeWithHeaders(context.headers, ClientHelper.ML_ORIGIN, client, searchRequestBuilder::get);
    }

    private SearchRequestBuilder buildSearchRequest(long start) {
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client, SearchAction.INSTANCE)
                .setScroll(SCROLL_TIMEOUT)
                .addSort(context.extractedFields.timeField(), SortOrder.ASC)
                .setIndices(context.indices)
                .setIndicesOptions(context.indicesOptions)
                .setSize(context.scrollSize)
                .setAllowPartialSearchResults(false)
                .setQuery(ExtractorUtils.wrapInTimeRangeQuery(
                        context.query, context.extractedFields.timeField(), start, context.end));

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
            SearchHit lastHit = searchResponse.getHits().getHits()[searchResponse.getHits().getHits().length -1];
            lastTimestamp = context.extractedFields.timeFieldValue(lastHit);
        }
        return new ByteArrayInputStream(outputStream.toByteArray());
    }

    private InputStream continueScroll() throws IOException {
        LOGGER.debug("[{}] Continuing scroll with id [{}]", context.jobId, scrollId);
        SearchResponse searchResponse;
        try {
             searchResponse = executeSearchScrollRequest(scrollId);
        } catch (SearchPhaseExecutionException searchExecutionException) {
            if (searchHasShardFailure) {
                throw searchExecutionException;
            }
            LOGGER.debug("[{}] search failed due to SearchPhaseExecutionException. Will attempt again with new scroll",
                context.jobId);
            markScrollAsErrored();
            searchResponse = executeSearchRequest(buildSearchRequest(lastTimestamp == null ? context.start : lastTimestamp));
        }
        LOGGER.debug("[{}] Search response was obtained", context.jobId);
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

    protected SearchResponse executeSearchScrollRequest(String scrollId) {
        return ClientHelper.executeWithHeaders(context.headers, ClientHelper.ML_ORIGIN, client,
            () -> new SearchScrollRequestBuilder(client, SearchScrollAction.INSTANCE)
                .setScroll(SCROLL_TIMEOUT)
                .setScrollId(scrollId)
                .get());
    }

    private void clearScroll() {
        if (scrollId != null) {
            ClearScrollRequest request = new ClearScrollRequest();
            request.addScrollId(scrollId);
            ClientHelper.executeWithHeaders(context.headers, ClientHelper.ML_ORIGIN, client,
                    () -> client.execute(ClearScrollAction.INSTANCE, request).actionGet());
            scrollId = null;
        }
    }
}
