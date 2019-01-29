/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.extractor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.datafeed.extractor.ExtractorUtils;
import org.elasticsearch.xpack.ml.datafeed.extractor.fields.ExtractedField;
import org.elasticsearch.xpack.ml.dataframe.DataFrameAnalyticsFields;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * An implementation that extracts data from elasticsearch using search and scroll on a client.
 * It supports safe and responsive cancellation by continuing the scroll until a new timestamp
 * is seen.
 * Note that this class is NOT thread-safe.
 */
public class DataFrameDataExtractor {

    private static final Logger LOGGER = LogManager.getLogger(DataFrameDataExtractor.class);
    private static final TimeValue SCROLL_TIMEOUT = new TimeValue(30, TimeUnit.MINUTES);

    private final Client client;
    private final DataFrameDataExtractorContext context;
    private String scrollId;
    private boolean isCancelled;
    private boolean hasNext;
    private boolean searchHasShardFailure;

    DataFrameDataExtractor(Client client, DataFrameDataExtractorContext context) {
        this.client = Objects.requireNonNull(client);
        this.context = Objects.requireNonNull(context);
        hasNext = true;
        searchHasShardFailure = false;
    }

    public boolean hasNext() {
        return hasNext;
    }

    public boolean isCancelled() {
        return isCancelled;
    }

    public void cancel() {
        isCancelled = true;
    }

    public Optional<List<Row>> next() throws IOException {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        Optional<List<Row>> hits = scrollId == null ? Optional.ofNullable(initScroll()) : Optional.ofNullable(continueScroll());
        if (!hits.isPresent()) {
            hasNext = false;
        }
        return hits;
    }

    protected List<Row> initScroll() throws IOException {
        LOGGER.debug("[{}] Initializing scroll", context.jobId);
        SearchResponse searchResponse = executeSearchRequest(buildSearchRequest());
        LOGGER.debug("[{}] Search response was obtained", context.jobId);
        return processSearchResponse(searchResponse);
    }

    protected SearchResponse executeSearchRequest(SearchRequestBuilder searchRequestBuilder) {
        return ClientHelper.executeWithHeaders(context.headers, ClientHelper.ML_ORIGIN, client, searchRequestBuilder::get);
    }

    private SearchRequestBuilder buildSearchRequest() {
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client, SearchAction.INSTANCE)
                .setScroll(SCROLL_TIMEOUT)
                .addSort(DataFrameAnalyticsFields.ID, SortOrder.ASC)
                .setIndices(context.indices)
                .setSize(context.scrollSize)
                .setQuery(context.query)
                .setFetchSource(context.includeSource);

        for (ExtractedField docValueField : context.extractedFields.getDocValueFields()) {
            searchRequestBuilder.addDocValueField(docValueField.getName(), docValueField.getDocValueFormat());
        }

        return searchRequestBuilder;
    }

    private List<Row> processSearchResponse(SearchResponse searchResponse) throws IOException {

        if (searchResponse.getFailedShards() > 0 && searchHasShardFailure == false) {
            LOGGER.debug("[{}] Resetting scroll search after shard failure", context.jobId);
            markScrollAsErrored();
            return initScroll();
        }

        ExtractorUtils.checkSearchWasSuccessful(context.jobId, searchResponse);
        scrollId = searchResponse.getScrollId();
        if (searchResponse.getHits().getHits().length == 0) {
            hasNext = false;
            clearScroll(scrollId);
            return null;
        }

        SearchHit[] hits = searchResponse.getHits().getHits();
        List<Row> rows = new ArrayList<>(hits.length);
        for (SearchHit hit : hits) {
            if (isCancelled) {
                hasNext = false;
                clearScroll(scrollId);
                break;
            }
            rows.add(createRow(hit));
        }
        return rows;

    }

    private Row createRow(SearchHit hit) {
        String[] extractedValues = new String[context.extractedFields.getAllFields().size()];
        for (int i = 0; i < extractedValues.length; ++i) {
            ExtractedField field = context.extractedFields.getAllFields().get(i);
            Object[] values = field.value(hit);
            if (values.length == 1 && values[0] instanceof Number) {
                extractedValues[i] = Objects.toString(values[0]);
            } else {
                extractedValues = null;
                break;
            }
        }
        return new Row(extractedValues, hit);
    }

    private List<Row> continueScroll() throws IOException {
        LOGGER.debug("[{}] Continuing scroll with id [{}]", context.jobId, scrollId);
        SearchResponse searchResponse = executeSearchScrollRequest(scrollId);
        LOGGER.debug("[{}] Search response was obtained", context.jobId);
        return processSearchResponse(searchResponse);
    }

    private void markScrollAsErrored() {
        // This could be a transient error with the scroll Id.
        // Reinitialise the scroll and try again but only once.
        resetScroll();
        searchHasShardFailure = true;
    }

    protected SearchResponse executeSearchScrollRequest(String scrollId) {
        return ClientHelper.executeWithHeaders(context.headers, ClientHelper.ML_ORIGIN, client,
                () -> new SearchScrollRequestBuilder(client, SearchScrollAction.INSTANCE)
                .setScroll(SCROLL_TIMEOUT)
                .setScrollId(scrollId)
                .get());
    }

    private void resetScroll() {
        clearScroll(scrollId);
        scrollId = null;
    }

    private void clearScroll(String scrollId) {
        if (scrollId != null) {
            ClearScrollRequest request = new ClearScrollRequest();
            request.addScrollId(scrollId);
            ClientHelper.executeWithHeaders(context.headers, ClientHelper.ML_ORIGIN, client,
                    () -> client.execute(ClearScrollAction.INSTANCE, request).actionGet());
        }
    }

    public List<String> getFieldNames() {
        return context.extractedFields.getAllFields().stream().map(ExtractedField::getAlias).collect(Collectors.toList());
    }

    public DataSummary collectDataSummary() {
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client, SearchAction.INSTANCE)
            .setIndices(context.indices)
            .setSize(0)
            .setQuery(context.query)
            .setTrackTotalHits(true);

        SearchResponse searchResponse = executeSearchRequest(searchRequestBuilder);
        return new DataSummary(searchResponse.getHits().getTotalHits().value, context.extractedFields.getAllFields().size());
    }

    public static class DataSummary {

        public final long rows;
        public final int cols;

        public DataSummary(long rows, int cols) {
            this.rows = rows;
            this.cols = cols;
        }
    }

    public static class Row {

        private SearchHit hit;

        @Nullable
        private String[] values;

        private Row(String[] values, SearchHit hit) {
            this.values = values;
            this.hit = hit;
        }

        @Nullable
        public String[] getValues() {
            return values;
        }

        public SearchHit getHit() {
            return hit;
        }

        public boolean shouldSkip() {
            return values == null;
        }

        public int getChecksum() {
            return Arrays.hashCode(values);
        }
    }
}
