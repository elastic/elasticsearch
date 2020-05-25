/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.extractor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
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
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.DataFrameAnalysis;
import org.elasticsearch.xpack.ml.dataframe.DestinationIndex;
import org.elasticsearch.xpack.ml.extractor.ExtractedField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
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

    public static final String NULL_VALUE = "\0";

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

    public Map<String, String> getHeaders() {
        return Collections.unmodifiableMap(context.headers);
    }

    public boolean hasNext() {
        return hasNext;
    }

    public boolean isCancelled() {
        return isCancelled;
    }

    public void cancel() {
        LOGGER.debug("[{}] Data extractor was cancelled", context.jobId);
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
        return tryRequestWithSearchResponse(() -> executeSearchRequest(buildSearchRequest()));
    }

    private List<Row> tryRequestWithSearchResponse(Supplier<SearchResponse> request) throws IOException {
        try {
            // We've set allow_partial_search_results to false which means if something
            // goes wrong the request will throw.
            SearchResponse searchResponse = request.get();
            LOGGER.debug("[{}] Search response was obtained", context.jobId);

            // Request was successful so we can restore the flag to retry if a future failure occurs
            searchHasShardFailure = false;

            return processSearchResponse(searchResponse);
        } catch (Exception e) {
            if (searchHasShardFailure) {
                throw e;
            }
            LOGGER.warn(new ParameterizedMessage("[{}] Search resulted to failure; retrying once", context.jobId), e);
            markScrollAsErrored();
            return initScroll();
        }
    }

    protected SearchResponse executeSearchRequest(SearchRequestBuilder searchRequestBuilder) {
        return ClientHelper.executeWithHeaders(context.headers, ClientHelper.ML_ORIGIN, client, searchRequestBuilder::get);
    }

    private SearchRequestBuilder buildSearchRequest() {
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client, SearchAction.INSTANCE)
                .setScroll(SCROLL_TIMEOUT)
                // This ensures the search throws if there are failures and the scroll context gets cleared automatically
                .setAllowPartialSearchResults(false)
                .addSort(DestinationIndex.ID_COPY, SortOrder.ASC)
                .setIndices(context.indices)
                .setSize(context.scrollSize)
                .setQuery(context.query);
        setFetchSource(searchRequestBuilder);

        for (ExtractedField docValueField : context.extractedFields.getDocValueFields()) {
            searchRequestBuilder.addDocValueField(docValueField.getSearchField(), docValueField.getDocValueFormat());
        }

        return searchRequestBuilder;
    }

    private void setFetchSource(SearchRequestBuilder searchRequestBuilder) {
        if (context.includeSource) {
            searchRequestBuilder.setFetchSource(true);
        } else {
            String[] sourceFields = context.extractedFields.getSourceFields();
            if (sourceFields.length == 0) {
                searchRequestBuilder.setFetchSource(false);
                searchRequestBuilder.storedFields(StoredFieldsContext._NONE_);
            } else {
                searchRequestBuilder.setFetchSource(sourceFields, null);
            }
        }
    }

    private List<Row> processSearchResponse(SearchResponse searchResponse) throws IOException {
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
            if (values.length == 1 && (values[0] instanceof Number || values[0] instanceof String)) {
                extractedValues[i] = Objects.toString(values[0]);
            } else {
                if (values.length == 0 && context.supportsRowsWithMissingValues) {
                    // if values is empty then it means it's a missing value
                    extractedValues[i] = NULL_VALUE;
                } else {
                    // we are here if we have a missing value but the analysis does not support those
                    // or the value type is not supported (e.g. arrays, etc.)
                    extractedValues = null;
                    break;
                }
            }
        }
        return new Row(extractedValues, hit);
    }

    private List<Row> continueScroll() throws IOException {
        LOGGER.debug("[{}] Continuing scroll with id [{}]", context.jobId, scrollId);
        return tryRequestWithSearchResponse(() -> executeSearchScrollRequest(scrollId));
    }

    private void markScrollAsErrored() {
        // This could be a transient error with the scroll Id.
        // Reinitialise the scroll and try again but only once.
        scrollId = null;
        searchHasShardFailure = true;
    }

    protected SearchResponse executeSearchScrollRequest(String scrollId) {
        return ClientHelper.executeWithHeaders(context.headers, ClientHelper.ML_ORIGIN, client,
                () -> new SearchScrollRequestBuilder(client, SearchScrollAction.INSTANCE)
                .setScroll(SCROLL_TIMEOUT)
                .setScrollId(scrollId)
                .get());
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
        return context.extractedFields.getAllFields().stream().map(ExtractedField::getName).collect(Collectors.toList());
    }

    public List<ExtractedField> getAllExtractedFields() {
        return context.extractedFields.getAllFields();
    }

    public DataSummary collectDataSummary() {
        SearchRequestBuilder searchRequestBuilder = buildDataSummarySearchRequestBuilder();
        SearchResponse searchResponse = executeSearchRequest(searchRequestBuilder);
        long rows = searchResponse.getHits().getTotalHits().value;
        LOGGER.debug("[{}] Data summary rows [{}]", context.jobId, rows);
        return new DataSummary(rows, context.extractedFields.getAllFields().size());
    }

    public void collectDataSummaryAsync(ActionListener<DataSummary> dataSummaryActionListener) {
        SearchRequestBuilder searchRequestBuilder = buildDataSummarySearchRequestBuilder();
        final int numberOfFields = context.extractedFields.getAllFields().size();

        ClientHelper.executeWithHeadersAsync(context.headers,
            ClientHelper.ML_ORIGIN,
            client,
            SearchAction.INSTANCE,
            searchRequestBuilder.request(),
            ActionListener.wrap(
                searchResponse -> dataSummaryActionListener.onResponse(
                    new DataSummary(searchResponse.getHits().getTotalHits().value, numberOfFields)),
            dataSummaryActionListener::onFailure
        ));
    }

    private SearchRequestBuilder buildDataSummarySearchRequestBuilder() {

        QueryBuilder summaryQuery = context.query;
        if (context.supportsRowsWithMissingValues == false) {
            summaryQuery = QueryBuilders.boolQuery()
                .filter(summaryQuery)
                .filter(allExtractedFieldsExistQuery());
        }

        return new SearchRequestBuilder(client, SearchAction.INSTANCE)
            .setIndices(context.indices)
            .setSize(0)
            .setQuery(summaryQuery)
            .setTrackTotalHits(true);
    }

    private QueryBuilder allExtractedFieldsExistQuery() {
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        for (ExtractedField field : context.extractedFields.getAllFields()) {
            query.filter(QueryBuilders.existsQuery(field.getName()));
        }
        return query;
    }

    public Set<String> getCategoricalFields(DataFrameAnalysis analysis) {
        return ExtractedFieldsDetector.getCategoricalFields(context.extractedFields, analysis);
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
