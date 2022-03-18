/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.dataframe.extractor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.CachedSupplier;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.DataFrameAnalysis;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.dataframe.DestinationIndex;
import org.elasticsearch.xpack.ml.dataframe.traintestsplit.TrainTestSplitter;
import org.elasticsearch.xpack.ml.extractor.ExtractedField;
import org.elasticsearch.xpack.ml.extractor.ExtractedFields;
import org.elasticsearch.xpack.ml.extractor.ProcessedField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An implementation that extracts data from elasticsearch using ranged searches
 * on the incremental id.
 * We detect the end of the extraction by doing an additional search at the end
 * which should return empty results.
 * It supports safe and responsive cancellation by continuing from the latest
 * incremental id that was seen.
 * Note that this class is NOT thread-safe.
 */
public class DataFrameDataExtractor {

    private static final Logger LOGGER = LogManager.getLogger(DataFrameDataExtractor.class);

    public static final String NULL_VALUE = "\0";

    private final Client client;
    private final DataFrameDataExtractorContext context;
    private long lastSortKey = -1;
    private boolean isCancelled;
    private boolean hasNext;
    private boolean hasPreviousSearchFailed;
    private final CachedSupplier<TrainTestSplitter> trainTestSplitter;
    // These are fields that are sent directly to the analytics process
    // They are not passed through a feature_processor
    private final String[] organicFeatures;
    // These are the output field names for the feature_processors
    private final String[] processedFeatures;
    private final Map<String, ExtractedField> extractedFieldsByName;

    DataFrameDataExtractor(Client client, DataFrameDataExtractorContext context) {
        this.client = Objects.requireNonNull(client);
        this.context = Objects.requireNonNull(context);
        this.organicFeatures = context.extractedFields.extractOrganicFeatureNames();
        this.processedFeatures = context.extractedFields.extractProcessedFeatureNames();
        this.extractedFieldsByName = new LinkedHashMap<>();
        context.extractedFields.getAllFields().forEach(f -> this.extractedFieldsByName.put(f.getName(), f));
        hasNext = true;
        hasPreviousSearchFailed = false;
        this.trainTestSplitter = new CachedSupplier<>(context.trainTestSplitterFactory::create);
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
        LOGGER.debug(() -> new ParameterizedMessage("[{}] Data extractor was cancelled", context.jobId));
        isCancelled = true;
    }

    public Optional<List<Row>> next() throws IOException {
        if (hasNext() == false) {
            throw new NoSuchElementException();
        }

        Optional<List<Row>> hits = Optional.ofNullable(nextSearch());
        if (hits.isPresent() && hits.get().isEmpty() == false) {
            lastSortKey = hits.get().get(hits.get().size() - 1).getSortKey();
        } else {
            hasNext = false;
        }
        return hits;
    }

    /**
     * Provides a preview of the data. Assumes this was created from the source indices.
     * Does no sorting of the results.
     * @param listener To alert with the extracted rows
     */
    public void preview(ActionListener<List<Row>> listener) {

        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client, SearchAction.INSTANCE)
            // This ensures the search throws if there are failures and the scroll context gets cleared automatically
            .setAllowPartialSearchResults(false)
            .setIndices(context.indices)
            .setSize(context.scrollSize)
            .setQuery(QueryBuilders.boolQuery().filter(context.query));

        setFetchSource(searchRequestBuilder);

        for (ExtractedField docValueField : context.extractedFields.getDocValueFields()) {
            searchRequestBuilder.addDocValueField(docValueField.getSearchField(), docValueField.getDocValueFormat());
        }

        searchRequestBuilder.setRuntimeMappings(context.runtimeMappings);

        ClientHelper.executeWithHeadersAsync(
            context.headers,
            ClientHelper.ML_ORIGIN,
            client,
            SearchAction.INSTANCE,
            searchRequestBuilder.request(),
            ActionListener.wrap(searchResponse -> {
                if (searchResponse.getHits().getHits().length == 0) {
                    listener.onResponse(Collections.emptyList());
                    return;
                }

                final SearchHit[] hits = searchResponse.getHits().getHits();
                List<Row> rows = new ArrayList<>(hits.length);
                for (SearchHit hit : hits) {
                    String[] extractedValues = extractValues(hit);
                    rows.add(extractedValues == null ? new Row(null, hit, true) : new Row(extractedValues, hit, false));
                }
                listener.onResponse(rows);
            }, listener::onFailure)
        );
    }

    protected List<Row> nextSearch() throws IOException {
        return tryRequestWithSearchResponse(() -> executeSearchRequest(buildSearchRequest()));
    }

    private List<Row> tryRequestWithSearchResponse(Supplier<SearchResponse> request) throws IOException {
        try {

            // We've set allow_partial_search_results to false which means if something
            // goes wrong the request will throw.
            SearchResponse searchResponse = request.get();
            LOGGER.trace(() -> new ParameterizedMessage("[{}] Search response was obtained", context.jobId));

            List<Row> rows = processSearchResponse(searchResponse);

            // Request was successfully executed and processed so we can restore the flag to retry if a future failure occurs
            hasPreviousSearchFailed = false;

            return rows;
        } catch (Exception e) {
            if (hasPreviousSearchFailed) {
                throw e;
            }
            LOGGER.warn(new ParameterizedMessage("[{}] Search resulted to failure; retrying once", context.jobId), e);
            markScrollAsErrored();
            return nextSearch();
        }
    }

    protected SearchResponse executeSearchRequest(SearchRequestBuilder searchRequestBuilder) {
        return ClientHelper.executeWithHeaders(context.headers, ClientHelper.ML_ORIGIN, client, searchRequestBuilder::get);
    }

    private SearchRequestBuilder buildSearchRequest() {
        long from = lastSortKey + 1;
        long to = from + context.scrollSize;

        LOGGER.trace(
            () -> new ParameterizedMessage(
                "[{}] Searching docs with [{}] in [{}, {})",
                context.jobId,
                DestinationIndex.INCREMENTAL_ID,
                from,
                to
            )
        );

        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client, SearchAction.INSTANCE)
            // This ensures the search throws if there are failures and the scroll context gets cleared automatically
            .setAllowPartialSearchResults(false)
            .addSort(DestinationIndex.INCREMENTAL_ID, SortOrder.ASC)
            .setIndices(context.indices)
            .setSize(context.scrollSize);

        searchRequestBuilder.setQuery(
            QueryBuilders.boolQuery()
                .filter(context.query)
                .filter(QueryBuilders.rangeQuery(DestinationIndex.INCREMENTAL_ID).gte(from).lt(to))
        );

        setFetchSource(searchRequestBuilder);

        for (ExtractedField docValueField : context.extractedFields.getDocValueFields()) {
            searchRequestBuilder.addDocValueField(docValueField.getSearchField(), docValueField.getDocValueFormat());
        }

        searchRequestBuilder.setRuntimeMappings(context.runtimeMappings);

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

    private List<Row> processSearchResponse(SearchResponse searchResponse) {
        if (searchResponse.getHits().getHits().length == 0) {
            hasNext = false;
            return null;
        }

        SearchHit[] hits = searchResponse.getHits().getHits();
        List<Row> rows = new ArrayList<>(hits.length);
        for (SearchHit hit : hits) {
            if (isCancelled) {
                hasNext = false;
                break;
            }
            rows.add(createRow(hit));
        }
        return rows;
    }

    private String extractNonProcessedValues(SearchHit hit, String organicFeature) {
        ExtractedField field = extractedFieldsByName.get(organicFeature);
        Object[] values = field.value(hit);
        if (values.length == 1 && isValidValue(values[0])) {
            return Objects.toString(values[0]);
        }
        if (values.length == 0 && context.supportsRowsWithMissingValues) {
            // if values is empty then it means it's a missing value
            return NULL_VALUE;
        }
        // we are here if we have a missing value but the analysis does not support those
        // or the value type is not supported (e.g. arrays, etc.)
        return null;
    }

    private String[] extractProcessedValue(ProcessedField processedField, SearchHit hit) {
        Object[] values = processedField.value(hit, extractedFieldsByName::get);
        if (values.length == 0 && context.supportsRowsWithMissingValues == false) {
            return null;
        }
        final String[] extractedValue = new String[processedField.getOutputFieldNames().size()];
        for (int i = 0; i < processedField.getOutputFieldNames().size(); i++) {
            extractedValue[i] = NULL_VALUE;
        }
        // if values is empty then it means it's a missing value
        if (values.length == 0) {
            return extractedValue;
        }

        if (values.length != processedField.getOutputFieldNames().size()) {
            throw ExceptionsHelper.badRequestException(
                "field_processor [{}] output size expected to be [{}], instead it was [{}]",
                processedField.getProcessorName(),
                processedField.getOutputFieldNames().size(),
                values.length
            );
        }

        for (int i = 0; i < processedField.getOutputFieldNames().size(); ++i) {
            Object value = values[i];
            if (value == null && context.supportsRowsWithMissingValues) {
                continue;
            }
            if (isValidValue(value) == false) {
                // we are here if we have a missing value but the analysis does not support those
                // or the value type is not supported (e.g. arrays, etc.)
                return null;
            }
            extractedValue[i] = Objects.toString(value);
        }
        return extractedValue;
    }

    private Row createRow(SearchHit hit) {
        String[] extractedValues = extractValues(hit);
        if (extractedValues == null) {
            return new Row(null, hit, true);
        }
        boolean isTraining = trainTestSplitter.get().isTraining(extractedValues);
        Row row = new Row(extractedValues, hit, isTraining);
        LOGGER.trace(
            () -> new ParameterizedMessage(
                "[{}] Extracted row: sort key = [{}], is_training = [{}], values = {}",
                context.jobId,
                row.getSortKey(),
                isTraining,
                Arrays.toString(row.values)
            )
        );
        return row;
    }

    private String[] extractValues(SearchHit hit) {
        String[] extractedValues = new String[organicFeatures.length + processedFeatures.length];
        int i = 0;
        for (String organicFeature : organicFeatures) {
            String extractedValue = extractNonProcessedValues(hit, organicFeature);
            if (extractedValue == null) {
                return null;
            }
            extractedValues[i++] = extractedValue;
        }
        for (ProcessedField processedField : context.extractedFields.getProcessedFields()) {
            String[] processedValues = extractProcessedValue(processedField, hit);
            if (processedValues == null) {
                return null;
            }
            for (String processedValue : processedValues) {
                extractedValues[i++] = processedValue;
            }
        }
        return extractedValues;
    }

    private void markScrollAsErrored() {
        // This could be a transient error with the scroll Id.
        // Reinitialise the scroll and try again but only once.
        hasPreviousSearchFailed = true;
    }

    public List<String> getFieldNames() {
        return Stream.concat(Arrays.stream(organicFeatures), Arrays.stream(processedFeatures)).collect(Collectors.toList());
    }

    public ExtractedFields getExtractedFields() {
        return context.extractedFields;
    }

    public DataSummary collectDataSummary() {
        SearchRequestBuilder searchRequestBuilder = buildDataSummarySearchRequestBuilder();
        SearchResponse searchResponse = executeSearchRequest(searchRequestBuilder);
        long rows = searchResponse.getHits().getTotalHits().value;
        LOGGER.debug(() -> new ParameterizedMessage("[{}] Data summary rows [{}]", context.jobId, rows));
        return new DataSummary(rows, organicFeatures.length + processedFeatures.length);
    }

    public void collectDataSummaryAsync(ActionListener<DataSummary> dataSummaryActionListener) {
        SearchRequestBuilder searchRequestBuilder = buildDataSummarySearchRequestBuilder();
        final int numberOfFields = organicFeatures.length + processedFeatures.length;

        ClientHelper.executeWithHeadersAsync(
            context.headers,
            ClientHelper.ML_ORIGIN,
            client,
            SearchAction.INSTANCE,
            searchRequestBuilder.request(),
            ActionListener.wrap(
                searchResponse -> dataSummaryActionListener.onResponse(
                    new DataSummary(searchResponse.getHits().getTotalHits().value, numberOfFields)
                ),
                dataSummaryActionListener::onFailure
            )
        );
    }

    private SearchRequestBuilder buildDataSummarySearchRequestBuilder() {

        QueryBuilder summaryQuery = context.query;
        if (context.supportsRowsWithMissingValues == false) {
            summaryQuery = QueryBuilders.boolQuery().filter(summaryQuery).filter(allExtractedFieldsExistQuery());
        }

        return new SearchRequestBuilder(client, SearchAction.INSTANCE).setAllowPartialSearchResults(false)
            .setIndices(context.indices)
            .setSize(0)
            .setQuery(summaryQuery)
            .setTrackTotalHits(true)
            .setRuntimeMappings(context.runtimeMappings);
    }

    private QueryBuilder allExtractedFieldsExistQuery() {
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        for (ExtractedField field : context.extractedFields.getAllFields()) {
            query.filter(QueryBuilders.existsQuery(field.getName()));
        }
        return query;
    }

    public Set<String> getCategoricalFields(DataFrameAnalysis analysis) {
        return ExtractedFieldsDetector.getCategoricalOutputFields(context.extractedFields, analysis);
    }

    public static boolean isValidValue(Object value) {
        // We should allow a number, string or a boolean.
        // It is possible for a field to be categorical and have a `keyword` mapping, but be any of these
        // three types, in the same index.
        return value instanceof Number || value instanceof String || value instanceof Boolean;
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

        private final SearchHit hit;

        @Nullable
        private final String[] values;

        private final boolean isTraining;

        private Row(String[] values, SearchHit hit, boolean isTraining) {
            this.values = values;
            this.hit = hit;
            this.isTraining = isTraining;
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

        public boolean isTraining() {
            return isTraining;
        }

        public int getChecksum() {
            return (int) getSortKey();
        }

        public long getSortKey() {
            return (long) hit.getSortValues()[0];
        }
    }
}
