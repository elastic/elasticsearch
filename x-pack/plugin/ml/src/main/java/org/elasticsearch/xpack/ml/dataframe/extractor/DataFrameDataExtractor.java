/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.dataframe.extractor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
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
import org.elasticsearch.xpack.ml.extractor.SourceSupplier;

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

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.ml.dataframe.DestinationIndex.INCREMENTAL_ID;

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
        this.trainTestSplitter = CachedSupplier.wrap(context.trainTestSplitterFactory::create);
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
        LOGGER.debug(() -> "[" + context.jobId + "] Data extractor was cancelled");
        isCancelled = true;
    }

    public Optional<SearchHit[]> next() throws IOException {
        if (hasNext() == false) {
            throw new NoSuchElementException();
        }

        Optional<SearchHit[]> hits = Optional.ofNullable(nextSearch());
        if (hits.isPresent() && hits.get().length > 0) {
            lastSortKey = (long) hits.get()[hits.get().length - 1].getSortValues()[0];
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
    public void preview(ActionListener<List<String[]>> listener) {

        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client)
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
            TransportSearchAction.TYPE,
            searchRequestBuilder.request(),
            listener.delegateFailureAndWrap((delegate, searchResponse) -> {
                if (searchResponse.getHits().getHits().length == 0) {
                    delegate.onResponse(Collections.emptyList());
                    return;
                }

                List<String[]> rows = new ArrayList<>(searchResponse.getHits().getHits().length);
                for (SearchHit hit : searchResponse.getHits().getHits()) {
                    String[] extractedValues = extractValues(hit, new SourceSupplier(hit));
                    rows.add(extractedValues);
                }
                delegate.onResponse(rows);
            })
        );
    }

    protected SearchHit[] nextSearch() throws IOException {
        if (isCancelled) {
            return null;
        }
        return tryRequestWithSearchResponse(() -> executeSearchRequest(buildSearchRequest()));
    }

    private SearchHit[] tryRequestWithSearchResponse(Supplier<SearchResponse> request) throws IOException {
        try {

            // We've set allow_partial_search_results to false which means if something
            // goes wrong the request will throw.
            SearchResponse searchResponse = request.get();
            try {
                LOGGER.trace(() -> "[" + context.jobId + "] Search response was obtained");

                SearchHit[] rows = processSearchResponse(searchResponse);

                // Request was successfully executed and processed so we can restore the flag to retry if a future failure occurs
                hasPreviousSearchFailed = false;

                return rows;
            } finally {
                searchResponse.decRef();
            }
        } catch (Exception e) {
            if (hasPreviousSearchFailed) {
                throw e;
            }
            LOGGER.warn(() -> "[" + context.jobId + "] Search resulted to failure; retrying once", e);
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

        LOGGER.trace(() -> format("[%s] Searching docs with [%s] in [%s, %s)", context.jobId, INCREMENTAL_ID, from, to));

        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client)
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

    private SearchHit[] processSearchResponse(SearchResponse searchResponse) {
        if (isCancelled || searchResponse.getHits().getHits().length == 0) {
            hasNext = false;
            return null;
        }
        return searchResponse.getHits().asUnpooled().getHits();
    }

    private String extractNonProcessedValues(SearchHit hit, SourceSupplier sourceSupplier, String organicFeature) {
        ExtractedField field = extractedFieldsByName.get(organicFeature);
        Object[] values = field.value(hit, sourceSupplier);
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

    private String[] extractProcessedValue(ProcessedField processedField, SearchHit hit, SourceSupplier sourceSupplier) {
        Object[] values = processedField.value(hit, sourceSupplier, extractedFieldsByName::get);
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

    public Row createRow(SearchHit hit) {
        SourceSupplier sourceSupplier = new SourceSupplier(hit);
        String[] extractedValues = extractValues(hit, sourceSupplier);
        if (extractedValues == null) {
            return new Row(null, hit, sourceSupplier, true);
        }
        boolean isTraining = trainTestSplitter.get().isTraining(extractedValues);
        Row row = new Row(extractedValues, hit, sourceSupplier, isTraining);
        LOGGER.trace(
            () -> format(
                "[%s] Extracted row: sort key = [%s], is_training = [%s], values = %s",
                context.jobId,
                row.getSortKey(),
                isTraining,
                Arrays.toString(row.values)
            )
        );
        return row;
    }

    private String[] extractValues(SearchHit hit, SourceSupplier sourceSupplier) {
        String[] extractedValues = new String[organicFeatures.length + processedFeatures.length];
        int i = 0;
        for (String organicFeature : organicFeatures) {
            String extractedValue = extractNonProcessedValues(hit, sourceSupplier, organicFeature);
            if (extractedValue == null) {
                return null;
            }
            extractedValues[i++] = extractedValue;
        }
        for (ProcessedField processedField : context.extractedFields.getProcessedFields()) {
            String[] processedValues = extractProcessedValue(processedField, hit, sourceSupplier);
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
        try {
            long rows = searchResponse.getHits().getTotalHits().value();
            LOGGER.debug(() -> format("[%s] Data summary rows [%s]", context.jobId, rows));
            return new DataSummary(rows, organicFeatures.length + processedFeatures.length);
        } finally {
            searchResponse.decRef();
        }
    }

    public void collectDataSummaryAsync(ActionListener<DataSummary> dataSummaryActionListener) {
        SearchRequestBuilder searchRequestBuilder = buildDataSummarySearchRequestBuilder();
        final int numberOfFields = organicFeatures.length + processedFeatures.length;

        ClientHelper.executeWithHeadersAsync(
            context.headers,
            ClientHelper.ML_ORIGIN,
            client,
            TransportSearchAction.TYPE,
            searchRequestBuilder.request(),
            dataSummaryActionListener.delegateFailureAndWrap(
                (l, searchResponse) -> l.onResponse(new DataSummary(searchResponse.getHits().getTotalHits().value(), numberOfFields))
            )
        );
    }

    private SearchRequestBuilder buildDataSummarySearchRequestBuilder() {

        QueryBuilder summaryQuery = context.query;
        if (context.supportsRowsWithMissingValues == false) {
            summaryQuery = QueryBuilders.boolQuery().filter(summaryQuery).filter(allExtractedFieldsExistQuery());
        }

        return new SearchRequestBuilder(client).setAllowPartialSearchResults(false)
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

        private final SourceSupplier sourceSupplier;

        private Row(String[] values, SearchHit hit, SourceSupplier sourceSupplier, boolean isTraining) {
            this.values = values;
            this.hit = hit;
            this.sourceSupplier = sourceSupplier;
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

        public Map<String, Object> getSource() {
            return sourceSupplier.get();
        }
    }
}
