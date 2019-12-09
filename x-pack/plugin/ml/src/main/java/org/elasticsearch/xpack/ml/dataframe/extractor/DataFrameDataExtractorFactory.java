/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.extractor;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesAction;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.metrics.Cardinality;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.extractor.ExtractedField;
import org.elasticsearch.xpack.ml.extractor.ExtractedFields;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class DataFrameDataExtractorFactory {

    private final Client client;
    private final String analyticsId;
    private final List<String> indices;
    private final QueryBuilder sourceQuery;
    private final ExtractedFields extractedFields;
    private final Map<String, String> headers;
    private final boolean includeRowsWithMissingValues;

    private DataFrameDataExtractorFactory(Client client, String analyticsId, List<String> indices, QueryBuilder sourceQuery,
                                         ExtractedFields extractedFields, Map<String, String> headers,
                                         boolean includeRowsWithMissingValues) {
        this.client = Objects.requireNonNull(client);
        this.analyticsId = Objects.requireNonNull(analyticsId);
        this.indices = Objects.requireNonNull(indices);
        this.sourceQuery = Objects.requireNonNull(sourceQuery);
        this.extractedFields = Objects.requireNonNull(extractedFields);
        this.headers = headers;
        this.includeRowsWithMissingValues = includeRowsWithMissingValues;
    }

    public DataFrameDataExtractor newExtractor(boolean includeSource) {
        DataFrameDataExtractorContext context = new DataFrameDataExtractorContext(
                analyticsId,
                extractedFields,
                indices,
                createQuery(),
                1000,
                headers,
                includeSource,
                includeRowsWithMissingValues
            );
        return new DataFrameDataExtractor(client, context);
    }

    private QueryBuilder createQuery() {
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        query.filter(sourceQuery);
        if (includeRowsWithMissingValues == false) {
            query.filter(allExtractedFieldsExistQuery());
        }
        return query;
    }

    private QueryBuilder allExtractedFieldsExistQuery() {
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        for (ExtractedField field : extractedFields.getAllFields()) {
            query.filter(QueryBuilders.existsQuery(field.getName()));
        }
        return query;
    }

    /**
     * Validate and create a new extractor factory
     *
     * The source index must exist and contain at least 1 compatible field or validations will fail.
     *
     * @param client ES Client used to make calls against the cluster
     * @param config The config from which to create the extractor factory
     * @param listener The listener to notify on creation or failure
     */
    public static void createForSourceIndices(Client client,
                                              String taskId,
                                              DataFrameAnalyticsConfig config,
                                              ActionListener<DataFrameDataExtractorFactory> listener) {
        validateIndexAndExtractFields(
            client,
            config.getSource().getIndex(),
            config,
            null,
            false,
            ActionListener.wrap(
                extractedFields -> listener.onResponse(
                    new DataFrameDataExtractorFactory(
                        client, taskId, Arrays.asList(config.getSource().getIndex()), config.getSource().getParsedQuery(), extractedFields,
                        config.getHeaders(), config.getAnalysis().supportsMissingValues())),
                listener::onFailure
            )
        );
    }

    /**
     * Validate and create a new extractor factory
     *
     * The destination index must exist and contain at least 1 compatible field or validations will fail.
     *
     * @param client ES Client used to make calls against the cluster
     * @param config The config from which to create the extractor factory
     * @param isTaskRestarting Whether the task is restarting
     * @param listener The listener to notify on creation or failure
     */
    public static void createForDestinationIndex(Client client,
                                                 DataFrameAnalyticsConfig config,
                                                 boolean isTaskRestarting,
                                                 ActionListener<DataFrameDataExtractorFactory> listener) {
        validateIndexAndExtractFields(
            client,
            new String[] {config.getDest().getIndex()},
            config,
            config.getDest().getResultsField(),
            isTaskRestarting,
            ActionListener.wrap(
                extractedFields -> listener.onResponse(
                    new DataFrameDataExtractorFactory(
                        client, config.getId(), Arrays.asList(config.getDest().getIndex()), config.getSource().getParsedQuery(),
                        extractedFields, config.getHeaders(), config.getAnalysis().supportsMissingValues())),
                listener::onFailure
            )
        );
    }

    /**
     * Validates the source index and analytics config
     *
     * @param client ES Client to make calls
     * @param config Analytics config to validate
     * @param listener The listener to notify on failure or completion
     */
    public static void validateConfigAndSourceIndex(Client client,
                                                    DataFrameAnalyticsConfig config,
                                                    ActionListener<DataFrameAnalyticsConfig> listener) {
        validateIndexAndExtractFields(
            client,
            config.getSource().getIndex(),
            config,
            config.getDest().getResultsField(),
            false,
            ActionListener.wrap(
                fields -> {
                    config.getSource().getParsedQuery(); // validate query is acceptable
                    listener.onResponse(config);
                },
                listener::onFailure
            )
        );
    }

    private static void validateIndexAndExtractFields(Client client,
                                                      String[] index,
                                                      DataFrameAnalyticsConfig config,
                                                      String resultsField,
                                                      boolean isTaskRestarting,
                                                      ActionListener<ExtractedFields> listener) {
        AtomicInteger docValueFieldsLimitHolder = new AtomicInteger();
        AtomicReference<ExtractedFields> extractedFieldsHolder = new AtomicReference<>();

        // Step 4. Check fields cardinality vs limits and notify listener
        ActionListener<SearchResponse> checkCardinalityHandler = ActionListener.wrap(
            searchResponse -> {
                if (searchResponse != null) {
                    Aggregations aggs = searchResponse.getAggregations();
                    if (aggs == null) {
                        listener.onFailure(ExceptionsHelper.serverError("Unexpected null response when gathering field cardinalities"));
                        return;
                    }
                    for (Map.Entry<String, Long> entry : config.getAnalysis().getFieldCardinalityLimits().entrySet()) {
                        String fieldName = entry.getKey();
                        Long limit = entry.getValue();
                        Cardinality cardinality = aggs.get(fieldName);
                        if (cardinality == null) {
                            listener.onFailure(ExceptionsHelper.serverError("Unexpected null response when gathering field cardinalities"));
                            return;
                        }
                        if (cardinality.getValue() > limit) {
                            listener.onFailure(
                                ExceptionsHelper.badRequestException(
                                    "Field [{}] must have at most [{}] distinct values but there were at least [{}]",
                                    fieldName, limit, cardinality.getValue()));
                            return;
                        }
                    }
                }
                listener.onResponse(extractedFieldsHolder.get());
            },
            listener::onFailure
        );

        // Step 3. Extract fields (if possible)
        ActionListener<FieldCapabilitiesResponse> fieldCapabilitiesHandler = ActionListener.wrap(
            fieldCapabilitiesResponse -> {
                extractedFieldsHolder.set(
                    new ExtractedFieldsDetector(
                            index, config, resultsField, isTaskRestarting, docValueFieldsLimitHolder.get(), fieldCapabilitiesResponse)
                        .detect());

                Map<String, Long> fieldCardinalityLimits = config.getAnalysis().getFieldCardinalityLimits();
                if (fieldCardinalityLimits.isEmpty()) {
                    checkCardinalityHandler.onResponse(null);
                } else {
                    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().size(0).query(config.getSource().getParsedQuery());
                    for (Map.Entry<String, Long> entry : fieldCardinalityLimits.entrySet()) {
                        String fieldName = entry.getKey();
                        Long limit = entry.getValue();
                        searchSourceBuilder.aggregation(
                            AggregationBuilders.cardinality(fieldName)
                                .field(fieldName)
                                .precisionThreshold(limit + 1));
                    }
                    SearchRequest searchRequest = new SearchRequest(config.getSource().getIndex()).source(searchSourceBuilder);
                    ClientHelper.executeWithHeadersAsync(
                        config.getHeaders(), ClientHelper.ML_ORIGIN, client, SearchAction.INSTANCE, searchRequest, checkCardinalityHandler);
                }
            },
            listener::onFailure
        );

        // Step 2. Get field capabilities necessary to build the information of how to extract fields
        ActionListener<Integer> docValueFieldsLimitListener = ActionListener.wrap(
            docValueFieldsLimit -> {
                docValueFieldsLimitHolder.set(docValueFieldsLimit);

                FieldCapabilitiesRequest fieldCapabilitiesRequest = new FieldCapabilitiesRequest();
                fieldCapabilitiesRequest.indices(index);
                fieldCapabilitiesRequest.indicesOptions(IndicesOptions.lenientExpandOpen());
                fieldCapabilitiesRequest.fields("*");
                ClientHelper.executeWithHeaders(config.getHeaders(), ClientHelper.ML_ORIGIN, client, () -> {
                    client.execute(FieldCapabilitiesAction.INSTANCE, fieldCapabilitiesRequest, fieldCapabilitiesHandler);
                    // This response gets discarded - the listener handles the real response
                    return null;
                });
            },
            listener::onFailure
        );

        // Step 1. Get doc value fields limit
        getDocValueFieldsLimit(client, index, docValueFieldsLimitListener);
    }

    private static void getDocValueFieldsLimit(Client client, String[] index, ActionListener<Integer> docValueFieldsLimitListener) {
        ActionListener<GetSettingsResponse> settingsListener = ActionListener.wrap(getSettingsResponse -> {
                Integer minDocValueFieldsLimit = Integer.MAX_VALUE;

                ImmutableOpenMap<String, Settings> indexToSettings = getSettingsResponse.getIndexToSettings();
                Iterator<ObjectObjectCursor<String, Settings>> iterator = indexToSettings.iterator();
                while (iterator.hasNext()) {
                    ObjectObjectCursor<String, Settings> indexSettings = iterator.next();
                    Integer indexMaxDocValueFields = IndexSettings.MAX_DOCVALUE_FIELDS_SEARCH_SETTING.get(indexSettings.value);
                    if (indexMaxDocValueFields < minDocValueFieldsLimit) {
                        minDocValueFieldsLimit = indexMaxDocValueFields;
                    }
                }
                docValueFieldsLimitListener.onResponse(minDocValueFieldsLimit);
            },
            e -> {
                if (ExceptionsHelper.unwrapCause(e) instanceof IndexNotFoundException) {
                    docValueFieldsLimitListener.onFailure(new ResourceNotFoundException("cannot retrieve data because index "
                        + ((IndexNotFoundException) e).getIndex() + " does not exist"));
                } else {
                    docValueFieldsLimitListener.onFailure(e);
                }
            }
        );

        GetSettingsRequest getSettingsRequest = new GetSettingsRequest();
        getSettingsRequest.indices(index);
        getSettingsRequest.includeDefaults(true);
        getSettingsRequest.names(IndexSettings.MAX_DOCVALUE_FIELDS_SEARCH_SETTING.getKey());
        client.admin().indices().getSettings(getSettingsRequest, settingsListener);
    }
}
