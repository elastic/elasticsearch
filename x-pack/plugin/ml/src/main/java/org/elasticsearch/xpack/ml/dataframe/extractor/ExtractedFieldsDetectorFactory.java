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
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.metrics.Cardinality;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A factory that retrieves all the parts necessary to build a {@link ExtractedFieldsDetector}.
 */
public class ExtractedFieldsDetectorFactory {

    private final Client client;

    public ExtractedFieldsDetectorFactory(Client client) {
        this.client = Objects.requireNonNull(client);
    }

    public void createFromSource(DataFrameAnalyticsConfig config, ActionListener<ExtractedFieldsDetector> listener) {
        create(config.getSource().getIndex(), config, listener);
    }

    public void createFromDest(DataFrameAnalyticsConfig config, ActionListener<ExtractedFieldsDetector> listener) {
        create(new String[] {config.getDest().getIndex()}, config, listener);
    }

    private void create(String[] index, DataFrameAnalyticsConfig config, ActionListener<ExtractedFieldsDetector> listener) {
        AtomicInteger docValueFieldsLimitHolder = new AtomicInteger();
        AtomicReference<FieldCapabilitiesResponse> fieldCapsResponseHolder = new AtomicReference<>();

        // Step 4. Create cardinality by field map and build detector
        ActionListener<Map<String, Long>> fieldCardinalitiesHandler = ActionListener.wrap(
            fieldCardinalities -> {
                ExtractedFieldsDetector detector =
                    new ExtractedFieldsDetector(
                        index, config, docValueFieldsLimitHolder.get(), fieldCapsResponseHolder.get(), fieldCardinalities);
                listener.onResponse(detector);
            },
            listener::onFailure
        );

        // Step 3. Get cardinalities for fields with limits
        ActionListener<FieldCapabilitiesResponse> fieldCapabilitiesHandler = ActionListener.wrap(
            fieldCapabilitiesResponse -> {
                fieldCapsResponseHolder.set(fieldCapabilitiesResponse);
                getCardinalitiesForFieldsWithLimit(index, config, fieldCardinalitiesHandler);
            },
            listener::onFailure
        );

        // Step 2. Get field capabilities necessary to build the information of how to extract fields
        ActionListener<Integer> docValueFieldsLimitListener = ActionListener.wrap(
            docValueFieldsLimit -> {
                docValueFieldsLimitHolder.set(docValueFieldsLimit);
                getFieldCaps(index, config, fieldCapabilitiesHandler);
            },
            listener::onFailure
        );

        // Step 1. Get doc value fields limit
        getDocValueFieldsLimit(index, docValueFieldsLimitListener);
    }

    private void getCardinalitiesForFieldsWithLimit(String[] index, DataFrameAnalyticsConfig config,
                                                    ActionListener<Map<String, Long>> listener) {
        Map<String, Long> fieldCardinalityLimits = config.getAnalysis().getFieldCardinalityLimits();
        if (fieldCardinalityLimits.isEmpty()) {
            listener.onResponse(Collections.emptyMap());
            return;
        }

        ActionListener<SearchResponse> searchListener = ActionListener.wrap(
            searchResponse -> buildFieldCardinalitiesMap(config, searchResponse, listener),
            listener::onFailure
        );

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().size(0).query(config.getSource().getParsedQuery());
        for (Map.Entry<String, Long> entry : fieldCardinalityLimits.entrySet()) {
            String fieldName = entry.getKey();
            Long limit = entry.getValue();
            searchSourceBuilder.aggregation(
                AggregationBuilders.cardinality(fieldName)
                    .field(fieldName)
                    .precisionThreshold(limit + 1));
        }
        SearchRequest searchRequest = new SearchRequest(index).source(searchSourceBuilder);
        ClientHelper.executeWithHeadersAsync(
            config.getHeaders(), ClientHelper.ML_ORIGIN, client, SearchAction.INSTANCE, searchRequest, searchListener);
    }

    private void buildFieldCardinalitiesMap(DataFrameAnalyticsConfig config, SearchResponse searchResponse,
                                            ActionListener<Map<String, Long>> listener) {
        Aggregations aggs = searchResponse.getAggregations();
        if (aggs == null) {
            listener.onFailure(ExceptionsHelper.serverError("Unexpected null response when gathering field cardinalities"));
            return;
        }

        Map<String, Long> fieldCardinalities = new HashMap<>(config.getAnalysis().getFieldCardinalityLimits().size());
        for (String field : config.getAnalysis().getFieldCardinalityLimits().keySet()) {
            Cardinality cardinality = aggs.get(field);
            if (cardinality == null) {
                listener.onFailure(ExceptionsHelper.serverError("Unexpected null response when gathering field cardinalities"));
                return;
            }
            fieldCardinalities.put(field, cardinality.getValue());
        }
        listener.onResponse(fieldCardinalities);
    }

    private void getFieldCaps(String[] index, DataFrameAnalyticsConfig config, ActionListener<FieldCapabilitiesResponse> listener) {
        FieldCapabilitiesRequest fieldCapabilitiesRequest = new FieldCapabilitiesRequest();
        fieldCapabilitiesRequest.indices(index);
        fieldCapabilitiesRequest.indicesOptions(IndicesOptions.lenientExpandOpen());
        fieldCapabilitiesRequest.fields("*");
        ClientHelper.executeWithHeaders(config.getHeaders(), ClientHelper.ML_ORIGIN, client, () -> {
            client.execute(FieldCapabilitiesAction.INSTANCE, fieldCapabilitiesRequest, listener);
            // This response gets discarded - the listener handles the real response
            return null;
        });
    }

    private void getDocValueFieldsLimit(String[] index, ActionListener<Integer> docValueFieldsLimitListener) {
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
                Throwable cause = ExceptionsHelper.unwrapCause(e);
                if (cause instanceof IndexNotFoundException) {
                    docValueFieldsLimitListener.onFailure(new ResourceNotFoundException("cannot retrieve data because index "
                        + ((IndexNotFoundException) cause).getIndex() + " does not exist"));
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
