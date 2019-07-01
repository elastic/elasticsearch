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
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.ml.datafeed.extractor.fields.ExtractedFields;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class DataFrameDataExtractorFactory {

    private final Client client;
    private final String analyticsId;
    private final String index;
    private final ExtractedFields extractedFields;
    private final Map<String, String> headers;

    private DataFrameDataExtractorFactory(Client client, String analyticsId, String index, ExtractedFields extractedFields,
                                          Map<String, String> headers) {
        this.client = Objects.requireNonNull(client);
        this.analyticsId = Objects.requireNonNull(analyticsId);
        this.index = Objects.requireNonNull(index);
        this.extractedFields = Objects.requireNonNull(extractedFields);
        this.headers = headers;
    }

    public DataFrameDataExtractor newExtractor(boolean includeSource) {
        DataFrameDataExtractorContext context = new DataFrameDataExtractorContext(
                analyticsId,
                extractedFields,
                Arrays.asList(index),
                QueryBuilders.matchAllQuery(),
                1000,
                headers,
                includeSource
            );
        return new DataFrameDataExtractor(client, context);
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
    public static void create(Client client,
                              DataFrameAnalyticsConfig config,
                              boolean isTaskRestarting,
                              ActionListener<DataFrameDataExtractorFactory> listener) {
        validateIndexAndExtractFields(client, new String[] {config.getDest().getIndex()}, config, isTaskRestarting,
            ActionListener.wrap(extractedFields -> listener.onResponse(new DataFrameDataExtractorFactory(
                    client, config.getId(), config.getDest().getIndex(), extractedFields, config.getHeaders())),
                listener::onFailure
        ));
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
        validateIndexAndExtractFields(client, config.getSource().getIndex(), config, false, ActionListener.wrap(
                fields -> {
                    config.getSource().getParsedQuery(); // validate query is acceptable
                    listener.onResponse(config);
                },
                listener::onFailure
        ));
    }

    private static void validateIndexAndExtractFields(Client client,
                                                      String[] index,
                                                      DataFrameAnalyticsConfig config,
                                                      boolean isTaskRestarting,
                                                      ActionListener<ExtractedFields> listener) {
        AtomicInteger docValueFieldsLimitHolder = new AtomicInteger();

        // Step 3. Extract fields (if possible) and notify listener
        ActionListener<FieldCapabilitiesResponse> fieldCapabilitiesHandler = ActionListener.wrap(
            fieldCapabilitiesResponse -> listener.onResponse(new ExtractedFieldsDetector(index, config, isTaskRestarting,
                docValueFieldsLimitHolder.get(), fieldCapabilitiesResponse).detect()),
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
                if (e instanceof IndexNotFoundException) {
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
