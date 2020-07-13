/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsDest;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

/**
 * {@link DestinationIndex} class encapsulates logic for creating destination index based on source index metadata.
 */
public final class DestinationIndex {

    public static final String ID_COPY = "ml__id_copy";

    /**
     * The field that indicates whether a doc was used for training or not
     */
    public static final String IS_TRAINING = "is_training";

    // Metadata fields
    static final String CREATION_DATE_MILLIS = "creation_date_in_millis";
    static final String VERSION = "version";
    static final String CREATED = "created";
    static final String CREATED_BY = "created_by";
    static final String ANALYTICS = "analytics";

    private static final String PROPERTIES = "properties";
    private static final String META = "_meta";

    /**
     * We only preserve the most important settings.
     * If the user needs other settings on the destination index they
     * should create the destination index before starting the analytics.
     */
    private static final String[] PRESERVED_SETTINGS = new String[] {"index.number_of_shards", "index.number_of_replicas"};

    private DestinationIndex() {}

    /**
     * Creates destination index based on source index metadata.
     */
    public static void createDestinationIndex(Client client,
                                              Clock clock,
                                              DataFrameAnalyticsConfig analyticsConfig,
                                              ActionListener<CreateIndexResponse> listener) {
        ActionListener<CreateIndexRequest> createIndexRequestListener = ActionListener.wrap(
            createIndexRequest -> ClientHelper.executeWithHeadersAsync(analyticsConfig.getHeaders(), ClientHelper.ML_ORIGIN, client,
                    CreateIndexAction.INSTANCE, createIndexRequest, listener),
            listener::onFailure
        );

        prepareCreateIndexRequest(client, clock, analyticsConfig, createIndexRequestListener);
    }

    private static void prepareCreateIndexRequest(Client client, Clock clock, DataFrameAnalyticsConfig config,
                                                  ActionListener<CreateIndexRequest> listener) {
        AtomicReference<Settings> settingsHolder = new AtomicReference<>();

        ActionListener<MappingMetadata> mappingsListener = ActionListener.wrap(
            mappings -> listener.onResponse(createIndexRequest(clock, config, settingsHolder.get(), mappings)),
            listener::onFailure
        );

        ActionListener<Settings> settingsListener = ActionListener.wrap(
            settings -> {
                settingsHolder.set(settings);
                MappingsMerger.mergeMappings(client, config.getHeaders(), config.getSource(), mappingsListener);
            },
            listener::onFailure
        );

        ActionListener<GetSettingsResponse> getSettingsResponseListener = ActionListener.wrap(
            settingsResponse -> settingsListener.onResponse(settings(settingsResponse)),
            listener::onFailure
        );

        GetSettingsRequest getSettingsRequest =
            new GetSettingsRequest()
                .indices(config.getSource().getIndex())
                .indicesOptions(IndicesOptions.lenientExpandOpen())
                .names(PRESERVED_SETTINGS);
        ClientHelper.executeWithHeadersAsync(
            config.getHeaders(), ML_ORIGIN, client, GetSettingsAction.INSTANCE, getSettingsRequest, getSettingsResponseListener);
    }

    private static CreateIndexRequest createIndexRequest(Clock clock, DataFrameAnalyticsConfig config, Settings settings,
                                                         MappingMetadata mappings) {
        String destinationIndex = config.getDest().getIndex();
        Map<String, Object> mappingsAsMap = mappings.sourceAsMap();
        Map<String, Object> properties = getOrPutDefault(mappingsAsMap, PROPERTIES, HashMap::new);
        checkResultsFieldIsNotPresentInProperties(config, properties);
        properties.putAll(createAdditionalMappings(config, Collections.unmodifiableMap(properties)));
        Map<String, Object> metadata = getOrPutDefault(mappingsAsMap, META, HashMap::new);
        metadata.putAll(createMetadata(config.getId(), clock));
        return new CreateIndexRequest(destinationIndex, settings).mapping(mappingsAsMap);
    }

    private static Settings settings(GetSettingsResponse settingsResponse) {
        Integer maxNumberOfShards = findMaxSettingValue(settingsResponse, IndexMetadata.SETTING_NUMBER_OF_SHARDS);
        Integer maxNumberOfReplicas = findMaxSettingValue(settingsResponse, IndexMetadata.SETTING_NUMBER_OF_REPLICAS);

        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), ID_COPY);
        settingsBuilder.put(IndexSortConfig.INDEX_SORT_ORDER_SETTING.getKey(), SortOrder.ASC);
        if (maxNumberOfShards != null) {
            settingsBuilder.put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, maxNumberOfShards);
        }
        if (maxNumberOfReplicas != null) {
            settingsBuilder.put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, maxNumberOfReplicas);
        }
        return settingsBuilder.build();
    }

    @Nullable
    private static Integer findMaxSettingValue(GetSettingsResponse settingsResponse, String settingKey) {
        Integer maxValue = null;
        Iterator<Settings> settingsIterator = settingsResponse.getIndexToSettings().valuesIt();
        while (settingsIterator.hasNext()) {
            Settings settings = settingsIterator.next();
            Integer indexValue = settings.getAsInt(settingKey, null);
            if (indexValue != null) {
                maxValue = maxValue == null ? indexValue : Math.max(indexValue, maxValue);
            }
        }
        return maxValue;
    }

    private static Map<String, Object> createAdditionalMappings(DataFrameAnalyticsConfig config, Map<String, Object> mappingsProperties) {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ID_COPY, Map.of("type", KeywordFieldMapper.CONTENT_TYPE));
        properties.putAll(config.getAnalysis().getExplicitlyMappedFields(mappingsProperties, config.getDest().getResultsField()));
        return properties;
    }

    private static Map<String, Object> createMetadata(String analyticsId, Clock clock) {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put(CREATION_DATE_MILLIS, clock.millis());
        metadata.put(CREATED_BY, "data-frame-analytics");
        metadata.put(VERSION, Map.of(CREATED, Version.CURRENT));
        metadata.put(ANALYTICS, analyticsId);
        return metadata;
    }

    @SuppressWarnings("unchecked")
    private static <K, V> V getOrPutDefault(Map<K, Object> map, K key, Supplier<V> valueSupplier) {
        V value = (V) map.get(key);
        if (value == null) {
            value = valueSupplier.get();
            map.put(key, value);
        }
        return value;
    }

    @SuppressWarnings("unchecked")
    public static void updateMappingsToDestIndex(Client client, DataFrameAnalyticsConfig config, GetIndexResponse getIndexResponse,
                                                 ActionListener<AcknowledgedResponse> listener) {
        // We have validated the destination index should match a single index
        assert getIndexResponse.indices().length == 1;

        // Fetch mappings from destination index
        Map<String, Object> destMappingsAsMap = getIndexResponse.mappings().valuesIt().next().sourceAsMap();
        Map<String, Object> destPropertiesAsMap =
            (Map<String, Object>)destMappingsAsMap.getOrDefault(PROPERTIES, Collections.emptyMap());

        // Verify that the results field does not exist in the dest index
        checkResultsFieldIsNotPresentInProperties(config, destPropertiesAsMap);

        // Determine mappings to be added to the destination index
        Map<String, Object> addedMappings =
            Map.of(PROPERTIES, createAdditionalMappings(config, Collections.unmodifiableMap(destPropertiesAsMap)));

        // Add the mappings to the destination index
        PutMappingRequest putMappingRequest =
            new PutMappingRequest(getIndexResponse.indices())
                .source(addedMappings);
        ClientHelper.executeWithHeadersAsync(
            config.getHeaders(), ML_ORIGIN, client, PutMappingAction.INSTANCE, putMappingRequest, listener);
    }

    private static void checkResultsFieldIsNotPresentInProperties(DataFrameAnalyticsConfig config, Map<String, Object> properties) {
        String resultsField = config.getDest().getResultsField();
        if (properties.containsKey(resultsField)) {
            throw ExceptionsHelper.badRequestException(
                "A field that matches the {}.{} [{}] already exists; please set a different {}",
                DataFrameAnalyticsConfig.DEST.getPreferredName(),
                DataFrameAnalyticsDest.RESULTS_FIELD.getPreferredName(),
                resultsField,
                DataFrameAnalyticsDest.RESULTS_FIELD.getPreferredName());
        }
    }
}
