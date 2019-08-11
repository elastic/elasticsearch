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
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;

import java.time.Clock;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

/**
 * {@link DataFrameAnalyticsIndex} class encapsulates logic for creating destination index based on source index metadata.
 */
public final class DataFrameAnalyticsIndex {

    public static final String ID_COPY = "ml__id_copy";

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

    private DataFrameAnalyticsIndex() {}

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

        String[] sourceIndex = config.getSource().getIndex();

        ActionListener<ImmutableOpenMap<String, MappingMetaData>> mappingsListener = ActionListener.wrap(
            mappings -> listener.onResponse(createIndexRequest(clock, config, settingsHolder.get(), mappings)),
            listener::onFailure
        );

        ActionListener<Settings> settingsListener = ActionListener.wrap(
            settings -> {
                settingsHolder.set(settings);
                MappingsMerger.mergeMappings(client, config.getHeaders(), sourceIndex, mappingsListener);
            },
            listener::onFailure
        );

        ActionListener<GetSettingsResponse> getSettingsResponseListener = ActionListener.wrap(
            settingsResponse -> settingsListener.onResponse(settings(settingsResponse)),
            listener::onFailure
        );

        GetSettingsRequest getSettingsRequest = new GetSettingsRequest();
        getSettingsRequest.indices(sourceIndex);
        getSettingsRequest.indicesOptions(IndicesOptions.lenientExpandOpen());
        getSettingsRequest.names(PRESERVED_SETTINGS);
        ClientHelper.executeWithHeadersAsync(config.getHeaders(), ML_ORIGIN, client, GetSettingsAction.INSTANCE,
            getSettingsRequest, getSettingsResponseListener);
    }

    private static CreateIndexRequest createIndexRequest(Clock clock, DataFrameAnalyticsConfig config, Settings settings,
                                                         ImmutableOpenMap<String, MappingMetaData> mappings) {
        // There should only be 1 type
        assert mappings.size() == 1;

        String destinationIndex = config.getDest().getIndex();
        String type = mappings.keysIt().next();
        Map<String, Object> mappingsAsMap = mappings.valuesIt().next().sourceAsMap();
        addProperties(mappingsAsMap);
        addMetaData(mappingsAsMap, config.getId(), clock);
        return new CreateIndexRequest(destinationIndex, settings).mapping(type, mappingsAsMap);
    }

    private static Settings settings(GetSettingsResponse settingsResponse) {
        Integer maxNumberOfShards = findMaxSettingValue(settingsResponse, IndexMetaData.SETTING_NUMBER_OF_SHARDS);
        Integer maxNumberOfReplicas = findMaxSettingValue(settingsResponse, IndexMetaData.SETTING_NUMBER_OF_REPLICAS);

        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), ID_COPY);
        settingsBuilder.put(IndexSortConfig.INDEX_SORT_ORDER_SETTING.getKey(), SortOrder.ASC);
        if (maxNumberOfShards != null) {
            settingsBuilder.put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, maxNumberOfShards);
        }
        if (maxNumberOfReplicas != null) {
            settingsBuilder.put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, maxNumberOfReplicas);
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

    private static void addProperties(Map<String, Object> mappingsAsMap) {
        Map<String, Object> properties = getOrPutDefault(mappingsAsMap, PROPERTIES, HashMap::new);
        properties.put(ID_COPY, Map.of("type", "keyword"));
    }

    private static void addMetaData(Map<String, Object> mappingsAsMap, String analyticsId, Clock clock) {
        Map<String, Object> metadata = getOrPutDefault(mappingsAsMap, META, HashMap::new);
        metadata.put(CREATION_DATE_MILLIS, clock.millis());
        metadata.put(CREATED_BY, "data-frame-analytics");
        metadata.put(VERSION, Map.of(CREATED, Version.CURRENT));
        metadata.put(ANALYTICS, analyticsId);
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

    public static void updateMappingsToDestIndex(Client client, DataFrameAnalyticsConfig analyticsConfig, GetIndexResponse getIndexResponse,
                                                 ActionListener<AcknowledgedResponse> listener) {
        // We have validated the destination index should match a single index
        assert getIndexResponse.indices().length == 1;

        ImmutableOpenMap<String, MappingMetaData> mappings = getIndexResponse.getMappings().get(getIndexResponse.indices()[0]);
        String type = mappings.keysIt().next();

        Map<String, Object> addedMappings = Map.of(PROPERTIES, Map.of(ID_COPY, Map.of("type", "keyword")));

        PutMappingRequest putMappingRequest = new PutMappingRequest(getIndexResponse.indices());
        putMappingRequest.type(type);
        putMappingRequest.source(addedMappings);
        ClientHelper.executeWithHeadersAsync(analyticsConfig.getHeaders(), ML_ORIGIN, client, PutMappingAction.INSTANCE,
            putMappingRequest, listener);
    }
}

