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
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;

import java.time.Clock;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * {@link DataFrameAnalyticsIndex} class encapsulates logic for creating destination index based on source index metadata.
 */
final class DataFrameAnalyticsIndex {

    private static final String PROPERTIES = "properties";
    private static final String META = "_meta";

    /**
     * Unfortunately, getting the settings of an index include internal settings that should
     * not be set explicitly. There is no way to filter those out. Thus, we have to maintain
     * a list of them and filter them out manually.
     */
    private static final List<String> INTERNAL_SETTINGS = Arrays.asList(
        "index.creation_date",
        "index.provided_name",
        "index.uuid",
        "index.version.created",
        "index.version.upgraded"
    );

    /**
     * Creates destination index based on source index metadata.
     */
    public static void createDestinationIndex(Client client,
                                              Clock clock,
                                              ClusterState clusterState,
                                              DataFrameAnalyticsConfig analyticsConfig,
                                              ActionListener<CreateIndexResponse> listener) {
        String sourceIndex = analyticsConfig.getSource().getIndex();
        Map<String, String> headers = analyticsConfig.getHeaders();
        IndexMetaData sourceIndexMetaData = clusterState.getMetaData().getIndices().get(sourceIndex);
        if (sourceIndexMetaData == null) {
            listener.onFailure(new IndexNotFoundException(sourceIndex));
            return;
        }
        CreateIndexRequest createIndexRequest =
            prepareCreateIndexRequest(sourceIndexMetaData, analyticsConfig.getDest().getIndex(), analyticsConfig.getId(), clock);
        ClientHelper.executeWithHeadersAsync(
            headers, ClientHelper.ML_ORIGIN, client, CreateIndexAction.INSTANCE, createIndexRequest, listener);
    }

    private static CreateIndexRequest prepareCreateIndexRequest(IndexMetaData sourceIndexMetaData,
                                                                String destinationIndex,
                                                                String analyticsId,
                                                                Clock clock) {
        // Settings
        Settings.Builder settingsBuilder = Settings.builder().put(sourceIndexMetaData.getSettings());
        INTERNAL_SETTINGS.forEach(settingsBuilder::remove);
        settingsBuilder.put(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), DataFrameAnalyticsFields.ID);
        settingsBuilder.put(IndexSortConfig.INDEX_SORT_ORDER_SETTING.getKey(), SortOrder.ASC);
        Settings settings = settingsBuilder.build();

        // Mappings
        String singleMappingType = sourceIndexMetaData.getMappings().keysIt().next();
        Map<String, Object> mappingsAsMap = sourceIndexMetaData.getMappings().valuesIt().next().sourceAsMap();
        addProperties(mappingsAsMap);
        addMetaData(mappingsAsMap, analyticsId, clock);

        return new CreateIndexRequest(destinationIndex, settings).mapping(singleMappingType, mappingsAsMap);
    }

    private static void addProperties(Map<String, Object> mappingsAsMap) {
        Map<String, Object> properties = getOrPutDefault(mappingsAsMap, PROPERTIES, HashMap::new);
        properties.put(DataFrameAnalyticsFields.ID, Map.of("type", "keyword"));
    }

    private static voidaddMetaData(Map<String, Object> mappingsAsMap, String analyticsId, Clock clock) {
        Map<String, Object> metadata = getOrPutDefault(mappingsAsMap, META, HashMap::new);
        metadata.put(DataFrameAnalyticsFields.CREATION_DATE_MILLIS, clock.millis());
        metadata.put(DataFrameAnalyticsFields.CREATED_BY, "data-frame-analytics");
        metadata.put(DataFrameAnalyticsFields.VERSION, Map.of(DataFrameAnalyticsFields.CREATED, Version.CURRENT));
        metadata.put(DataFrameAnalyticsFields.ANALYTICS, analyticsId);
    }

    private static <K, V> V getOrPutDefault(Map<K, Object> map, K key, Supplier<V> valueSupplier) {
        V value = (V) map.get(key);
        if (value == null) {
            value = valueSupplier.get();
            map.put(key, value);
        }
        return value;
    }

    private DataFrameAnalyticsIndex() {}
}

