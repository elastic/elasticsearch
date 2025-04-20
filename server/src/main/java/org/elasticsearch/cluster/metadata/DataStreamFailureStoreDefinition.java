/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.RoutingFieldMapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * A utility class that contains the mappings and settings logic for failure store indices that are a part of data streams.
 */
public class DataStreamFailureStoreDefinition {

    public static final String FAILURE_STORE_REFRESH_INTERVAL_SETTING_NAME = "data_streams.failure_store.refresh_interval";
    public static final String INDEX_FAILURE_STORE_VERSION_SETTING_NAME = "index.failure_store.version";
    public static final Settings DATA_STREAM_FAILURE_STORE_SETTINGS;
    // Only a subset of user configurable settings is applicable for a failure index. Here we have an
    // allowlist that will filter all other settings out.
    public static final Set<String> SUPPORTED_USER_SETTINGS = Set.of(
        DataTier.TIER_PREFERENCE,
        IndexMetadata.SETTING_INDEX_HIDDEN,
        INDEX_FAILURE_STORE_VERSION_SETTING_NAME,
        IndexMetadata.SETTING_NUMBER_OF_SHARDS,
        IndexMetadata.SETTING_NUMBER_OF_REPLICAS,
        IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS,
        IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(),
        IndexMetadata.LIFECYCLE_NAME
    );
    public static final Set<String> SUPPORTED_USER_SETTINGS_PREFIXES = Set.of(
        IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + ".",
        IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX + ".",
        IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "."
    );
    public static final CompressedXContent DATA_STREAM_FAILURE_STORE_MAPPING;

    public static final int FAILURE_STORE_DEFINITION_VERSION = 1;
    public static final Setting<Integer> FAILURE_STORE_DEFINITION_VERSION_SETTING = Setting.intSetting(
        INDEX_FAILURE_STORE_VERSION_SETTING_NAME,
        0,
        Setting.Property.IndexScope
    );

    static {
        DATA_STREAM_FAILURE_STORE_SETTINGS = Settings.builder()
            // Always start with the hidden settings for a backing index.
            .put(IndexMetadata.SETTING_INDEX_HIDDEN, true)
            .put(FAILURE_STORE_DEFINITION_VERSION_SETTING.getKey(), FAILURE_STORE_DEFINITION_VERSION)
            .build();

        try {
            /*
             * The data stream failure store mapping. The JSON content is as follows:
             * {
             *   "_doc": {
             *     "dynamic": false,
             *     "_routing": {
             *       "required": false
             *     },
             *     "properties": {
             *       "@timestamp": {
             *         "type": "date",
             *         "ignore_malformed": false
             *       },
             *       "document": {
             *         "properties": {
             *           "id": {
             *             "type": "keyword"
             *           },
             *           "routing": {
             *             "type": "keyword"
             *           },
             *           "index": {
             *             "type": "keyword"
             *           }
             *         }
             *       },
             *       "error": {
             *         "properties": {
             *           "message": {
             *              "type": "match_only_text"
             *           },
             *           "stack_trace": {
             *              "type": "text"
             *           },
             *           "type": {
             *              "type": "keyword"
             *           },
             *           "pipeline": {
             *              "type": "keyword"
             *           },
             *           "pipeline_trace": {
             *              "type": "keyword"
             *           },
             *           "processor_tag": {
             *              "type": "keyword"
             *           },
             *           "processor_type": {
             *              "type": "keyword"
             *           }
             *         }
             *       }
             *     }
             *   }
             * }
             */
            DATA_STREAM_FAILURE_STORE_MAPPING = new CompressedXContent(
                (builder, params) -> builder.startObject(MapperService.SINGLE_MAPPING_NAME)
                    .field("dynamic", false)
                    .startObject(RoutingFieldMapper.NAME)
                    .field("required", false)
                    .endObject()
                    .startObject("properties")
                    .startObject(MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD)
                    .field("type", DateFieldMapper.CONTENT_TYPE)
                    .field("ignore_malformed", false)
                    .endObject()
                    .startObject("document")
                    .startObject("properties")
                    // document.source is unmapped so that it can be persisted in source only without worrying that the document might cause
                    // a mapping error
                    .startObject("id")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("routing")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("index")
                    .field("type", "keyword")
                    .endObject()
                    .endObject()
                    .endObject()
                    .startObject("error")
                    .startObject("properties")
                    .startObject("message")
                    .field("type", "match_only_text")
                    .endObject()
                    .startObject("stack_trace")
                    .field("type", "text")
                    .endObject()
                    .startObject("type")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("pipeline")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("pipeline_trace")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("processor_tag")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("processor_type")
                    .field("type", "keyword")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            );
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    public static TimeValue getFailureStoreRefreshInterval(Settings settings) {
        return settings.getAsTime(FAILURE_STORE_REFRESH_INTERVAL_SETTING_NAME, null);
    }

    /**
     * Obtains the default settings to be used when creating a failure store index on a data stream.
     * @param nodeSettings settings from the cluster service which capture the node's current settings
     * @return either the existing settings if no changes are needed, or a new settings instance which includes failure store specific
     * settings
     */
    public static Settings buildFailureStoreIndexSettings(Settings nodeSettings) {
        // Optionally set a custom refresh interval for the failure store index.
        TimeValue refreshInterval = getFailureStoreRefreshInterval(nodeSettings);
        if (refreshInterval != null) {
            return Settings.builder()
                .put(DATA_STREAM_FAILURE_STORE_SETTINGS)
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), refreshInterval)
                .build();
        }
        return DATA_STREAM_FAILURE_STORE_SETTINGS;
    }

    /**
     * Modifies an existing index's settings so that it can be added to a data stream's failure store.
     * @param nodeSettings settings from the cluster service which capture the node's current settings
     * @param builder to capture failure store specific index settings
     * @return the original settings builder, with any failure store specific settings applied
     */
    public static Settings.Builder applyFailureStoreSettings(Settings nodeSettings, Settings.Builder builder) {
        // Optionally set a custom refresh interval for the failure store index.
        TimeValue refreshInterval = getFailureStoreRefreshInterval(nodeSettings);
        if (refreshInterval != null) {
            builder.put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), refreshInterval);
        }
        return builder;
    }

    /**
     * Removes the unsupported by the failure store settings from the settings provided.
     * ATTENTION: This method should be applied BEFORE we set the necessary settings for an index
     * @param builder the settings builder that is going to be updated
     * @return the original settings builder, with the unsupported settings removed.
     */
    public static Settings.Builder filterUserDefinedSettings(Settings.Builder builder) {
        if (builder.keys().isEmpty() == false) {
            Set<String> existingKeys = new HashSet<>(builder.keys());
            for (String setting : existingKeys) {
                if (SUPPORTED_USER_SETTINGS.contains(setting) == false
                    && SUPPORTED_USER_SETTINGS_PREFIXES.stream().anyMatch(setting::startsWith) == false) {
                    builder.remove(setting);
                }
            }
        }
        return builder;
    }
}
