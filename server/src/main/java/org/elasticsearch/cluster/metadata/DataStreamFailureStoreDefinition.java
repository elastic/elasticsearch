/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.RoutingFieldMapper;

import java.io.IOException;

/**
 * A utility class that contains the mappings and settings logic for failure store indices that are a part of data streams.
 */
public class DataStreamFailureStoreDefinition {

    public static final String FAILURE_STORE_REFRESH_INTERVAL_SETTING_NAME = "data_streams.failure_store.refresh_interval";
    public static final CompressedXContent DATA_STREAM_FAILURE_STORE_MAPPING;

    static {
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
             *              "type": "wildcard"
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
             *           "processor": {
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
                    .field("type", "wildcard")
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
                    .startObject("processor")
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
     * Like {@link DataStreamFailureStoreDefinition#applyFailureStoreSettings} but optionally applied on an existing {@link Settings}
     * @param existingSettings initial settings to update
     * @param nodeSettings settings from the cluster service which capture the node's current settings
     * @return either the existing settings if no changes are needed, or a new settings instance which includes failure store specific
     * settings
     */
    public static Settings buildFailureStoreIndexSettings(Settings existingSettings, Settings nodeSettings) {
        // Optionally set a custom refresh interval for the failure store index.
        TimeValue refreshInterval = getFailureStoreRefreshInterval(nodeSettings);
        if (refreshInterval != null) {
            return Settings.builder()
                .put(existingSettings)
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), refreshInterval)
                .build();
        }
        return existingSettings;
    }

    /**
     * Like {@link DataStreamFailureStoreDefinition#buildFailureStoreIndexSettings} but for usage with a {@link Settings.Builder}
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
}
