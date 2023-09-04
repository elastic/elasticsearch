/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class InferenceIndex {

    private InferenceIndex() {

    }

    public static final String INDEX_NAME = ".inference";
    public static final String INDEX_PATTERN = INDEX_NAME + "*";

    private static final int INDEX_MAPPING_VERSION = 1;

    public static Settings settings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
            .build();
    }

    /**
     * To prevent a mapping explosion dy
     * @return
     */
    public static XContentBuilder mappings() {
        try {
            return jsonBuilder().startObject()
                .startObject(SINGLE_MAPPING_NAME)
                .startObject("_meta")
                .field("version", Version.CURRENT)   // TODO
                .field("managed_index_mappings_version", INDEX_MAPPING_VERSION)
                .endObject()
                .field("dynamic", "strict")
                .startObject("properties")
                .startObject("model_id")
                .field("type", "keyword")
                .endObject()
                .startObject("task_type")
                .field("type", "keyword")
                .endObject()
                .startObject("service")
                .field("type", "keyword")
                .endObject()
                .startObject("service_settings")
                .field("dynamic", "false")
                .startObject("properties")
                .endObject()
                .endObject()
                .startObject("task_settings")
                .field("dynamic", "false")
                .startObject("properties")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to build mappings for index " + INDEX_NAME, e);
        }
    }
}
