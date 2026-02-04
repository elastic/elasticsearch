/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.ccm;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class CCMIndex {

    private CCMIndex() {}

    public static final String INDEX_NAME = ".ccm-inference";
    public static final String INDEX_PATTERN = INDEX_NAME + "*";

    // Increment this version number when the mappings change
    private static final int INDEX_MAPPING_VERSION = 1;

    public static Settings settings() {
        return builder().build();
    }

    // Public to allow tests to create the index with custom settings
    public static Settings.Builder builder() {
        return Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1");
    }

    public static XContentBuilder mappings() {
        try {
            return jsonBuilder().startObject()
                .startObject(SINGLE_MAPPING_NAME)
                .startObject("_meta")
                .field(SystemIndexDescriptor.VERSION_META_KEY, INDEX_MAPPING_VERSION)
                .endObject()
                .field("dynamic", "strict")
                .startObject("properties")
                .startObject("api_key")
                .field("type", "keyword")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to build mappings for index " + INDEX_NAME, e);
        }
    }
}
