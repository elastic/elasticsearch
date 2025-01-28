/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class InferenceIndex {

    private InferenceIndex() {}

    public static final String INDEX_NAME = ".inference";
    public static final String INDEX_PATTERN = INDEX_NAME + "*";
    public static final String INDEX_ALIAS = ".inference-alias";

    // Increment this version number when the mappings change
    private static final int INDEX_MAPPING_VERSION = 2;

    public static Settings settings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
            .build();
    }

    /**
     * Reject any unknown fields being added by setting dynamic mappings to
     * {@code strict} for the top level object. A document that contains unknown
     * fields in the document root will be rejected at index time.
     *
     * The {@code service_settings} and {@code task_settings} objects
     * have dynamic mappings set to {@code false} which means all fields will
     * be accepted without throwing an error but those fields are not indexed.
     *
     * The reason for mixing {@code strict} and {@code false} dynamic settings
     * is that {@code service_settings} and {@code task_settings} are defined by
     * the inference services and therefore are not known when creating the
     * index. However, the top level settings are known in advance and can
     * be strictly mapped.
     *
     * If the top level strict mapping changes then the no new documents should
     * be indexed until the index mappings have been updated, this happens
     * automatically once all nodes in the cluster are of a compatible version.
     *
     * @return The index mappings
     */
    public static XContentBuilder mappings() {
        try {
            return jsonBuilder().startObject()
                .startObject(SINGLE_MAPPING_NAME)
                .startObject("_meta")
                .field(SystemIndexDescriptor.VERSION_META_KEY, INDEX_MAPPING_VERSION)
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
                .startObject("chunking_settings")
                .field("dynamic", "false")
                .startObject("properties")
                .startObject("strategy")
                .field("type", "keyword")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to build mappings for index " + INDEX_NAME, e);
        }
    }

    public static XContentBuilder mappingsV1() {
        try {
            return jsonBuilder().startObject()
                .startObject(SINGLE_MAPPING_NAME)
                .startObject("_meta")
                .field(SystemIndexDescriptor.VERSION_META_KEY, 1)
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
