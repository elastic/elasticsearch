/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.RELEVANCE_SETTINGS_ORIGIN;

public class RelevanceSettingsIndexManager {
    public static SystemIndexDescriptor getSystemIndexDescriptor() {
        return SystemIndexDescriptor.builder()
            .setIndexPattern(".ent-search*")
            .setDescription("Relevance Settings Index")
            .setMappings(mappings())
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
                    .build()
            )
            .setOrigin(RELEVANCE_SETTINGS_ORIGIN)
            .setVersionMetaKey("version")
            .setPrimaryIndex(".ent-search")
            .setNetNew()
            .setType(SystemIndexDescriptor.Type.INTERNAL_MANAGED)
            .build();
    }

    private static XContentBuilder mappings() {
        try {
            return jsonBuilder().startObject()
                .startObject(SINGLE_MAPPING_NAME)
                .startObject("_meta")
                .field("version", Version.CURRENT)
                .endObject()
                .field("dynamic", "strict")
                .startObject("properties")
                .startObject("name")
                .field("type", "keyword")
                .endObject()
                .startObject("type")
                .field("type", "keyword")
                .endObject()
                .startObject("query_type")
                .field("type", "keyword")
                .endObject()
                .startObject("query_configuration")
                .startObject("properties")
                .startObject("fields")
                .field("type", "keyword")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to build mappings", e);
        }
    }
}
