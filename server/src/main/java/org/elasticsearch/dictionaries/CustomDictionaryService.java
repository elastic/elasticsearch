/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.dictionaries;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.elasticsearch.action.admin.cluster.node.tasks.get.TransportGetTaskAction.TASKS_ORIGIN;
import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class CustomDictionaryService {
    private static final String CUSTOM_DICTIONARIES_ORIGIN = TASKS_ORIGIN;  // TODO: Use more restricted origin
    private static final String CUSTOM_DICTIONARIES_INDEX_NAME_PATTERN = ".custom_dictionaries-*";
    private static final int CUSTOM_DICTIONARIES_INDEX_FORMAT = 1;
    private static final String CUSTOM_DICTIONARIES_INDEX_CONCRETE_NAME = ".custom_dictionaries-" + CUSTOM_DICTIONARIES_INDEX_FORMAT;
    private static final String CUSTOM_DICTIONARIES_ALIAS_NAME = ".custom_dictionaries";
    private static final int CUSTOM_DICTIONARIES_INDEX_MAPPINGS_VERSION = 1;
    private static final String CUSTOM_DICTIONARY_ID_FIELD = "id";
    private static final String CUSTOM_DICTIONARY_VALUE_FIELD = "value";

    public static final SystemIndexDescriptor CUSTOM_DICTIONARIES_DESCRIPTOR = SystemIndexDescriptor.builder()
        .setIndexPattern(CUSTOM_DICTIONARIES_INDEX_NAME_PATTERN)
        .setDescription("System index for custom dictionaries")
        .setPrimaryIndex(CUSTOM_DICTIONARIES_INDEX_CONCRETE_NAME)
        .setAliasName(CUSTOM_DICTIONARIES_ALIAS_NAME)
        .setIndexFormat(CUSTOM_DICTIONARIES_INDEX_FORMAT)
        .setMappings(mappings())
        .setSettings(settings())
        .setOrigin(CUSTOM_DICTIONARIES_ORIGIN)
        .build();

    private static XContentBuilder mappings() {
        try {
            XContentBuilder builder = jsonBuilder();
            builder.startObject();
            {
                builder.startObject(SINGLE_MAPPING_NAME);
                {
                    builder.startObject("_meta");
                    {
                        builder.field("version", Version.CURRENT.toString());
                        builder.field(SystemIndexDescriptor.VERSION_META_KEY, CUSTOM_DICTIONARIES_INDEX_MAPPINGS_VERSION);
                    }
                    builder.endObject();
                    builder.field("dynamic", "strict");
                    builder.startObject("properties");
                    {
                        builder.startObject(CUSTOM_DICTIONARY_ID_FIELD);
                        {
                            builder.field("type", "keyword");
                        }
                        builder.endObject();
                        builder.startObject(CUSTOM_DICTIONARY_VALUE_FIELD);
                        {
                            builder.field("type", "keyword");
                            builder.field("doc_values", false);
                            builder.field("index", false);
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            return builder;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to build mappings for " + CUSTOM_DICTIONARIES_INDEX_CONCRETE_NAME, e);
        }
    }

    private static Settings settings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-all")
            .put(IndexMetadata.INDEX_FORMAT_SETTING.getKey(), CUSTOM_DICTIONARIES_INDEX_FORMAT)
            .build();
    }
}
