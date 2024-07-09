/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.elasticsearch.Version;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class WordListsIndexService {
    public static final String WORD_LISTS_FEATURE_NAME = "synonyms";
    public static final String ANALYSIS_ORIGIN = "analysis";

    private static final String WORD_LISTS_INDEX_NAME_PATTERN = ".word_lists-*";
    private static final int WORD_LISTS_INDEX_FORMAT = 1;
    private static final String WORD_LISTS_INDEX_CONCRETE_NAME = ".word_lists-" + WORD_LISTS_INDEX_FORMAT;
    private static final String WORD_LISTS_ALIAS_NAME = ".word_lists";
    private static final int WORD_LISTS_INDEX_MAPPINGS_VERSION = 1;
    private static final String WORD_LIST_ID_FIELD = "id";
    private static final String WORD_LIST_VALUE_FIELD = "value";

    public static final SystemIndexDescriptor WORD_LISTS_DESCRIPTOR = SystemIndexDescriptor.builder()
        .setIndexPattern(WORD_LISTS_INDEX_NAME_PATTERN)
        .setDescription("System index for word lists fetched from remote sources")
        .setPrimaryIndex(WORD_LISTS_INDEX_CONCRETE_NAME)
        .setAliasName(WORD_LISTS_ALIAS_NAME)
        .setIndexFormat(WORD_LISTS_INDEX_FORMAT)
        .setMappings(mappings())
        .setSettings(settings())
        .setVersionMetaKey("version")
        .setOrigin(ANALYSIS_ORIGIN)
        .build();

    private final Client client;

    public WordListsIndexService(Client client) {
        this.client = client;
    }

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
                        builder.field(SystemIndexDescriptor.VERSION_META_KEY, WORD_LISTS_INDEX_MAPPINGS_VERSION);
                    }
                    builder.endObject();
                    builder.field("dynamic", "strict");
                    builder.startObject("properties");
                    {
                        builder.startObject(WORD_LIST_ID_FIELD);
                        {
                            builder.field("type", "keyword");
                        }
                        builder.endObject();
                        builder.startObject(WORD_LIST_VALUE_FIELD);
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
            throw new UncheckedIOException("Failed to build mappings for " + WORD_LISTS_INDEX_CONCRETE_NAME, e);
        }
    }

    private static Settings settings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-all")
            .put(IndexMetadata.INDEX_FORMAT_SETTING.getKey(), WORD_LISTS_INDEX_FORMAT)
            .build();
    }
}
