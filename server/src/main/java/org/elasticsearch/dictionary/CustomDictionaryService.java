/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.dictionary;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

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
    private static final String CUSTOM_DICTIONARY_VALUE_FIELD = "value";

    public static final String CUSTOM_DICTIONARIES_FEATURE_NAME = "custom_dictionaries";
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

    public enum DictionaryOperationResult {
        CREATED,
        UPDATED,
        DELETED
    }

    private final Client client;

    public CustomDictionaryService(Client client) {
        this.client = new OriginSettingClient(client, CUSTOM_DICTIONARIES_ORIGIN);
    }

    public void getDictionary(String id, ActionListener<String> listener) {
        client.prepareGet(CUSTOM_DICTIONARIES_ALIAS_NAME, id).execute(ActionListener.wrap(getResponse -> {
            if (getResponse.isExists() == false) {
                listener.onFailure(new ResourceNotFoundException("custom dictionary [" + id + "] not found"));
                return;
            }

            // Extract the dictionary value from the source
            Object dictionaryValue = getResponse.getSourceAsMap().get(CUSTOM_DICTIONARY_VALUE_FIELD);
            if (dictionaryValue == null) {
                listener.onFailure(new IllegalStateException("custom dictionary [" + id + "] has no value field"));
                return;
            }

            listener.onResponse(dictionaryValue.toString());
        }, e -> {
            if (e instanceof IndexNotFoundException) {
                listener.onFailure(new ResourceNotFoundException("custom dictionary [" + id + "] not found"));
            } else {
                listener.onFailure(e);
            }
        }));
    }

    public void putDictionary(String id, String content, ActionListener<DictionaryOperationResult> listener) {
        try {
            IndexRequest indexRequest = createDictionaryIndexRequest(id, content);
            indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

            client.index(indexRequest, listener.delegateFailureAndWrap((l, indexResponse) -> {
                DictionaryOperationResult result = indexResponse.status() == RestStatus.CREATED
                    ? DictionaryOperationResult.CREATED
                    : DictionaryOperationResult.UPDATED;
                l.onResponse(result);
            }));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    public void deleteDictionary(String id, ActionListener<DictionaryOperationResult> listener) {
        client.prepareDelete(CUSTOM_DICTIONARIES_ALIAS_NAME, id)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .execute(ActionListener.wrap(deleteResponse -> {
                if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                    listener.onFailure(new ResourceNotFoundException("custom dictionary [" + id + "] not found"));
                    return;
                }
                listener.onResponse(DictionaryOperationResult.DELETED);
            }, e -> {
                if (e instanceof IndexNotFoundException) {
                    listener.onFailure(new ResourceNotFoundException("custom dictionary [" + id + "] not found"));
                } else {
                    listener.onFailure(e);
                }
            }));
    }

    private IndexRequest createDictionaryIndexRequest(String id, String content) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            {
                builder.field(CUSTOM_DICTIONARY_VALUE_FIELD, content);
            }
            builder.endObject();

            return new IndexRequest(CUSTOM_DICTIONARIES_ALIAS_NAME).id(id).opType(DocWriteRequest.OpType.INDEX).source(builder);
        }
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
                        builder.field(SystemIndexDescriptor.VERSION_META_KEY, CUSTOM_DICTIONARIES_INDEX_MAPPINGS_VERSION);
                    }
                    builder.endObject();
                    builder.field("dynamic", "strict");
                    builder.startObject("properties");
                    {
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
