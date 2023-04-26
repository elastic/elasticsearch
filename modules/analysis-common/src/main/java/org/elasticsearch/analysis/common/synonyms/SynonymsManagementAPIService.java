/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common.synonyms;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class SynonymsManagementAPIService {
    private static final FeatureFlag SYNONYMS_API_FEATURE_FLAG = new FeatureFlag("synonyms_api");
    public static final String SYNONYMS_INDEX = ".synonyms";
    public static final String SYNONYMS_ORIGIN = "synonyms";

    private final Client client;

    public SynonymsManagementAPIService(Client client) {
        // TODO Should we set an OriginSettingClient? We would need to check the origin at AuthorizationUtils if we set an
        this.client = client;
    }

    public void putSynonymSet(String resourceName, SynonymSet synonymSet, ActionListener<IndexResponse> listener) {

        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            {
                synonymSet.toXContent(builder, ToXContent.EMPTY_PARAMS);
            }
            builder.endObject();

            final IndexRequest indexRequest = new IndexRequest(SYNONYMS_INDEX).opType(DocWriteRequest.OpType.INDEX)
                .id(resourceName)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .source(builder);
            client.index(indexRequest, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    public static SystemIndexDescriptor getSystemIndexDescriptor() {
        return SystemIndexDescriptor.builder()
            .setIndexPattern(SYNONYMS_INDEX + "*")
            .setDescription("Synonyms index for synonyms managed through APIs")
            .setPrimaryIndex(SYNONYMS_INDEX)
            .setMappings(mappings())
            .setSettings(settings())
            .setVersionMetaKey("version")
            .setOrigin(SYNONYMS_ORIGIN)
            .build();
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
                    }
                    builder.endObject();
                    builder.field("dynamic", "strict");
                    builder.startObject("properties");
                    {
                        builder.startObject("synonyms");
                        {
                            builder.field("type", "object");
                            builder.field("enabled", "false");
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
            throw new UncheckedIOException("Failed to build mappings for " + SYNONYMS_INDEX, e);
        }
    }

    static Settings settings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-all")
            .build();
    }

    public static boolean isEnabled() {
        return SYNONYMS_API_FEATURE_FLAG.isEnabled();
    }
}
