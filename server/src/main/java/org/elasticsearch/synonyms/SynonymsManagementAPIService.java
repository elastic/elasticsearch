/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.synonyms;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.synonyms.PutSynonymsAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class SynonymsManagementAPIService {
    public static final String SYNONYMS_INDEX = ".synonyms";
    public static final String SYNONYMS_ORIGIN = "synonyms";

    public static final String SYNONYMS_FEATURE_NAME = "synonyms";
    public static final String SYNONYM_SET_FIELD = "synonym_set";
    public static final String SYNONYM_FIELD = "synonym";

    private final Client client;

    public static final SystemIndexDescriptor SYNONYMS_DESCRIPTOR = SystemIndexDescriptor.builder()
        .setIndexPattern(SYNONYMS_INDEX + "*")
        .setDescription("Synonyms index for synonyms managed through APIs")
        .setPrimaryIndex(SYNONYMS_INDEX)
        .setMappings(mappings())
        .setSettings(settings())
        .setVersionMetaKey("version")
        .setOrigin(SYNONYMS_ORIGIN)
        .build();

    public SynonymsManagementAPIService(Client client) {
        // TODO Should we set an OriginSettingClient? We would need to check the origin at AuthorizationUtils if we set an
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
                    }
                    builder.endObject();
                    builder.field("dynamic", "strict");
                    builder.startObject("properties");
                    {
                        builder.startObject("synonym");
                        {
                            builder.field("type", "text");
                        }
                        builder.endObject();
                        builder.startObject(SYNONYM_SET_FIELD);
                        {
                            builder.field("type", "keyword");
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

    public void putSynonymSet(String resourceName, SynonymSet synonymSet, ActionListener<PutSynonymsAction.Response> listener) {

        // TODO Add synonym rules validation

        // Delete synonym set if it existed previously
        DeleteByQueryRequestBuilder deleteBuilder = new DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE).source(
            SYNONYMS_INDEX
        ).filter(QueryBuilders.termQuery(SYNONYM_SET_FIELD, resourceName));
        deleteBuilder.execute(listener.delegateFailure((deleteByQueryResponseListener, bulkByScrollResponse) -> {
            boolean created = bulkByScrollResponse.getDeleted() == 0;

            // Insert as bulk requests
            BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
            try {
                for (SynonymRule synonymRule : synonymSet.synonyms()) {

                    try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                        builder.startObject();
                        {
                            builder.field(SYNONYM_FIELD, synonymRule.synonym());
                            builder.field(SYNONYM_SET_FIELD, resourceName);
                        }
                        builder.endObject();

                        final IndexRequest indexRequest = new IndexRequest(SYNONYMS_INDEX).opType(DocWriteRequest.OpType.INDEX)
                            .source(builder);
                        final String synonymRuleId = synonymRule.id();
                        if (synonymRuleId != null) {
                            indexRequest.id(synonymRuleId);
                        }

                        bulkRequestBuilder.add(indexRequest);
                    }
                }
            } catch (IOException ex) {
                listener.onFailure(ex);
            }

            bulkRequestBuilder.execute(deleteByQueryResponseListener.delegateFailure((bulkResponseListener, bulkResponse) -> {
                if (bulkResponse.hasFailures() == false) {
                    PutSynonymsAction.Response.Result result = created
                        ? PutSynonymsAction.Response.Result.CREATED
                        : PutSynonymsAction.Response.Result.UPDATED;
                    bulkResponseListener.onResponse(new PutSynonymsAction.Response(result));
                } else {
                    bulkResponseListener.onFailure(
                        new ElasticsearchException("Couldn't update synonyms: " + bulkResponse.buildFailureMessage())
                    );
                }
            }));
        }));
    }

    static Settings settings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-all")
            .build();
    }

}
