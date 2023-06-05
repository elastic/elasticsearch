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
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.synonyms.PutSynonymsAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class SynonymsManagementAPIService {
    public static final String SYNONYMS_INDEX_NAME_PATTERN = ".synonyms-*";
    public static final String SYNONYMS_INDEX_CONCRETE_NAME = ".synonyms-1";
    public static final String SYNONYMS_ALIAS_NAME = ".synonyms";

    public static final String SYNONYMS_FEATURE_NAME = "synonyms";
    public static final String SYNONYMS_SET_FIELD = "synonyms_set";
    public static final String SYNONYMS_FIELD = "synonyms";

    private final Client client;

    public static final String SYNONYMS_ORIGIN = "synonyms";
    public static final SystemIndexDescriptor SYNONYMS_DESCRIPTOR = SystemIndexDescriptor.builder()
        .setIndexPattern(SYNONYMS_INDEX_NAME_PATTERN)
        .setDescription("Synonyms index for synonyms managed through APIs")
        .setPrimaryIndex(SYNONYMS_INDEX_CONCRETE_NAME)
        .setAliasName(SYNONYMS_ALIAS_NAME)
        .setMappings(mappings())
        .setSettings(settings())
        .setVersionMetaKey("version")
        .setOrigin(SYNONYMS_ORIGIN)
        .build();

    public SynonymsManagementAPIService(Client client) {
        this.client = new OriginSettingClient(client, SYNONYMS_ORIGIN);
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
                        builder.startObject(SYNONYMS_FIELD);
                        {
                            builder.field("type", "match_only_text");
                        }
                        builder.endObject();
                        builder.startObject(SYNONYMS_SET_FIELD);
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
            throw new UncheckedIOException("Failed to build mappings for " + SYNONYMS_INDEX_CONCRETE_NAME, e);
        }
    }

    public void putSynonymsSet(String resourceName, SynonymsSet synonymsset, ActionListener<PutSynonymsAction.Response> listener) {

        // TODO Add synonym rules validation

        // Delete synonyms set if it existed previously. Avoid catching an index not found error by ignoring unavailable indices
        DeleteByQueryRequest dbqRequest = new DeleteByQueryRequest(SYNONYMS_ALIAS_NAME).setQuery(
            QueryBuilders.termQuery(SYNONYMS_SET_FIELD, resourceName)
        ).setIndicesOptions(IndicesOptions.fromOptions(true, true, false, false));

        client.execute(
            DeleteByQueryAction.INSTANCE,
            dbqRequest,
            listener.delegateFailure((deleteByQueryResponseListener, bulkByScrollResponse) -> {
                boolean created = bulkByScrollResponse.getDeleted() == 0;
                final List<BulkItemResponse.Failure> bulkFailures = bulkByScrollResponse.getBulkFailures();
                if (bulkFailures.isEmpty() == false) {
                    listener.onFailure(
                        new ElasticsearchException(
                            "Error updating synonyms: "
                                + bulkFailures.stream().map(BulkItemResponse.Failure::getMessage).collect(Collectors.joining("\n"))
                        )
                    );
                }

                // Insert as bulk requests
                BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
                try {
                    for (SynonymRule synonymRule : synonymsset.synonyms()) {

                        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                            builder.startObject();
                            {
                                builder.field(SYNONYMS_FIELD, synonymRule.synonyms());
                                builder.field(SYNONYMS_SET_FIELD, resourceName);
                            }
                            builder.endObject();

                            final IndexRequest indexRequest = new IndexRequest(SYNONYMS_ALIAS_NAME).opType(DocWriteRequest.OpType.INDEX)
                                .source(builder);
                            String synonymRuleId = synonymRule.id();
                            if (synonymRuleId == null) {
                                synonymRuleId = UUIDs.base64UUID();
                            }
                            indexRequest.id(resourceName + "_" + synonymRuleId);

                            bulkRequestBuilder.add(indexRequest);
                        }
                    }
                } catch (IOException ex) {
                    listener.onFailure(ex);
                }

                bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .execute(deleteByQueryResponseListener.delegateFailure((bulkResponseListener, bulkResponse) -> {
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
            })
        );
    }

    static Settings settings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-all")
            .build();
    }

}
