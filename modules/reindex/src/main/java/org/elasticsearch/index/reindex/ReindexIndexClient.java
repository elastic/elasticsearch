/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

public class ReindexIndexClient {

    // TODO: Eventually this should be an alias for index versioning
    public static final String REINDEX_INDEX = ".reindex";
    public static final String REINDEX_ORIGIN = "reindex";

    private final Client client;
    private final ClusterService clusterService;
    private final NamedXContentRegistry xContentRegistry;

    public ReindexIndexClient(Client client, ClusterService clusterService, NamedXContentRegistry xContentRegistry) {
        this.client = new OriginSettingClient(client, REINDEX_ORIGIN);
        this.clusterService = clusterService;
        this.xContentRegistry = xContentRegistry;
    }

    public void getReindexTaskDoc(String taskId, ActionListener<ReindexTaskIndexState> listener) {
        GetRequest getRequest = new GetRequest(REINDEX_INDEX).id(taskId);
        client.get(getRequest, new ActionListener<>() {
            @Override
            public void onResponse(GetResponse response) {
                BytesReference source = response.getSourceAsBytesRef();
                try (XContentParser parser = XContentHelper.createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, source,
                    XContentType.JSON)) {
                    ReindexTaskIndexState taskState = ReindexTaskIndexState.fromXContent(parser);
                    listener.onResponse(taskState);
                } catch (IOException e) {
                    listener.onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    public void createReindexTaskDoc(String taskId, ReindexTaskIndexState reindexState, ActionListener<Void> listener) {
        boolean reindexIndexExists = clusterService.state().routingTable().hasIndex(ReindexIndexClient.REINDEX_INDEX);
        createReindexTaskDoc(taskId, reindexState, reindexIndexExists, listener);
    }

    private void createReindexTaskDoc(String taskId, ReindexTaskIndexState reindexState, boolean indexExists,
                                     ActionListener<Void> listener) {
        if (indexExists) {
            index(taskId, reindexState, DocWriteRequest.OpType.CREATE, listener);
        } else {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest();
            createIndexRequest.settings(reindexIndexSettings());
            createIndexRequest.index(REINDEX_INDEX);
            createIndexRequest.cause("auto(reindex api)");
            createIndexRequest.mapping("_doc", "{\"dynamic\": false}", XContentType.JSON);

            client.admin().indices().create(createIndexRequest, new ActionListener<>() {
                @Override
                public void onResponse(CreateIndexResponse result) {
                    createReindexTaskDoc(taskId, reindexState, true, listener);
                }

                @Override
                public void onFailure(Exception e) {
                    if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                        try {
                            createReindexTaskDoc(taskId, reindexState, true, listener);
                        } catch (Exception inner) {
                            inner.addSuppressed(e);
                            listener.onFailure(inner);
                        }
                    } else {
                        listener.onFailure(e);
                    }
                }
            });
        }
    }

    // TODO: Potentially add compare-and-set semantics to ensure that another node has not been assigned
    //  this task
    public void updateReindexTaskDoc(String taskId, ReindexTaskIndexState reindexState, ActionListener<Void> listener) {
        index(taskId, reindexState, DocWriteRequest.OpType.INDEX, listener);
    }

    private void index(String taskId, ReindexTaskIndexState reindexState, DocWriteRequest.OpType opType, ActionListener<Void> listener) {
        IndexRequest indexRequest = new IndexRequest(REINDEX_INDEX).id(taskId).opType(opType);
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            reindexState.toXContent(builder, ToXContent.EMPTY_PARAMS);
            indexRequest.source(builder);
        } catch (IOException e) {
            listener.onFailure(new ElasticsearchException("Couldn't serialize ReindexTaskIndexState into XContent", e));
            return;
        }
        client.index(indexRequest, new ActionListener<>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                listener.onResponse(null);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private static Settings reindexIndexSettings() {
        // TODO: Copied from task index
        return Settings.builder()
            .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetaData.INDEX_AUTO_EXPAND_REPLICAS_SETTING.getKey(), "0-1")
            .put(IndexMetaData.SETTING_PRIORITY, Integer.MAX_VALUE)
            .build();
    }
}
