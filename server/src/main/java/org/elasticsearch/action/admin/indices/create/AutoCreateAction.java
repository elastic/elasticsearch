/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateV2;
import org.elasticsearch.cluster.metadata.IndexTemplateV2.DataStreamTemplate;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateDataStreamService;
import org.elasticsearch.cluster.metadata.MetadataCreateDataStreamService.CreateDataSteamClusterStateUpdateRequest;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

/**
 * Api that auto creates an index that originate from requests that write into an index that doesn't yet exist.
 */
public final class AutoCreateAction extends ActionType<CreateIndexResponse> {

    public static final AutoCreateAction INSTANCE = new AutoCreateAction();
    public static final String NAME = "indices:admin/auto_create";

    private AutoCreateAction() {
        super(NAME, CreateIndexResponse::new);
    }

    public static final class TransportAction extends TransportMasterNodeAction<CreateIndexRequest, CreateIndexResponse> {

        private final MetadataCreateIndexService createIndexService;
        private final MetadataCreateDataStreamService metadataCreateDataStreamService;

        @Inject
        public TransportAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               MetadataCreateIndexService createIndexService,
                               MetadataCreateDataStreamService metadataCreateDataStreamService) {
            super(NAME, transportService, clusterService, threadPool, actionFilters, CreateIndexRequest::new, indexNameExpressionResolver);
            this.createIndexService = createIndexService;
            this.metadataCreateDataStreamService = metadataCreateDataStreamService;
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        protected CreateIndexResponse read(StreamInput in) throws IOException {
            return new CreateIndexResponse(in);
        }

        @Override
        protected void masterOperation(Task task,
                                       CreateIndexRequest request,
                                       ClusterState state,
                                       ActionListener<CreateIndexResponse> listener) {
            DataStreamTemplate dataStreamTemplate = resolveAutoCreateDataStream(request, state.metadata());
            if (dataStreamTemplate != null) {
                CreateDataSteamClusterStateUpdateRequest createRequest = new CreateDataSteamClusterStateUpdateRequest(
                    request.index(), dataStreamTemplate.getTimestampField(), request.timeout());

                metadataCreateDataStreamService.createDataStream(createRequest, ActionListener.wrap(
                    response -> {
                        listener.onResponse(new CreateIndexResponse(response.isAcknowledged(), false, request.index()));
                    },
                    listener::onFailure)
                );
            } else {
                TransportCreateIndexAction.innerCreateIndex(request, listener, indexNameExpressionResolver, createIndexService);
            }
        }

        @Override
        protected ClusterBlockException checkBlock(CreateIndexRequest request, ClusterState state) {
            return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.index());
        }
    }

    static DataStreamTemplate resolveAutoCreateDataStream(CreateIndexRequest request, Metadata metadata) {
        String v2Template = MetadataIndexTemplateService.findV2Template(metadata, request.index(), false);
        if (v2Template != null && resolvePreferV2Templates(request)) {
            IndexTemplateV2 indexTemplateV2 = metadata.templatesV2().get(v2Template);
            if (indexTemplateV2.getDataStreamTemplate() != null) {
                return indexTemplateV2.getDataStreamTemplate();
            }
        }

        return null;
    }

    private static boolean resolvePreferV2Templates(CreateIndexRequest request) {
        return request.preferV2Templates() == null ?
            IndexMetadata.PREFER_V2_TEMPLATES_SETTING.get(request.settings()) : request.preferV2Templates();
    }

}
