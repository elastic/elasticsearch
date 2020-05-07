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
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
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

        @Inject
        public TransportAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               MetadataCreateIndexService createIndexService) {
            super(NAME, transportService, clusterService, threadPool, actionFilters, CreateIndexRequest::new, indexNameExpressionResolver);
            this.createIndexService = createIndexService;
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
            TransportCreateIndexAction.innerCreateIndex(request, listener, indexNameExpressionResolver, createIndexService);
        }

        @Override
        protected ClusterBlockException checkBlock(CreateIndexRequest request, ClusterState state) {
            return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.index());
        }
    }

}
