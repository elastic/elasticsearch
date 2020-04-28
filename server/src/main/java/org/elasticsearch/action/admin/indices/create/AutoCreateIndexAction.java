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

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Api that auto creates an index that originate from requests that write into an index that doesn't yet exist.
 */
public final class AutoCreateIndexAction extends ActionType<AutoCreateAction.Response> {

    public static final AutoCreateIndexAction INSTANCE = new AutoCreateIndexAction();
    public static final String NAME = "indices:admin/auto_create_index";

    private AutoCreateIndexAction() {
        super(NAME, AutoCreateAction.Response::new);
    }

    public static final class TransportAction extends TransportMasterNodeAction<AutoCreateAction.Request, AutoCreateAction.Response> {

        private final MetadataCreateIndexService createIndexService;

        @Inject
        public TransportAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               MetadataCreateIndexService createIndexService) {
            super(NAME, transportService, clusterService, threadPool, actionFilters, AutoCreateAction.Request::new, indexNameExpressionResolver);
            this.createIndexService = createIndexService;
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        protected AutoCreateAction.Response read(StreamInput in) throws IOException {
            return new AutoCreateAction.Response(in);
        }

        @Override
        protected void masterOperation(Task task,
                                       AutoCreateAction.Request request,
                                       ClusterState state,
                                       ActionListener<AutoCreateAction.Response> listener) throws Exception {
            // Should this be an AckedClusterStateUpdateTask and
            // should we add ActiveShardsObserver.waitForActiveShards(...) here?
            // (This api used from TransportBulkAction only and it currently ignores CreateIndexResponse, so
            // I think there is no need to include acked and shard ackeds here)
            clusterService.submitStateUpdateTask("auto create resources for [" + request.getNames() + "]",
                new ClusterStateUpdateTask(Priority.HIGH) {

                    final Map<String, Exception> result = new HashMap<>();

                    @Override
                    public TimeValue timeout() {
                        return request.masterNodeTimeout();
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        listener.onFailure(e);
                    }

                    @Override
                    public ClusterState execute(ClusterState currentState) throws Exception {
                        return autoCreate(request, result, currentState, createIndexService, indexNameExpressionResolver);
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        listener.onResponse(new AutoCreateAction.Response(result));
                    }
                });
        }

        @Override
        protected ClusterBlockException checkBlock(AutoCreateAction.Request request, ClusterState state) {
            return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, request.getNames().toArray(new String[0]));
        }
    }

    static ClusterState autoCreate(AutoCreateAction.Request request,
                                   Map<String, Exception> result,
                                   ClusterState currentState,
                                   MetadataCreateIndexService createIndexService,
                                   IndexNameExpressionResolver resolver) {
        for (String indexName : request.getNames()) {
            indexName = resolver.resolveDateMathExpression(indexName);
            CreateIndexClusterStateUpdateRequest req = new CreateIndexClusterStateUpdateRequest(request.getCause(),
                indexName, indexName).masterNodeTimeout(request.masterNodeTimeout())
                .preferV2Templates(request.getPreferV2Templates());
            try {
                currentState = createIndexService.applyCreateIndexRequest(currentState, req, false);
                result.put(indexName, null);
            } catch (ResourceAlreadyExistsException e) {
                // ignore resource already exists exception.
            } catch (Exception e) {
                result.put(indexName, e);
            }
        }
        return currentState;
    }

}
