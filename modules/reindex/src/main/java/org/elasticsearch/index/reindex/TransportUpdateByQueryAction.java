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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ParentTaskAssigningClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.internal.IdFieldMapper;
import org.elasticsearch.index.mapper.internal.IndexFieldMapper;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.mapper.internal.RoutingFieldMapper;
import org.elasticsearch.index.mapper.internal.TTLFieldMapper;
import org.elasticsearch.index.mapper.internal.TimestampFieldMapper;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Map;
import java.util.function.BiFunction;

public class TransportUpdateByQueryAction extends HandledTransportAction<UpdateByQueryRequest, BulkIndexByScrollResponse> {
    private final Client client;
    private final ScriptService scriptService;
    private final ClusterService clusterService;

    @Inject
    public TransportUpdateByQueryAction(Settings settings, ThreadPool threadPool, ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver, Client client, TransportService transportService,
            ScriptService scriptService, ClusterService clusterService) {
        super(settings, UpdateByQueryAction.NAME, threadPool, transportService, actionFilters,
                indexNameExpressionResolver, UpdateByQueryRequest::new);
        this.client = client;
        this.scriptService = scriptService;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, UpdateByQueryRequest request, ActionListener<BulkIndexByScrollResponse> listener) {
        ClusterState state = clusterService.state();
        ParentTaskAssigningClient client = new ParentTaskAssigningClient(this.client, clusterService.localNode(), task);
        new AsyncIndexBySearchAction((BulkByScrollTask) task, logger, client, threadPool, request, listener, scriptService, state).start();
    }

    @Override
    protected void doExecute(UpdateByQueryRequest request, ActionListener<BulkIndexByScrollResponse> listener) {
        throw new UnsupportedOperationException("task required");
    }

    /**
     * Simple implementation of update-by-query using scrolling and bulk.
     */
    static class AsyncIndexBySearchAction extends AbstractAsyncBulkIndexByScrollAction<UpdateByQueryRequest> {

        public AsyncIndexBySearchAction(BulkByScrollTask task, ESLogger logger, ParentTaskAssigningClient client, ThreadPool threadPool,
                                        UpdateByQueryRequest request, ActionListener<BulkIndexByScrollResponse> listener,
                                        ScriptService scriptService, ClusterState clusterState) {
            super(task, logger, client, threadPool, request, request.getSearchRequest(), listener, scriptService, clusterState);
        }

        @Override
        protected BiFunction<RequestWrapper<?>, SearchHit, RequestWrapper<?>> buildScriptApplier() {
            Script script = mainRequest.getScript();
            if (script != null) {
                return new UpdateByQueryScriptApplier(task, scriptService, script, clusterState, script.getParams());
            }
            return super.buildScriptApplier();
        }

        @Override
        protected RequestWrapper<IndexRequest> buildRequest(SearchHit doc) {
            IndexRequest index = new IndexRequest();
            index.index(doc.index());
            index.type(doc.type());
            index.id(doc.id());
            index.source(doc.sourceRef());
            index.versionType(VersionType.INTERNAL);
            index.version(doc.version());
            index.setPipeline(mainRequest.getPipeline());
            return wrap(index);
        }

        class UpdateByQueryScriptApplier extends ScriptApplier {

            UpdateByQueryScriptApplier(BulkByScrollTask task, ScriptService scriptService, Script script, ClusterState state,
                                 Map<String, Object> params) {
                super(task, scriptService, script, state, params);
            }

            @Override
            protected void scriptChangedIndex(RequestWrapper<?> request, Object to) {
                throw new IllegalArgumentException("Modifying [" + IndexFieldMapper.NAME + "] not allowed");
            }

            @Override
            protected void scriptChangedType(RequestWrapper<?> request, Object to) {
                throw new IllegalArgumentException("Modifying [" + TypeFieldMapper.NAME + "] not allowed");
            }

            @Override
            protected void scriptChangedId(RequestWrapper<?> request, Object to) {
                throw new IllegalArgumentException("Modifying [" + IdFieldMapper.NAME + "] not allowed");
            }

            @Override
            protected void scriptChangedVersion(RequestWrapper<?> request, Object to) {
                throw new IllegalArgumentException("Modifying [_version] not allowed");
            }

            @Override
            protected void scriptChangedRouting(RequestWrapper<?> request, Object to) {
                throw new IllegalArgumentException("Modifying [" + RoutingFieldMapper.NAME + "] not allowed");
            }

            @Override
            protected void scriptChangedParent(RequestWrapper<?> request, Object to) {
                throw new IllegalArgumentException("Modifying [" + ParentFieldMapper.NAME + "] not allowed");
            }

            @Override
            protected void scriptChangedTimestamp(RequestWrapper<?> request, Object to) {
                throw new IllegalArgumentException("Modifying [" + TimestampFieldMapper.NAME + "] not allowed");
            }

            @Override
            protected void scriptChangedTTL(RequestWrapper<?> request, Object to) {
                throw new IllegalArgumentException("Modifying [" + TTLFieldMapper.NAME + "] not allowed");
            }
        }
    }
}
