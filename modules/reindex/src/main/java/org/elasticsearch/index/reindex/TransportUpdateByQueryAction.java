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
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

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
        new AsyncIndexBySearchAction((BulkByScrollTask) task, logger, scriptService, client, threadPool, state, request, listener)
                .start();
    }

    @Override
    protected void doExecute(UpdateByQueryRequest request, ActionListener<BulkIndexByScrollResponse> listener) {
        throw new UnsupportedOperationException("task required");
    }

    /**
     * Simple implementation of update-by-query using scrolling and bulk.
     */
    static class AsyncIndexBySearchAction extends AbstractAsyncBulkIndexByScrollAction<UpdateByQueryRequest> {
        public AsyncIndexBySearchAction(BulkByScrollTask task, ESLogger logger, ScriptService scriptService,
                ParentTaskAssigningClient client, ThreadPool threadPool, ClusterState clusterState, UpdateByQueryRequest request,
                ActionListener<BulkIndexByScrollResponse> listener) {
            super(task, logger, scriptService, clusterState, client, threadPool, request, request.getSearchRequest(), listener);
        }

        @Override
        protected IndexRequest buildIndexRequest(SearchHit doc) {
            IndexRequest index = new IndexRequest();
            index.index(doc.index());
            index.type(doc.type());
            index.id(doc.id());
            index.source(doc.sourceRef());
            index.versionType(VersionType.INTERNAL);
            index.version(doc.version());
            index.setPipeline(mainRequest.getPipeline());
            return index;
        }

        @Override
        protected void scriptChangedIndex(IndexRequest index, Object to) {
            throw new IllegalArgumentException("Modifying [" + IndexFieldMapper.NAME + "] not allowed");
        }

        @Override
        protected void scriptChangedType(IndexRequest index, Object to) {
            throw new IllegalArgumentException("Modifying [" + TypeFieldMapper.NAME + "] not allowed");
        }

        @Override
        protected void scriptChangedId(IndexRequest index, Object to) {
            throw new IllegalArgumentException("Modifying [" + IdFieldMapper.NAME + "] not allowed");
        }

        @Override
        protected void scriptChangedVersion(IndexRequest index, Object to) {
            throw new IllegalArgumentException("Modifying [_version] not allowed");
        }

        @Override
        protected void scriptChangedRouting(IndexRequest index, Object to) {
            throw new IllegalArgumentException("Modifying [" + RoutingFieldMapper.NAME + "] not allowed");
        }

        @Override
        protected void scriptChangedParent(IndexRequest index, Object to) {
            throw new IllegalArgumentException("Modifying [" + ParentFieldMapper.NAME + "] not allowed");
        }

        @Override
        protected void scriptChangedTimestamp(IndexRequest index, Object to) {
            throw new IllegalArgumentException("Modifying [" + TimestampFieldMapper.NAME + "] not allowed");
        }

        @Override
        protected void scriptChangedTTL(IndexRequest index, Object to) {
            throw new IllegalArgumentException("Modifying [" + TTLFieldMapper.NAME + "] not allowed");
        }
    }
}
