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
import org.elasticsearch.action.delete.DeleteRequest;
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
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportDeleteByQueryAction extends HandledTransportAction<DeleteByQueryRequest, BulkIndexByScrollResponse> {
    private final Client client;
    private final ScriptService scriptService;
    private final ClusterService clusterService;

    @Inject
    public TransportDeleteByQueryAction(Settings settings, ThreadPool threadPool, ActionFilters actionFilters,
                                        IndexNameExpressionResolver resolver, Client client, TransportService transportService,
                                        ScriptService scriptService, ClusterService clusterService) {
        super(settings, DeleteByQueryAction.NAME, threadPool, transportService, actionFilters, resolver, DeleteByQueryRequest::new);
        this.client = client;
        this.scriptService = scriptService;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, DeleteByQueryRequest request, ActionListener<BulkIndexByScrollResponse> listener) {
        ClusterState state = clusterService.state();
        ParentTaskAssigningClient client = new ParentTaskAssigningClient(this.client, clusterService.localNode(), task);
        new AsyncDeleteBySearchAction((BulkByScrollTask) task, logger, client, threadPool, request, listener, scriptService, state).start();
    }

    @Override
    protected void doExecute(DeleteByQueryRequest request, ActionListener<BulkIndexByScrollResponse> listener) {
        throw new UnsupportedOperationException("task required");
    }

    /**
     * Implementation of delete-by-query using scrolling and bulk.
     */
    static class AsyncDeleteBySearchAction extends AbstractAsyncBulkIndexByScrollAction<DeleteByQueryRequest> {

        public AsyncDeleteBySearchAction(BulkByScrollTask task, ESLogger logger, ParentTaskAssigningClient client, ThreadPool threadPool,
                                         DeleteByQueryRequest request, ActionListener<BulkIndexByScrollResponse> listener,
                                         ScriptService scriptService, ClusterState clusterState) {
            super(task, logger, client, threadPool, request, listener, scriptService, clusterState);
        }

        @Override
        protected boolean needsSourceDocumentVersions() {
            /*
             * We always need the version of the source document so we can report a version conflict if we try to delete it and it has been
             * changed.
             */
            return true;
        }

        @Override
        protected boolean accept(ScrollableHitSource.Hit doc) {
            // Delete-by-query does not require the source to delete a document
            // and the default implementation checks for it
            return true;
        }

        @Override
        protected RequestWrapper<DeleteRequest> buildRequest(ScrollableHitSource.Hit doc) {
            DeleteRequest delete = new DeleteRequest();
            delete.index(doc.getIndex());
            delete.type(doc.getType());
            delete.id(doc.getId());
            delete.version(doc.getVersion());
            return wrap(delete);
        }

        /**
         * Overrides the parent {@link AbstractAsyncBulkIndexByScrollAction#copyMetadata(RequestWrapper, ScrollableHitSource.Hit)}
         * method that is much more Update/Reindex oriented and so also copies things like timestamp/ttl which we
         * don't care for a deletion.
         */
        @Override
        protected RequestWrapper<?> copyMetadata(RequestWrapper<?> request, ScrollableHitSource.Hit doc) {
            request.setParent(doc.getParent());
            request.setRouting(doc.getRouting());
            return request;
        }
    }
}
