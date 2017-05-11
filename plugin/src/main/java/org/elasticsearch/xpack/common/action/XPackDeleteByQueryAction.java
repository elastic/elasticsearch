/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.common.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class XPackDeleteByQueryAction extends Action<DeleteByQueryRequest, BulkByScrollResponse,
        DeleteByQueryRequestBuilder> {

    public static final XPackDeleteByQueryAction INSTANCE = new XPackDeleteByQueryAction();
    // Ideally we'd use an "internal" action here as we don't want transport client users running it
    // but unfortunately the _xpack user is forbidden to run "internal" actions as these are really
    // intended to be run as the system user
    public static final String NAME = "indices:internal/data/write/xpackdeletebyquery";

    private XPackDeleteByQueryAction() {
        super(NAME);
    }

    @Override
    public DeleteByQueryRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return DeleteByQueryAction.INSTANCE.newRequestBuilder(client);
    }

    @Override
    public BulkByScrollResponse newResponse() {
        return DeleteByQueryAction.INSTANCE.newResponse();
    }

    public static class TransportAction extends HandledTransportAction<DeleteByQueryRequest, BulkByScrollResponse> {
        private final Client client;

        @Inject
        public TransportAction(Settings settings, ThreadPool threadPool, ActionFilters actionFilters,
                                            IndexNameExpressionResolver resolver, TransportService transportService, Client client) {
            super(settings, XPackDeleteByQueryAction.NAME, threadPool, transportService, actionFilters, resolver, DeleteByQueryRequest::new);
            this.client = client;
        }

        @Override
        public void doExecute(Task task, DeleteByQueryRequest request, ActionListener<BulkByScrollResponse> listener) {
            this.client.execute(DeleteByQueryAction.INSTANCE, request, listener);
        }

        @Override
        protected void doExecute(DeleteByQueryRequest request, ActionListener<BulkByScrollResponse> listener) {
            throw new UnsupportedOperationException("task required");
        }
    }

    private static IndicesOptions addIgnoreUnavailable(IndicesOptions indicesOptions) {
        return IndicesOptions.fromOptions(true, indicesOptions.allowNoIndices(),
                indicesOptions.expandWildcardsOpen(), indicesOptions.expandWildcardsClosed(),
                indicesOptions);
    }

}
