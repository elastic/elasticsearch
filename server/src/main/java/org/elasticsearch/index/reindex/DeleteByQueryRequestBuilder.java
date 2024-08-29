/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;

public class DeleteByQueryRequestBuilder extends AbstractBulkByScrollRequestBuilder<DeleteByQueryRequest, DeleteByQueryRequestBuilder> {

    private Boolean abortOnVersionConflict;

    public DeleteByQueryRequestBuilder(ElasticsearchClient client) {
        this(client, new SearchRequestBuilder(client));
    }

    @SuppressWarnings("this-escape")
    private DeleteByQueryRequestBuilder(ElasticsearchClient client, SearchRequestBuilder search) {
        super(client, DeleteByQueryAction.INSTANCE, search);
        source().setFetchSource(false);
    }

    @Override
    protected DeleteByQueryRequestBuilder self() {
        return this;
    }

    @Override
    public DeleteByQueryRequestBuilder abortOnVersionConflict(boolean abortOnVersionConflict) {
        this.abortOnVersionConflict = abortOnVersionConflict;
        return this;
    }

    @Override
    public DeleteByQueryRequest request() {
        SearchRequest search = source().request();
        try {
            DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest(search, false);
            try {
                apply(deleteByQueryRequest);
                return deleteByQueryRequest;
            } catch (Exception e) {
                deleteByQueryRequest.decRef();
                throw e;
            }
        } catch (Exception e) {
            search.decRef();
            throw e;
        }
    }

    @Override
    public void apply(DeleteByQueryRequest request) {
        super.apply(request);
        if (abortOnVersionConflict != null) {
            request.setAbortOnVersionConflict(abortOnVersionConflict);
        }
    }
}
