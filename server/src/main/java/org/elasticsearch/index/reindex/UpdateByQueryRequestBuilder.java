/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;

public class UpdateByQueryRequestBuilder extends AbstractBulkIndexByScrollRequestBuilder<
    UpdateByQueryRequest,
    UpdateByQueryRequestBuilder> {

    private Boolean abortOnVersionConflict;
    private String pipeline;

    public UpdateByQueryRequestBuilder(ElasticsearchClient client) {
        this(client, new SearchRequestBuilder(client));
    }

    private UpdateByQueryRequestBuilder(ElasticsearchClient client, SearchRequestBuilder search) {
        super(client, UpdateByQueryAction.INSTANCE, search);
    }

    @Override
    protected UpdateByQueryRequestBuilder self() {
        return this;
    }

    @Override
    public UpdateByQueryRequestBuilder abortOnVersionConflict(boolean abortOnVersionConflict) {
        this.abortOnVersionConflict = abortOnVersionConflict;
        return this;
    }

    public UpdateByQueryRequestBuilder setPipeline(String pipeline) {
        this.pipeline = pipeline;
        return this;
    }

    @Override
    public UpdateByQueryRequest request() {
        SearchRequest search = source().request();
        try {
            UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest(search, false);
            try {
                apply(updateByQueryRequest);
                return updateByQueryRequest;
            } catch (Exception e) {
                updateByQueryRequest.decRef();
                throw e;
            }
        } catch (Exception e) {
            search.decRef();
            throw e;
        }
    }

    @Override
    public void apply(UpdateByQueryRequest request) {
        super.apply(request);
        if (abortOnVersionConflict != null) {
            request.setAbortOnVersionConflict(abortOnVersionConflict);
        }
        if (pipeline != null) {
            request.setPipeline(pipeline);
        }
    }
}
