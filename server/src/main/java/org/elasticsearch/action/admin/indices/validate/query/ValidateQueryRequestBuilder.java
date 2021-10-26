/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.validate.query;

import org.elasticsearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.index.query.QueryBuilder;

public class ValidateQueryRequestBuilder
    extends BroadcastOperationRequestBuilder<ValidateQueryRequest, ValidateQueryResponse, ValidateQueryRequestBuilder> {

    public ValidateQueryRequestBuilder(ElasticsearchClient client, ValidateQueryAction action) {
        super(client, action, new ValidateQueryRequest());
    }

    /**
     * The query to validate.
     *
     * @see org.elasticsearch.index.query.QueryBuilders
     */
    public ValidateQueryRequestBuilder setQuery(QueryBuilder queryBuilder) {
        request.query(queryBuilder);
        return this;
    }

    /**
     * Indicates if detailed information about the query should be returned.
     *
     * @see org.elasticsearch.index.query.QueryBuilders
     */
    public ValidateQueryRequestBuilder setExplain(boolean explain) {
        request.explain(explain);
        return this;
    }

    /**
     * Indicates whether the query should be rewritten into primitive queries
     */
    public ValidateQueryRequestBuilder setRewrite(boolean rewrite) {
        request.rewrite(rewrite);
        return this;
    }

    /**
     * Indicates whether the query should be validated on all shards
     */
    public ValidateQueryRequestBuilder setAllShards(boolean rewrite) {
        request.allShards(rewrite);
        return this;
    }
}
