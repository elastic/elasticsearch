/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.exists.indices;

import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

public class IndicesExistsRequestBuilder extends MasterNodeReadOperationRequestBuilder<
    IndicesExistsRequest,
    IndicesExistsResponse,
    IndicesExistsRequestBuilder> {

    public IndicesExistsRequestBuilder(ElasticsearchClient client, IndicesExistsAction action, String... indices) {
        super(client, action, new IndicesExistsRequest(indices));
    }

    public IndicesExistsRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    /**
     * Controls whether wildcard expressions will be expanded to existing open indices
     */
    public IndicesExistsRequestBuilder setExpandWildcardsOpen(boolean expandWildcardsOpen) {
        request.expandWilcardsOpen(expandWildcardsOpen);
        return this;
    }

    /**
     * Controls whether wildcard expressions will be expanded to existing closed indices
     */
    public IndicesExistsRequestBuilder setExpandWildcardsClosed(boolean expandWildcardsClosed) {
        request.expandWilcardsClosed(expandWildcardsClosed);
        return this;
    }
}
