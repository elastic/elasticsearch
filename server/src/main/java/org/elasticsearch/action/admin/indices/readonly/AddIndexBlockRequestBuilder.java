/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.readonly;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock;

/**
 * Builder for add index block request
 */
public class AddIndexBlockRequestBuilder extends AcknowledgedRequestBuilder<
    AddIndexBlockRequest,
    AddIndexBlockResponse,
    AddIndexBlockRequestBuilder> {

    public AddIndexBlockRequestBuilder(ElasticsearchClient client, AddIndexBlockAction action, APIBlock block, String... indices) {
        super(client, action, new AddIndexBlockRequest(block, indices));
    }

    /**
     * Sets the indices to be blocked
     *
     * @param indices the indices to be blocked
     * @return the request itself
     */
    public AddIndexBlockRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and wildcard indices expressions
     * For example indices that don't exist.
     *
     * @param indicesOptions the desired behaviour regarding indices to ignore and indices wildcard expressions
     * @return the request itself
     */
    public AddIndexBlockRequestBuilder setIndicesOptions(IndicesOptions indicesOptions) {
        request.indicesOptions(indicesOptions);
        return this;
    }
}
