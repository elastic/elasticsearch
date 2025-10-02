/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.readonly;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock;
import org.elasticsearch.core.TimeValue;

/**
 * Builder for remove index block request
 */
public class RemoveIndexBlockRequestBuilder extends AcknowledgedRequestBuilder<
    RemoveIndexBlockRequest,
    RemoveIndexBlockResponse,
    RemoveIndexBlockRequestBuilder> {

    public RemoveIndexBlockRequestBuilder(
        ElasticsearchClient client,
        TimeValue masterTimeout,
        TimeValue ackTimeout,
        APIBlock block,
        String... indices
    ) {
        super(client, TransportRemoveIndexBlockAction.TYPE, new RemoveIndexBlockRequest(masterTimeout, ackTimeout, block, indices));
    }

    /**
     * Sets the indices to be unblocked
     *
     * @param indices the indices to be unblocked
     * @return the request itself
     */
    public RemoveIndexBlockRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and how to deal with wildcard expressions.
     * For example indices that don't exist.
     *
     * @param indicesOptions the desired behaviour regarding indices to ignore and wildcard indices expressions
     * @return the request itself
     */
    public RemoveIndexBlockRequestBuilder setIndicesOptions(IndicesOptions indicesOptions) {
        request.indicesOptions(indicesOptions);
        return this;
    }
}
