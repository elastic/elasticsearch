/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.refresh;

import org.elasticsearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.client.internal.ElasticsearchClient;

/**
 * A refresh request making all operations performed since the last refresh available for search. The (near) real-time
 * capabilities depends on the index engine used. For example, the internal one requires refresh to be called, but by
 * default a refresh is scheduled periodically.
 */
public class RefreshRequestBuilder extends BroadcastOperationRequestBuilder<RefreshRequest, BroadcastResponse, RefreshRequestBuilder> {

    public RefreshRequestBuilder(ElasticsearchClient client) {
        super(client, RefreshAction.INSTANCE, new RefreshRequest());
    }
}
