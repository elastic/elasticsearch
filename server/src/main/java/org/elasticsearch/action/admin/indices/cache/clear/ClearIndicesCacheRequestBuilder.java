/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.cache.clear;

import org.elasticsearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;

public class ClearIndicesCacheRequestBuilder extends BroadcastOperationRequestBuilder<
    ClearIndicesCacheRequest,
    ClearIndicesCacheResponse,
    ClearIndicesCacheRequestBuilder> {

    public ClearIndicesCacheRequestBuilder(ElasticsearchClient client, ClearIndicesCacheAction action) {
        super(client, action, new ClearIndicesCacheRequest());
    }

    public ClearIndicesCacheRequestBuilder setQueryCache(boolean queryCache) {
        request.queryCache(queryCache);
        return this;
    }

    public ClearIndicesCacheRequestBuilder setRequestCache(boolean requestCache) {
        request.requestCache(requestCache);
        return this;
    }

    public ClearIndicesCacheRequestBuilder setFieldDataCache(boolean fieldDataCache) {
        request.fieldDataCache(fieldDataCache);
        return this;
    }

}
