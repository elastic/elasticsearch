/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.flush;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.ElasticsearchClient;

public class SyncedFlushRequestBuilder extends ActionRequestBuilder<SyncedFlushRequest, SyncedFlushResponse> {

    public SyncedFlushRequestBuilder(ElasticsearchClient client, SyncedFlushAction action) {
        super(client, action, new SyncedFlushRequest());
    }

    public SyncedFlushRequestBuilder setIndices(String[] indices) {
        super.request().indices(indices);
        return this;
    }

    public SyncedFlushRequestBuilder setIndicesOptions(IndicesOptions indicesOptions) {
        super.request().indicesOptions(indicesOptions);
        return this;
    }
}
