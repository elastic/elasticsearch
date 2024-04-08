/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.flush;

import org.elasticsearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.client.internal.ElasticsearchClient;

public class FlushRequestBuilder extends BroadcastOperationRequestBuilder<FlushRequest, BroadcastResponse, FlushRequestBuilder> {

    public FlushRequestBuilder(ElasticsearchClient client) {
        super(client, FlushAction.INSTANCE, new FlushRequest());
    }

    public FlushRequestBuilder setForce(boolean force) {
        request.force(force);
        return this;
    }

    public FlushRequestBuilder setWaitIfOngoing(boolean waitIfOngoing) {
        request.waitIfOngoing(waitIfOngoing);
        return this;
    }
}
