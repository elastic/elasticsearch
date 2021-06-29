/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.recovery;

import org.elasticsearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Recovery information request builder.
 */
public class RecoveryRequestBuilder extends BroadcastOperationRequestBuilder<RecoveryRequest, RecoveryResponse, RecoveryRequestBuilder> {

    /**
     * Constructs a new recovery information request builder.
     */
    public RecoveryRequestBuilder(ElasticsearchClient client, RecoveryAction action) {
        super(client, action, new RecoveryRequest());
    }

    public RecoveryRequestBuilder setDetailed(boolean detailed) {
        request.detailed(detailed);
        return this;
    }

    public RecoveryRequestBuilder setActiveOnly(boolean activeOnly) {
        request.activeOnly(activeOnly);
        return this;
    }
}
