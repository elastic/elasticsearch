/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.transport.actions.activate;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;

/**
 * A activate watch action request builder.
 */
public class ActivateWatchRequestBuilder extends ActionRequestBuilder<ActivateWatchRequest, ActivateWatchResponse> {

    public ActivateWatchRequestBuilder(ElasticsearchClient client, String id, boolean activate) {
        super(client, ActivateWatchAction.INSTANCE, new ActivateWatchRequest(id, activate));
    }

}
