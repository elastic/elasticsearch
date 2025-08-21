/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.tasks.cancel;

import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.support.tasks.TasksRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;

/**
 * Builder for the request to cancel tasks running on the specified nodes
 */
public class CancelTasksRequestBuilder extends TasksRequestBuilder<CancelTasksRequest, ListTasksResponse, CancelTasksRequestBuilder> {

    public CancelTasksRequestBuilder(ElasticsearchClient client) {
        super(client, TransportCancelTasksAction.TYPE, new CancelTasksRequest());
    }

    public CancelTasksRequestBuilder waitForCompletion(boolean waitForCompletion) {
        request.setWaitForCompletion(waitForCompletion);
        return this;
    }
}
