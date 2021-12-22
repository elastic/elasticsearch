/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.tasks;

import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;

public class PendingClusterTasksRequestBuilder extends MasterNodeReadOperationRequestBuilder<
    PendingClusterTasksRequest,
    PendingClusterTasksResponse,
    PendingClusterTasksRequestBuilder> {

    public PendingClusterTasksRequestBuilder(ElasticsearchClient client, PendingClusterTasksAction action) {
        super(client, action, new PendingClusterTasksRequest());
    }
}
