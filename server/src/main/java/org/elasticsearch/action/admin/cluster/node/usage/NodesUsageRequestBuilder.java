/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.usage;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.nodes.NodesOperationRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;

public class NodesUsageRequestBuilder extends NodesOperationRequestBuilder<
    NodesUsageRequest,
    NodesUsageResponse,
    NodesUsageRequestBuilder> {

    public NodesUsageRequestBuilder(ElasticsearchClient client, ActionType<NodesUsageResponse> action) {
        super(client, action, new NodesUsageRequest());
    }

}
