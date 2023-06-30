/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.hotthreads;

import org.elasticsearch.action.support.nodes.NodesOperationRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.monitor.jvm.HotThreads;

public class NodesHotThreadsRequestBuilder extends NodesOperationRequestBuilder<
    NodesHotThreadsRequest,
    NodesHotThreadsResponse,
    NodesHotThreadsRequestBuilder> {

    public NodesHotThreadsRequestBuilder(ElasticsearchClient client, NodesHotThreadsAction action) {
        super(client, action, new NodesHotThreadsRequest());
    }

    public NodesHotThreadsRequestBuilder setThreads(int threads) {
        request.threads(threads);
        return this;
    }

    public NodesHotThreadsRequestBuilder setIgnoreIdleThreads(boolean ignoreIdleThreads) {
        request.ignoreIdleThreads(ignoreIdleThreads);
        return this;
    }

    public NodesHotThreadsRequestBuilder setType(HotThreads.ReportType type) {
        request.type(type);
        return this;
    }

    public NodesHotThreadsRequestBuilder setInterval(TimeValue interval) {
        request.interval(interval);
        return this;
    }
}
