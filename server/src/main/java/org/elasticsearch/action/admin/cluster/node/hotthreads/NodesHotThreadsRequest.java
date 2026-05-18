/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.hotthreads;

import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.monitor.jvm.HotThreads;

public class NodesHotThreadsRequest extends BaseNodesRequest {

    final HotThreads.RequestOptions requestOptions;

    /**
     * Get hot threads from nodes based on the nodes ids specified. If none are passed, hot
     * threads for all nodes is used.
     */
    public NodesHotThreadsRequest(String[] nodesIds, HotThreads.RequestOptions requestOptions) {
        super(nodesIds);
        this.requestOptions = requestOptions;
    }

    /**
     * Get hot threads from the given node, for use if the node isn't a stable member of the cluster.
     */
    public NodesHotThreadsRequest(DiscoveryNode node, HotThreads.RequestOptions requestOptions) {
        super(node);
        this.requestOptions = requestOptions;
    }

    public int threads() {
        return requestOptions.threads();
    }

    public boolean ignoreIdleThreads() {
        return requestOptions.ignoreIdleThreads();
    }

    public HotThreads.ReportType type() {
        return requestOptions.reportType();
    }

    public HotThreads.SortOrder sortOrder() {
        return requestOptions.sortOrder();
    }

    public TimeValue interval() {
        return requestOptions.interval();
    }

    public int snapshots() {
        return requestOptions.snapshots();
    }
}
