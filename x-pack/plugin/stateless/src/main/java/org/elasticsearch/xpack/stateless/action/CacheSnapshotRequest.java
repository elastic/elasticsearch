/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.action;

import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;

/**
 * Request for the cache snapshot action. Targets a single node, identified by the
 * REST layer from the required {@code ?node_id} query parameter.
 */
public class CacheSnapshotRequest extends BaseNodesRequest {

    public CacheSnapshotRequest(DiscoveryNode targetNode) {
        super(targetNode);
    }
}
