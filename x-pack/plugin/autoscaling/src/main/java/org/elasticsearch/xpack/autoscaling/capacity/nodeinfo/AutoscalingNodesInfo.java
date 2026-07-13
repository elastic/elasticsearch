/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.capacity.nodeinfo;

import org.elasticsearch.cluster.node.DiscoveryNode;

import java.util.Optional;

public interface AutoscalingNodesInfo {
    AutoscalingNodesInfo EMPTY = n -> Optional.empty();

    /**
     * Get the memory and processor use for the indicated node. Returns null if not available (new, fetching or failed).
     * @param node the node to get info for
     * @return memory and processor info for node if possible
     */
    Optional<AutoscalingNodeInfo> get(DiscoveryNode node);
}
