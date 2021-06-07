/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.capacity.memory;

import org.elasticsearch.cluster.node.DiscoveryNode;

public interface AutoscalingMemoryInfo {
    AutoscalingMemoryInfo EMPTY = n -> null;

    /**
     * Get the memory use for the indicated node. Returns null if not available (new, fetching or failed).
     * @param node the node to get info for
     * @return info for node.
     */
    Long get(DiscoveryNode node);
}
