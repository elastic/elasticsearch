/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.core.Nullable;

/**
 * Interface for helper to pick a node to relocate a running reindex task to when the current node is about to shut down.
 */
public interface ReindexRelocationNodePicker {

    /**
     * Picks a suitable node.
     *
     * @param nodes The set of nodes to pick from
     * @return The ID of the selected node, or null if there are no suitable nodes
     */
    @Nullable
    String pickNode(DiscoveryNodes nodes);
}
