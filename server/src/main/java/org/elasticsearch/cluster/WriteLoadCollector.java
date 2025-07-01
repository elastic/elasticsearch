/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.action.ActionListener;

import java.util.Map;

/**
 * Collects the write load estimates for each node in the cluster.
 * <p>
 * Results are returned as a map of node ID to node write load.
 */
public interface WriteLoadCollector {
    /**
     * This will be used when there is no WriteLoadCollector available.
     */
    WriteLoadCollector EMPTY = listener -> listener.onResponse(Map.of());

    /**
     * Collects the write load estimates from the cluster.
     *
     * @param listener The listener to receive the write load results.
     */
    void collectWriteLoads(ActionListener<Map<String, NodeWriteLoad>> listener);
}
